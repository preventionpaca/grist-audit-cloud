#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Audit complet Grist (structure + contenu d'une table spécifique)
Version cloud optimisée pour Render (Python 3)
"""

import os, json, time, re, threading, unicodedata
from datetime import datetime
import requests
from flask import Flask, jsonify

# --------------------------- Configuration ---------------------------
HOST = os.getenv("GRIST_HOST", "https://docs.getgrist.com")
DOC = os.getenv("GRIST_DOC_ID") or ""
API_KEY = os.getenv("GRIST_API_KEY") or ""

# Cible par défaut + options de forçage
TARGET_TABLE        = os.getenv("GRIST_TABLE", "Liste_des_equipements")
TARGET_TABLE_ID     = os.getenv("GRIST_TABLE_ID") or ""      # si tu connais l'id exact
TARGET_TABLE_NAME   = os.getenv("GRIST_TABLE_NAME") or ""    # si le libellé diffère

DATA_DIR    = os.getenv("DATA_DIR", os.getcwd())
CUR_SCHEMA  = os.path.join(DATA_DIR, "schema_current.json")
DIFF_SCHEMA = os.path.join(DATA_DIR, "schema_diff.json")
CUR_EQUIP   = os.path.join(DATA_DIR, "equip_current.json")
DIFF_EQUIP  = os.path.join(DATA_DIR, "equip_diff.json")
LOCK_FILE   = os.path.join(DATA_DIR, ".lock")

PROGRESS = {"busy": False, "percent": 0, "step": ""}  # état pour /status

# --------------------------- Helpers API Grist -----------------------
def _req(method, url, **kw):
    for i in range(4):
        try:
            r = requests.request(method, url, timeout=40, **kw)
            if r.status_code in (502,503,504):
                time.sleep(1.5*(i+1)); continue
            r.raise_for_status()
            return r
        except requests.RequestException:
            if i == 3: raise
            time.sleep(1.5*(i+1))

def api_get(path, params=None):
    url = HOST.rstrip('/') + '/api' + path
    headers = {'Authorization': f'Bearer {API_KEY}'}
    return _req('GET', url, headers=headers, params=params).json()

def list_tables():        return api_get(f"/docs/{DOC}/tables").get("tables", [])
def list_columns(tid):    return api_get(f"/docs/{DOC}/tables/{tid}/columns", params={"hidden":"true"}).get("columns", [])

def fetch_rows(table_id):
    """Récupère TOUTES les lignes avec pagination."""
    all_recs, offset, page = [], 0, 5000
    while True:
        data = api_get(f"/docs/{DOC}/tables/{table_id}/data",
                       params={"limit": page, "offset": offset})
        recs = data.get("records", [])
        all_recs.extend(recs)
        if len(recs) < page: break
        offset += page
    return all_recs

# ---------------------- Résolution robuste table cible ----------------
def _normalize(s: str) -> str:
    s = str(s or "")
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.lower().strip()
    s = re.sub(r"[ .]+", "_", s)
    return s

def resolve_target_table_id() -> str:
    if TARGET_TABLE_ID:
        return TARGET_TABLE_ID
    wanted = TARGET_TABLE_NAME or TARGET_TABLE
    wanted_norm = _normalize(wanted)
    for t in list_tables():
        tid  = t.get("id","")
        name = t.get("name","") or tid
        if tid == wanted or name == wanted: return tid
        if _normalize(tid) == wanted_norm or _normalize(name) == wanted_norm: return tid
    return wanted  # fallback

# ----------------------------- Schéma --------------------------------
def ref_target(t):
    if not isinstance(t,str): return ""
    m = re.match(r"^Ref(?:List)?:([\\w.$-]+)$", t)
    return m.group(1) if m else ""

FIELDS = ["type","isFormula","refTableId","visibleCol","label","description"]

def scan_schema():
    rows=[]
    for t in list_tables():
        tid=t.get("id"); pos=0
        for c in list_columns(tid):
            f=c.get("fields") or {}
            type_=f.get("type",""); tgt=ref_target(type_)
            rows.append({
                "tableId":tid,
                "colId":c.get("id",""),
                "label":f.get("label",""),
                "type":type_,
                "isFormula":bool(f.get("isFormula")) or bool(f.get("formula")),
                "formula":(f.get("formula") or "")[:240],
                "isRef":bool(tgt),
                "refTableId":tgt,
                "visibleCol":f.get("visibleCol"),
                "description":(f.get("description") or "")[:240],
                "pos":pos
            }); pos+=1
    return rows

def _index(rows):  return { (r.get("tableId",""), r.get("colId","")): r for r in rows }
def _tables(rows): return set(r.get("tableId","") for r in rows)

def make_schema_diff(cur, base):
    out=[]; ic,ib=_index(cur),_index(base); tc,tb=_tables(cur),_tables(base)
    for t in sorted(tc - tb): out.append({"changeType":"ADDED_TABLE","tableId":t})
    for t in sorted(tb - tc): out.append({"changeType":"REMOVED_TABLE","tableId":t})
    for k in sorted(set(ic.keys()) | set(ib.keys())):
        c,b = ic.get(k), ib.get(k); t,col = k
        if c and not b: out.append({"changeType":"ADDED_COL","tableId":t,"colId":col})
        elif b and not c: out.append({"changeType":"REMOVED_COL","tableId":t,"colId":col})
        else:
            for f in FIELDS:
                if b.get(f) != c.get(f):
                    out.append({"changeType":"CHANGED_FIELD","tableId":t,"colId":col,"field":f,
                                "oldValue":str(b.get(f)),"newValue":str(c.get(f))})
    return out

# ------------------------- Diff contenu table -------------------------
def make_equip_diff(cur, base):
    cur_idx  = {r["id"]: r["fields"] for r in cur}
    base_idx = {r["id"]: r["fields"] for r in base}
    out=[]
    for id_ in set(cur_idx.keys()) | set(base_idx.keys()):
        c,b = cur_idx.get(id_), base_idx.get(id_)
        if c and not b:   out.append({"changeType":"ADDED_ROW","id":id_})
        elif b and not c: out.append({"changeType":"REMOVED_ROW","id":id_})
        elif c != b:      out.append({"changeType":"CHANGED_ROW","id":id_})
    return out

# ---------------------- Statuts pour coloration ----------------------
def mark_status_schema(cur, diff):
    status_map={}
    for d in diff:
        key=(d.get("tableId"), d.get("colId"))
        if   "ADDED"   in d["changeType"]: status_map[key]="added"
        elif "REMOVED" in d["changeType"]: status_map[key]="removed"
        elif "CHANGED" in d["changeType"]: status_map[key]="changed"
    for r in cur:
        key=(r["tableId"], r["colId"])
        r["status"]=status_map.get(key,"normal")
    return cur

def mark_status_equip(cur, diff):
    status_map={}
    for d in diff:
        id_=d.get("id")
        if   "ADDED"   in d["changeType"]: status_map[id_]="added"
        elif "REMOVED" in d["changeType"]: status_map[id_]="removed"
        elif "CHANGED" in d["changeType"]: status_map[id_]="changed"
    for r in cur:
        r["status"]=status_map.get(r["id"],"normal")
    return cur

def save_json(o, p):
    with open(p,"w",encoding="utf-8") as f: json.dump(o,f,ensure_ascii=False,indent=2)

# ------------------------------- Flask --------------------------------
app = Flask(__name__)

@app.after_request
def add_cors_headers(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return resp

@app.route("/", defaults={"_path": ""}, methods=["OPTIONS"])
@app.route("/<path:_path>", methods=["OPTIONS"])
def cors_preflight(_path): return ("", 204)

@app.get("/")
def index():
    return jsonify({"service":"grist-audit-cloud","ok":True,"hint":"use /run then /result"})

@app.get("/status")
def status():
    return jsonify({
        "ok": bool(API_KEY and DOC),
        "doc": DOC,
        "busy": PROGRESS["busy"],
        "percent": PROGRESS["percent"],
        "step": PROGRESS["step"],
        "target_table": TARGET_TABLE,
        "resolved_table_id": resolve_target_table_id(),
        "time": datetime.utcnow().isoformat()+"Z"
    })

# Debug pratique : voir échantillon brut de la table cible
@app.get("/debug/equip")
def debug_equip():
    tid = resolve_target_table_id()
    try:
        sample = fetch_rows(tid)[:3]
        return jsonify({"resolved_table_id": tid, "sample_count": len(sample), "sample": sample})
    except Exception as e:
        return jsonify({"resolved_table_id": tid, "error": str(e)}), 500

# ------------------------------ Thread audit --------------------------
def background_audit():
    PROGRESS.update({"busy": True, "percent": 5, "step": "Initialisation"})
    try:
        base_schema = json.load(open(CUR_SCHEMA)) if os.path.exists(CUR_SCHEMA) else []
        base_equip  = json.load(open(CUR_EQUIP))  if os.path.exists(CUR_EQUIP)  else []

        PROGRESS.update({"percent": 25, "step": "Scan du schéma"})
        cur_schema = scan_schema()

        real_id = resolve_target_table_id()
        PROGRESS.update({"percent": 55, "step": f"Lecture de {real_id}"})
        cur_equip=[]
        try:
            cur_equip = fetch_rows(real_id)
            PROGRESS.update({"step": f"Lecture de {real_id} : {len(cur_equip)} lignes"})
        except Exception as e:
            PROGRESS.update({"step": f"Impossible de lire {real_id}: {e}"})

        PROGRESS.update({"percent": 75, "step": "Calcul des différences"})
        diff_schema = make_schema_diff(cur_schema, base_schema)
        diff_equip  = make_equip_diff(cur_equip, base_equip)

        PROGRESS.update({"percent": 90, "step": "Sauvegarde"})
        save_json(cur_schema, CUR_SCHEMA)
        save_json(cur_equip,  CUR_EQUIP)
        save_json(diff_schema, DIFF_SCHEMA)
        save_json(diff_equip,  DIFF_EQUIP)

        PROGRESS.update({"percent": 100, "step": "Terminé"})
        time.sleep(0.5)
    finally:
        PROGRESS.update({"busy": False})
        if os.path.exists(LOCK_FILE):
            try: os.remove(LOCK_FILE)
            except: pass

# ------------------------------- Routes API ---------------------------
@app.post("/run")
def run():
    if PROGRESS["busy"]:
        return jsonify({"ok": False, "busy": True})
    open(LOCK_FILE,"w").write("run")
    threading.Thread(target=background_audit, daemon=True).start()
    return jsonify({"ok": True, "started": True})

@app.get("/result")
def result():
    # Toujours renvoyer structure + contenu même sans diff
    cur_schema = json.load(open(CUR_SCHEMA)) if os.path.exists(CUR_SCHEMA) else scan_schema()
    cur_equip  = json.load(open(CUR_EQUIP))  if os.path.exists(CUR_EQUIP)  else fetch_rows(resolve_target_table_id())
    diff_schema = json.load(open(DIFF_SCHEMA)) if os.path.exists(DIFF_SCHEMA) else []
    diff_equip  = json.load(open(DIFF_EQUIP))  if os.path.exists(DIFF_EQUIP)  else []
    schema_full = mark_status_schema(cur_schema, diff_schema)
    equip_full  = mark_status_equip(cur_equip,  diff_equip)
    return jsonify({
        "summary": f"{len(diff_schema)} chgt schéma, {len(diff_equip)} chgt contenu",
        "schema_full": schema_full,
        "equip_full": equip_full
    })

# ------------------------------- Run ----------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT","8000"))
    app.run(host="0.0.0.0", port=port)
