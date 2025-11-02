#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Audit Grist cloud (structure + diff + contenu Liste_des_equipements)
- Diff fiable (added/changed/removed) basé sur baseline CUR_EQUIP
- Persistance des suppressions et créations (historique entre audits)
- Expose /result avec "kind" par ligne: added/changed/normal (removed listé à part)
"""

import os, json, time, re, threading, unicodedata
from datetime import datetime
import requests
from flask import Flask, jsonify

HOST = os.getenv("GRIST_HOST", "https://docs.getgrist.com")
DOC  = os.getenv("GRIST_DOC_ID") or ""
API_KEY = os.getenv("GRIST_API_KEY") or ""

TARGET_TABLE      = os.getenv("GRIST_TABLE", "Liste_des_equipements")
TARGET_TABLE_ID   = os.getenv("GRIST_TABLE_ID") or ""
TARGET_TABLE_NAME = os.getenv("GRIST_TABLE_NAME") or ""

DATA_DIR    = os.getenv("DATA_DIR", os.getcwd())
CUR_SCHEMA  = os.path.join(DATA_DIR, "schema_current.json")
DIFF_SCHEMA = os.path.join(DATA_DIR, "schema_diff.json")
CUR_EQUIP   = os.path.join(DATA_DIR, "equip_current.json")
DIFF_EQUIP  = os.path.join(DATA_DIR, "equip_diff.json")

# Nouveaux historiques
EQUIP_REMOVED = os.path.join(DATA_DIR, "equip_removed.json")  # [{id, fields, deletedAt}]
EQUIP_ADDED   = os.path.join(DATA_DIR, "equip_added.json")    # [{id, fields, createdAt}]

LOCK_FILE = os.path.join(DATA_DIR, ".lock")
PROGRESS = {"busy": False, "percent": 0, "step": ""}

# ---------------- API helpers ----------------
def _req(method, url, **kw):
    for i in range(4):
        try:
            r = requests.request(method, url, timeout=40, **kw)
            if r.status_code in (502,503,504):
                time.sleep(1.2*(i+1)); continue
            r.raise_for_status()
            return r
        except requests.RequestException:
            if i == 3: raise
            time.sleep(1.2*(i+1))

def api_get(path, params=None):
    url = HOST.rstrip('/') + '/api' + path
    headers = {'Authorization': f'Bearer {API_KEY}'}
    return _req('GET', url, headers=headers, params=params).json()

def list_tables():     return api_get(f"/docs/{DOC}/tables").get("tables", [])
def list_columns(tid): return api_get(f"/docs/{DOC}/tables/{tid}/columns", params={"hidden":"true"}).get("columns", [])

def fetch_rows(table_id):
    """Paginate records. Essaye /records puis /data."""
    def _paged(pth):
        all_recs, offset, page = [], 0, 5000
        while True:
            data = api_get(pth, params={"limit": page, "offset": offset})
            recs = data.get("records", [])
            all_recs.extend(recs)
            if len(recs) < page: break
            offset += page
        return all_recs
    recs = _paged(f"/docs/{DOC}/tables/{table_id}/records")
    return recs if recs else _paged(f"/docs/{DOC}/tables/{table_id}/data")

# -------------- Resolve table id --------------
def _normalize(s: str) -> str:
    s = str(s or "")
    s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
    s = s.lower().strip()
    s = re.sub(r"[ .]+","_", s)
    return s

def resolve_target_table_id() -> str:
    if TARGET_TABLE_ID: return TARGET_TABLE_ID
    wanted = TARGET_TABLE_NAME or TARGET_TABLE
    wn = _normalize(wanted)
    for t in list_tables():
        tid = t.get("id",""); name = t.get("name","") or tid
        if tid == wanted or name == wanted: return tid
        if _normalize(tid) == wn or _normalize(name) == wn: return tid
    return wanted

# ----------------- Schéma ---------------------
def ref_target(t):
    if not isinstance(t,str): return ""
    m = re.match(r"^Ref(?:List)?:([\w.$-]+)$", t)
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
                "tableId":tid,"colId":c.get("id",""),"label":f.get("label",""),
                "type":type_,
                "isFormula":bool(f.get("isFormula")) or bool(f.get("formula")),
                "formula":(f.get("formula") or "")[:240],
                "isRef":bool(tgt),"refTableId":tgt,
                "visibleCol":f.get("visibleCol"),
                "description":(f.get("description") or "")[:240],
                "pos":pos
            }); pos+=1
    return rows

def _index(rows):  return {r["id"]: r for r in rows} if rows else {}
def _index_schema(rows): return {(r.get("tableId",""), r.get("colId","")): r for r in rows}
def _tables(rows): return set(r.get("tableId","") for r in rows)

def make_schema_diff(cur, base):
    out=[]; ic=_index_schema(cur); ib=_index_schema(base)
    tc=_tables(cur); tb=_tables(base)
    for t in sorted(tc-tb): out.append({"changeType":"ADDED_TABLE","tableId":t})
    for t in sorted(tb-tc): out.append({"changeType":"REMOVED_TABLE","tableId":t})
    for k in sorted(set(ic.keys())|set(ib.keys())):
        c,b=ic.get(k),ib.get(k); t,col=k
        if c and not b: out.append({"changeType":"ADDED_COL","tableId":t,"colId":col}); continue
        if b and not c: out.append({"changeType":"REMOVED_COL","tableId":t,"colId":col}); continue
        for f in FIELDS:
            if b.get(f)!=c.get(f):
                out.append({"changeType":"CHANGED_FIELD","tableId":t,"colId":col,"field":f,
                            "oldValue":str(b.get(f)),"newValue":str(c.get(f))})
    return out

# ------------- Diff contenu fiable ------------
def make_equip_diff(cur, base):
    """Compare par id: ADDED_ROW si id∈cur\base; REMOVED_ROW si id∈base\cur; CHANGED_ROW si fields diffèrent."""
    cur_idx  = {r["id"]: r.get("fields", {}) for r in (cur or [])}
    base_idx = {r["id"]: r.get("fields", {}) for r in (base or [])}
    out=[]
    ids = set(cur_idx.keys()) | set(base_idx.keys())
    for rid in ids:
        c = cur_idx.get(rid); b = base_idx.get(rid)
        if c is not None and b is None:
            out.append({"changeType":"ADDED_ROW","id":rid})
        elif b is not None and c is None:
            out.append({"changeType":"REMOVED_ROW","id":rid})
        elif c != b:
            out.append({"changeType":"CHANGED_ROW","id":rid})
    return out

# ---------- Status + persistance histos -------
def mark_status_equip(cur, diff):
    """Ajoute r['status'] ET r['kind'] aux lignes courantes en mappant le diff par id."""
    kind_map={}
    for d in (diff or []):
        ct=d.get("changeType"); rid=d.get("id")
        if ct=="ADDED_ROW":   kind_map[rid]="added"
        elif ct=="CHANGED_ROW": kind_map[rid]="changed"
        elif ct=="REMOVED_ROW": kind_map[rid]="removed"
    for r in (cur or []):
        k = kind_map.get(r["id"], "normal")
        r["status"] = "added" if k=="added" else ("changed" if k=="changed" else "normal")
        r["kind"]   = k
    return cur

def save_json(o,p):
    with open(p,"w",encoding="utf-8") as f:
        json.dump(o,f,ensure_ascii=False,indent=2)

def load_json_list(path):
    if not os.path.exists(path): return []
    try:
        with open(path,"r",encoding="utf-8") as f:
            data=json.load(f)
            return data if isinstance(data,list) else []
    except Exception:
        return []

# ---------------- Flask -----------------------
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

@app.get("/debug/equip")
def debug_equip():
    tid = resolve_target_table_id()
    try:
        recs_records = api_get(f"/docs/{DOC}/tables/{tid}/records", params={"limit": 3}).get("records", [])
        recs_data    = api_get(f"/docs/{DOC}/tables/{tid}/data",    params={"limit": 3}).get("records", [])
        return jsonify({
            "resolved_table_id": tid,
            "records_endpoint_count": len(recs_records),
            "data_endpoint_count": len(recs_data)
        })
    except Exception as e:
        return jsonify({"resolved_table_id": tid, "error": str(e)}), 500

# --------------- Background audit ------------
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

        # ---- Historiser suppressions et créations (depuis le diff fiable) ----
        try:
            now_iso = datetime.utcnow().isoformat()+"Z"
            base_idx  = {r["id"]: r.get("fields", {}) for r in (base_equip or [])}
            cur_idx   = {r["id"]: r.get("fields", {}) for r in (cur_equip or [])}

            removed_snaps = []
            added_snaps   = []
            for d in diff_equip:
                if d["changeType"]=="REMOVED_ROW":
                    rid = d["id"]
                    removed_snaps.append({"id": rid, "fields": base_idx.get(rid, {}), "deletedAt": now_iso})
                elif d["changeType"]=="ADDED_ROW":
                    rid = d["id"]
                    added_snaps.append({"id": rid, "fields": cur_idx.get(rid, {}), "createdAt": now_iso})

            if removed_snaps:
                hist = load_json_list(EQUIP_REMOVED)
                existing = {(h.get("id"), json.dumps(h.get("fields", {}), sort_keys=True)) for h in hist}
                for snap in removed_snaps:
                    key=(snap["id"], json.dumps(snap.get("fields", {}), sort_keys=True))
                    if key not in existing: hist.append(snap)
                save_json(hist, EQUIP_REMOVED)

            if added_snaps:
                hist = load_json_list(EQUIP_ADDED)
                existing = {(h.get("id"), json.dumps(h.get("fields", {}), sort_keys=True)) for h in hist}
                for snap in added_snaps:
                    key=(snap["id"], json.dumps(snap.get("fields", {}), sort_keys=True))
                    if key not in existing: hist.append(snap)
                save_json(hist, EQUIP_ADDED)
        except Exception:
            pass  # ne casse pas l'audit

        PROGRESS.update({"percent": 90, "step": "Sauvegarde"})
        save_json(cur_schema, CUR_SCHEMA)
        save_json(cur_equip,  CUR_EQUIP)
        save_json(diff_schema, DIFF_SCHEMA)
        save_json(diff_equip,  DIFF_EQUIP)

        PROGRESS.update({"percent": 100, "step": "Terminé"})
        time.sleep(0.4)
    finally:
        PROGRESS.update({"busy": False})
        try:
            if os.path.exists(LOCK_FILE): os.remove(LOCK_FILE)
        except: pass

# ---------------- Routes ---------------------
@app.post("/run")
def run():
    if PROGRESS["busy"]:
        return jsonify({"ok": False, "busy": True})
    open(LOCK_FILE,"w").write("run")
    threading.Thread(target=background_audit, daemon=True).start()
    return jsonify({"ok": True, "started": True})

@app.get("/result")
def result():
    # Charge dernier état (sans forcer un nouvel audit)
    cur_schema = json.load(open(CUR_SCHEMA)) if os.path.exists(CUR_SCHEMA) else scan_schema()
    cur_equip  = json.load(open(CUR_EQUIP))  if os.path.exists(CUR_EQUIP)  else fetch_rows(resolve_target_table_id())
    diff_schema = json.load(open(DIFF_SCHEMA)) if os.path.exists(DIFF_SCHEMA) else []
    diff_equip  = json.load(open(DIFF_EQUIP))  if os.path.exists(DIFF_EQUIP)  else []

    # Marque kind/status à partir du DIFF FIABLE
    equip_full  = mark_status_equip(cur_equip, diff_equip)
    return jsonify({
        "summary": f"{len(diff_schema)} chgt schéma, {len(diff_equip)} chgt contenu",
        "schema_full": cur_schema,   # pas besoin d'y injecter status ici
        "equip_full": equip_full     # contient status + kind
    })

if __name__ == "__main__":
    port = int(os.getenv("PORT","8000"))
    app.run(host="0.0.0.0", port=port)
