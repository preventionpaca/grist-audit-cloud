#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Grist Audit Cloud – structure + diff + historique persistant
- Diff fiable (added/changed/removed) basé sur baseline précédente
- Heuristique: si 'before' est vide => 'added' (création)
- Historisation persistante (equip_history.json) + confort (equip_added.json, equip_removed.json)
- /result renvoie l'état courant avec rec.kind (added/changed/normal)
- /files/<name>.json ne renvoie jamais 404 (crée [] si absent)
"""

import os, json, time, re, threading, unicodedata
from datetime import datetime
import requests
from flask import Flask, jsonify, send_from_directory

# ------------------------- Config -------------------------
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

# Journaux persistants
EQUIP_HISTORY = os.path.join(DATA_DIR, "equip_history.json")  # [{id, action, at, before, after}]
EQUIP_REMOVED = os.path.join(DATA_DIR, "equip_removed.json")  # [{id, fields, deletedAt}]
EQUIP_ADDED   = os.path.join(DATA_DIR, "equip_added.json")    # [{id, fields, createdAt}]

LOCK_FILE = os.path.join(DATA_DIR, ".lock")
PROGRESS = {"busy": False, "percent": 0, "step": ""}

# --------------------- Helpers JSON/files ------------------
def ensure_json_file(path, default):
    """Crée le fichier JSON s'il n'existe pas (valeur par défaut)."""
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(default, f, ensure_ascii=False, indent=2)

def save_json(obj, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def load_json(path, default=None):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def load_json_list(path):
    data = load_json(path, default=[])
    return data if isinstance(data, list) else []

# ----------------------- API Grist -------------------------
def _req(method, url, **kw):
    for i in range(4):
        try:
            r = requests.request(method, url, timeout=40, **kw)
            if r.status_code in (502, 503, 504):
                time.sleep(1.2 * (i + 1))
                continue
            r.raise_for_status()
            return r
        except requests.RequestException:
            if i == 3:
                raise
            time.sleep(1.2 * (i + 1))

def api_get(path, params=None):
    url = HOST.rstrip('/') + '/api' + path
    headers = {'Authorization': f'Bearer {API_KEY}'}
    return _req('GET', url, headers=headers, params=params).json()

def list_tables():
    return api_get(f"/docs/{DOC}/tables").get("tables", [])

def list_columns(tid):
    return api_get(f"/docs/{DOC}/tables/{tid}/columns", params={"hidden": "true"}).get("columns", [])

def fetch_rows(table_id):
    """Récupère toutes les lignes via /records (fallback /data)."""
    def _paged(pth):
        all_recs, offset, page = [], 0, 5000
        while True:
            data = api_get(pth, params={"limit": page, "offset": offset})
            recs = data.get("records", [])
            all_recs.extend(recs)
            if len(recs) < page:
                break
            offset += page
        return all_recs
    recs = _paged(f"/docs/{DOC}/tables/{table_id}/records")
    return recs if recs else _paged(f"/docs/{DOC}/tables/{table_id}/data")

# ---------------- Résolution table cible -------------------
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
    wn = _normalize(wanted)
    for t in list_tables():
        tid = t.get("id", "")
        name = t.get("name", "") or tid
        if tid == wanted or name == wanted:
            return tid
        if _normalize(tid) == wn or _normalize(name) == wn:
            return tid
    return wanted  # renvoie la saisie telle quelle si rien trouvé

# --------------------- Scan du schéma ----------------------
def ref_target(t):
    if not isinstance(t, str):
        return ""
    m = re.match(r"^Ref(?:List)?:([\w.$-]+)$", t)
    return m.group(1) if m else ""

FIELDS = ["type", "isFormula", "refTableId", "visibleCol", "label", "description"]

def scan_schema():
    rows = []
    for t in list_tables():
        tid = t.get("id")
        pos = 0
        for c in list_columns(tid):
            f = c.get("fields") or {}
            type_ = f.get("type", "")
            tgt = ref_target(type_)
            rows.append({
                "tableId": tid,
                "colId": c.get("id", ""),
                "label": f.get("label", ""),
                "type": type_,
                "isFormula": bool(f.get("isFormula")) or bool(f.get("formula")),
                "formula": (f.get("formula") or "")[:240],
                "isRef": bool(tgt),
                "refTableId": tgt,
                "visibleCol": f.get("visibleCol"),
                "description": (f.get("description") or "")[:240],
                "pos": pos
            })
            pos += 1
    return rows

def _index_schema(rows):
    return {(r.get("tableId", ""), r.get("colId", "")): r for r in rows}

def _tables(rows):
    return set(r.get("tableId", "") for r in rows)

def make_schema_diff(cur, base):
    out = []
    ic = _index_schema(cur)
    ib = _index_schema(base)
    tc = _tables(cur)
    tb = _tables(base)
    for t in sorted(tc - tb):
        out.append({"changeType": "ADDED_TABLE", "tableId": t})
    for t in sorted(tb - tc):
        out.append({"changeType": "REMOVED_TABLE", "tableId": t})
    for k in sorted(set(ic.keys()) | set(ib.keys())):
        c, b = ic.get(k), ib.get(k)
        t, col = k
        if c and not b:
            out.append({"changeType": "ADDED_COL", "tableId": t, "colId": col})
            continue
        if b and not c:
            out.append({"changeType": "REMOVED_COL", "tableId": t, "colId": col})
            continue
        for f in FIELDS:
            if (b.get(f) if b else None) != (c.get(f) if c else None):
                out.append({
                    "changeType": "CHANGED_FIELD",
                    "tableId": t, "colId": col, "field": f,
                    "oldValue": str(b.get(f) if b else None),
                    "newValue": str(c.get(f) if c else None)
                })
    return out

# ------------------- Diff du contenu -----------------------
def _is_empty_fields(fields: dict) -> bool:
    """
    Considère 'vide' si tous les champs sont None / "" / [] / {}.
    Utile pour reclasser CHANGED => ADDED quand la ligne 'avant' était un gabarit vide.
    """
    if not isinstance(fields, dict):
        return True
    for v in fields.values():
        if v is None:
            continue
        if isinstance(v, str) and v.strip() == "":
            continue
        if isinstance(v, (list, tuple, set)) and len(v) == 0:
            continue
        if isinstance(v, dict) and len(v) == 0:
            continue
        return False  # valeur substantielle trouvée
    return True

def make_equip_diff(cur, base):
    """
    Compare par id et reclasse 'CHANGED' en 'ADDED' si le 'before' est vide.
    """
    cur_idx  = {r["id"]: r.get("fields", {}) for r in (cur or [])}
    base_idx = {r["id"]: r.get("fields", {}) for r in (base or [])}
    out = []
    ids = set(cur_idx.keys()) | set(base_idx.keys())
    for rid in ids:
        c = cur_idx.get(rid)
        b = base_idx.get(rid)
        if c is not None and b is None:
            out.append({"changeType": "ADDED_ROW", "id": rid})
        elif b is not None and c is None:
            out.append({"changeType": "REMOVED_ROW", "id": rid})
        elif c != b:
            if _is_empty_fields(b):
                out.append({"changeType": "ADDED_ROW", "id": rid})
            else:
                out.append({"changeType": "CHANGED_ROW", "id": rid})
    return out

def mark_status_equip(cur, diff):
    """Ajoute rec.kind (added/changed/removed/normal) sur l'état courant depuis le diff."""
    kind_map = {}
    for d in (diff or []):
        ct = d.get("changeType")
        rid = d.get("id")
        if ct == "ADDED_ROW":
            kind_map[rid] = "added"
        elif ct == "CHANGED_ROW":
            kind_map[rid] = "changed"
        elif ct == "REMOVED_ROW":
            kind_map[rid] = "removed"
    for r in (cur or []):
        k = kind_map.get(r["id"], "normal")
        r["kind"] = k
        r["status"] = "added" if k == "added" else ("changed" if k == "changed" else "normal")
    return cur

# -------- Historisation consolidée (patch robuste) ---------
def persist_history_from_diff(diff_equip, base_equip, cur_equip,
                              history_path, removed_path, added_path):
    """
    Alimente l'historique persistant (equip_history.json) + fichiers de confort
    (equip_removed.json, equip_added.json) à partir du diff du run courant.
    """
    now_iso  = datetime.utcnow().isoformat() + "Z"
    base_idx = {r["id"]: r.get("fields", {}) for r in (base_equip or [])}
    cur_idx  = {r["id"]: r.get("fields", {}) for r in (cur_equip  or [])}

    history = load_json_list(history_path)
    added_snaps, removed_snaps = [], []

    for d in (diff_equip or []):
        rid = d["id"]; ct = d["changeType"]  # ADDED_ROW / CHANGED_ROW / REMOVED_ROW
        before = base_idx.get(rid, {})
        after  = cur_idx.get(rid, {})
        history.append({
            "id": rid,
            "action": ct.replace("_ROW", "").lower(),  # added / changed / removed
            "at": now_iso,
            "before": before,
            "after": after
        })
        if ct == "REMOVED_ROW":
            removed_snaps.append({"id": rid, "fields": before, "deletedAt": now_iso})
        elif ct == "ADDED_ROW":
            added_snaps.append({"id": rid, "fields": after, "createdAt": now_iso})

    save_json(history, history_path)
    save_json(removed_snaps, removed_path)
    save_json(added_snaps,   added_path)

# ----------------------- Flask app -------------------------
app = Flask(__name__)

@app.after_request
def add_cors_headers(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return resp

@app.route("/", defaults={"_path": ""}, methods=["OPTIONS"])
@app.route("/<path:_path>", methods=["OPTIONS"])
def cors_preflight(_path):
    return ("", 204)

@app.get("/")
def index():
    return jsonify({"service": "grist-audit-cloud",
                    "ok": True,
                    "hint": "POST /run then GET /result; history at /files/equip_history.json"})

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
        "time": datetime.utcnow().isoformat() + "Z"
    })

# Route fichiers JSON – jamais 404 (crée [] si absent)
@app.get("/files/<path:fname>")
def files(fname):
    mapping = {
        "equip_history.json": EQUIP_HISTORY,
        "equip_removed.json": EQUIP_REMOVED,
        "equip_added.json":   EQUIP_ADDED,
        "schema_current.json": CUR_SCHEMA,
        "schema_diff.json":    DIFF_SCHEMA,
        "equip_current.json":  CUR_EQUIP,
        "equip_diff.json":     DIFF_EQUIP,
    }
    target = mapping.get(fname)
    if not target:
        return jsonify({"error": "unknown file"}), 404
    ensure_json_file(target, [])
    return send_from_directory(
        directory=os.path.dirname(target) or ".",
        path=os.path.basename(target),
        mimetype="application/json"
    )

# ------------------- Traitement d'audit --------------------
def background_audit():
    PROGRESS.update({"busy": True, "percent": 5, "step": "Initialisation"})
    try:
        base_schema = load_json(CUR_SCHEMA, default=[])
        base_equip  = load_json(CUR_EQUIP,  default=[])

        PROGRESS.update({"percent": 25, "step": "Scan du schéma"})
        cur_schema = scan_schema()

        real_id = resolve_target_table_id()
        PROGRESS.update({"percent": 55, "step": f"Lecture de {real_id}"})
        cur_equip = []
        try:
            cur_equip = fetch_rows(real_id)
            PROGRESS.update({"step": f"Lecture de {real_id} : {len(cur_equip)} lignes"})
        except Exception as e:
            PROGRESS.update({"step": f"Impossible de lire {real_id}: {e}"})

        PROGRESS.update({"percent": 75, "step": "Calcul des différences"})
        diff_schema = make_schema_diff(cur_schema, base_schema)
        diff_equip  = make_equip_diff(cur_equip, base_equip)

        # ---- Historiser événements (added/changed/removed) ----
        persist_history_from_diff(
            diff_equip=diff_equip,
            base_equip=base_equip,
            cur_equip=cur_equip,
            history_path=EQUIP_HISTORY,
            removed_path=EQUIP_REMOVED,
            added_path=EQUIP_ADDED
        )

        PROGRESS.update({"percent": 90, "step": "Sauvegarde"})
        save_json(cur_schema, CUR_SCHEMA)
        save_json(cur_equip,  CUR_EQUIP)
        save_json(diff_schema, DIFF_SCHEMA)
        save_json(diff_equip,  DIFF_EQUIP)

        PROGRESS.update({"percent": 100, "step": "Terminé"})
        time.sleep(0.3)
    finally:
        PROGRESS.update({"busy": False})
        try:
            if os.path.exists(LOCK_FILE):
                os.remove(LOCK_FILE)
        except:
            pass

# ------------------------- Routes --------------------------
@app.post("/run")
def run():
    if PROGRESS["busy"]:
        return jsonify({"ok": False, "busy": True})
    open(LOCK_FILE, "w").write("run")
    threading.Thread(target=background_audit, daemon=True).start()
    return jsonify({"ok": True, "started": True})

@app.get("/result")
def result():
    # Dernier état disponible
    cur_schema = load_json(CUR_SCHEMA, default=scan_schema())
    cur_equip  = load_json(CUR_EQUIP,  default=fetch_rows(resolve_target_table_id()))
    diff_schema = load_json(DIFF_SCHEMA, default=[])
    diff_equip  = load_json(DIFF_EQUIP,  default=[])

    equip_full  = mark_status_equip(cur_equip, diff_equip)
    counts = {"ADDED_ROW":0, "CHANGED_ROW":0, "REMOVED_ROW":0}
    for d in diff_equip:
        ct = d.get("changeType")
        counts[ct] = counts.get(ct, 0) + 1

    return jsonify({
        "summary": f"{len(diff_schema)} chgts schéma, {len(diff_equip)} chgts contenu",
        "schema_full": cur_schema,
        "equip_full": equip_full,
        "content_counts": counts
    })

# -------------------------- Main ---------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port)
