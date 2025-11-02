#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, json, time, re, threading
from datetime import datetime
import requests
from flask import Flask, jsonify, send_from_directory

HOST = os.getenv("GRIST_HOST", "https://docs.getgrist.com")
DOC = os.getenv("GRIST_DOC_ID") or ""
API_KEY = os.getenv("GRIST_API_KEY") or ""
DATA_DIR = os.getenv("DATA_DIR", os.getcwd())
CUR_JSON = os.path.join(DATA_DIR, "schema_current.json")
DIFF_JSON = os.path.join(DATA_DIR, "schema_diff.json")
LOCK_FILE = os.path.join(DATA_DIR, ".lock")

def _req(method, url, **kw):
    for i in range(4):
        try:
            r = requests.request(method, url, timeout=30, **kw)
            if r.status_code in (502,503,504): time.sleep(1.5*(i+1)); continue
            r.raise_for_status()
            return r
        except requests.RequestException:
            if i == 3: raise
            time.sleep(1.5*(i+1))

def api_get(path, params=None):
    url = HOST.rstrip('/') + '/api' + path
    headers = {'Authorization': f'Bearer {API_KEY}'}
    return _req('GET', url, headers=headers, params=params).json()

def list_tables(): return api_get(f"/docs/{DOC}/tables").get("tables", [])
def list_columns(t): return api_get(f"/docs/{DOC}/tables/{t}/columns", params={"hidden":"true"}).get("columns", [])

def ref_target(t): 
    if not isinstance(t,str): return ""
    m=re.match(r"^Ref(?:List)?:([\w.$-]+)$",t); return m.group(1) if m else ""

def scan_schema():
    rows=[]
    for t in list_tables():
        tid=t.get("id"); pos=0
        for c in list_columns(tid):
            f=c.get("fields") or {}
            type_=f.get("type",""); tgt=ref_target(type_)
            rows.append({
                "tableId":tid,"colId":c.get("id",""),"label":f.get("label",""),
                "type":type_,"isFormula":bool(f.get("isFormula")) or bool(f.get("formula")),
                "formula":(f.get("formula") or "")[:240],"isRef":bool(tgt),"refTableId":tgt,
                "visibleCol":f.get("visibleCol"),"description":(f.get("description") or "")[:240],"pos":pos
            }); pos+=1
    return rows

FIELDS=["type","isFormula","refTableId","visibleCol","label","description"]
def _index(rows): return { (r.get("tableId",""),r.get("colId","")):r for r in rows }
def _tables(rows): return set(r.get("tableId","") for r in rows)

def make_diff(cur, base):
    out=[]; ic,ib=_index(cur),_index(base); tc,tb=_tables(cur),_tables(base)
    for t in sorted(tc-tb): out.append({"changeType":"ADDED_TABLE","tableId":t})
    for t in sorted(tb-tc): out.append({"changeType":"REMOVED_TABLE","tableId":t})
    for k in sorted(set(ic.keys())|set(ib.keys())):
        c,b=ic.get(k),ib.get(k); t,col=k
        if c and not b: out.append({"changeType":"ADDED_COL","tableId":t,"colId":col}); continue
        if b and not c: out.append({"changeType":"REMOVED_COL","tableId":t,"colId":col}); continue
        for f in FIELDS:
            ov,nv=b.get(f),c.get(f)
            if ov!=nv: out.append({"changeType":"CHANGED_FIELD","tableId":t,"colId":col,"field":f,"oldValue":str(ov),"newValue":str(nv)})
    return out

def save_json(o,p): open(p,"w",encoding="utf-8").write(json.dumps(o,ensure_ascii=False,indent=2))

app=Flask(__name__)

# ---- CORS ----
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
    return jsonify({"service":"grist-audit-cloud","ok":True,"hint":"use /status, /run, /result"})

@app.get("/status")
def status():
    busy = os.path.exists(LOCK_FILE)
    return jsonify({
        "ok": bool(API_KEY and DOC),
        "doc": DOC,
        "busy": busy,
        "has_current": os.path.exists(CUR_JSON),
        "has_diff": os.path.exists(DIFF_JSON),
        "time": datetime.utcnow().isoformat()+"Z"
    })

# ---- Thread background pour ne pas bloquer Render ----
def background_audit():
    open(LOCK_FILE,"w").write("running")
    try:
        base = json.load(open(CUR_JSON,encoding="utf-8")) if os.path.exists(CUR_JSON) else []
        cur = scan_schema()
        save_json(cur,CUR_JSON)
        diff = make_diff(cur,base)
        save_json(diff,DIFF_JSON)
    finally:
        if os.path.exists(LOCK_FILE): os.remove(LOCK_FILE)

@app.post("/run")
def run():
    if not API_KEY or not DOC:
        return jsonify({"ok":False,"error":"Missing GRIST_API_KEY or GRIST_DOC_ID"}),400
    if os.path.exists(LOCK_FILE):
        return jsonify({"ok":False,"busy":True,"error":"Audit already running"})
    threading.Thread(target=background_audit,daemon=True).start()
    return jsonify({"ok":True,"started":True,"time":datetime.utcnow().isoformat()+"Z"})

@app.get("/result")
def result():
    cur = json.load(open(CUR_JSON,encoding="utf-8")) if os.path.exists(CUR_JSON) else []
    diff = json.load(open(DIFF_JSON,encoding="utf-8")) if os.path.exists(DIFF_JSON) else []
    return jsonify({
        "summary": f"{len(diff)} changement(s).",
        "current_count": len(cur),
        "diff_count": len(diff),
        "diff": diff[:300]
    })

@app.get("/files/<path:fname>")
def files(fname):
    return send_from_directory(DATA_DIR,fname,mimetype="application/json")

if __name__=="__main__":
    port=int(os.getenv("PORT","8000"))
    app.run(host="0.0.0.0",port=port)
