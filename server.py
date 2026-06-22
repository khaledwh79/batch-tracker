#!/usr/bin/env python3
"""
Batch Tracker Pro — Pure Python server (no external packages)
Uses: sqlite3, http.server, threading, json — all built into Python 3
Real-time via Server-Sent Events (SSE) — works in all browsers
"""
import sqlite3, hashlib, json, threading, time, os, sys, secrets
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from datetime import datetime
import webbrowser

# ── Config ─────────────────────────────────────────────
PORT = int(os.environ.get('PORT', 3000))
DB_PATH = os.path.join(os.path.dirname(__file__), 'data', 'batchtracker.db')
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# ── SSE Subscribers ────────────────────────────────────
_subscribers = []
_sub_lock = threading.Lock()

def sse_broadcast(event, data):
    msg = f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"
    dead = []
    with _sub_lock:
        for q in _subscribers:
            try:
                q.put(msg)
            except:
                dead.append(q)
        for q in dead:
            _subscribers.remove(q)

# Simple thread-safe queue
class Queue:
    def __init__(self): self._items=[]; self._lock=threading.Lock(); self._cond=threading.Condition(self._lock)
    def put(self, item):
        with self._cond: self._items.append(item); self._cond.notify()
    def get(self, timeout=30):
        with self._cond:
            if not self._items: self._cond.wait(timeout)
            return self._items.pop(0) if self._items else None

# ── Database ───────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

db_lock = threading.Lock()
_db = get_db()

def db_exec(sql, params=()):
    with db_lock:
        _db.execute(sql, params); _db.commit()

def db_one(sql, params=()):
    with db_lock:
        return dict(_db.execute(sql, params).fetchone() or {})

def db_all(sql, params=()):
    with db_lock:
        rows = _db.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

# Schema
with db_lock:
    _db.executescript("""
    CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT,email TEXT UNIQUE,password TEXT,created_at TEXT DEFAULT(datetime('now')));
    CREATE TABLE IF NOT EXISTS sessions(token TEXT PRIMARY KEY,user_id INTEGER,user_name TEXT,user_email TEXT,created_at TEXT DEFAULT(datetime('now')));
    CREATE TABLE IF NOT EXISTS counters(name TEXT PRIMARY KEY,value INTEGER DEFAULT 0);
    CREATE TABLE IF NOT EXISTS materials(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT UNIQUE,unit TEXT,min_level REAL DEFAULT 0,created_at TEXT DEFAULT(datetime('now')));
    CREATE TABLE IF NOT EXISTS receipts(id INTEGER PRIMARY KEY AUTOINCREMENT,date TEXT,material_name TEXT,batch_no TEXT UNIQUE,qty REAL,unit TEXT,supplier TEXT,supplier_batch TEXT,production_date TEXT,expiry_date TEXT,inspection_status TEXT DEFAULT 'مقبول',receiver TEXT,notes TEXT,created_by INTEGER,created_at TEXT DEFAULT(datetime('now')));
    CREATE TABLE IF NOT EXISTS dispatches(id INTEGER PRIMARY KEY AUTOINCREMENT,date TEXT,material_name TEXT,batch_no TEXT,qty REAL,unit TEXT,issued_to TEXT,production_batch TEXT,responsible TEXT,notes TEXT,created_by INTEGER,created_at TEXT DEFAULT(datetime('now')));
    CREATE TABLE IF NOT EXISTS production(id INTEGER PRIMARY KEY AUTOINCREMENT,date TEXT,product_name TEXT,batch_no TEXT UNIQUE,qty REAL,unit TEXT,expiry_date TEXT,qc_result TEXT DEFAULT 'مقبول',responsible TEXT,notes TEXT,created_by INTEGER,created_at TEXT DEFAULT(datetime('now')));
    INSERT OR IGNORE INTO counters(name,value) VALUES('receipts',0);
    INSERT OR IGNORE INTO counters(name,value) VALUES('production',0);
    """)
    _db.commit()

def next_batch(name, prefix):
    with db_lock:
        _db.execute("UPDATE counters SET value=value+1 WHERE name=?", (name,))
        _db.commit()
        row = _db.execute("SELECT value FROM counters WHERE name=?", (name,)).fetchone()
        return f"{prefix}-{str(row[0]).zfill(5)}"

def hash_pw(pw): return hashlib.sha256(pw.encode()).hexdigest()

def get_session(token):
    if not token: return None
    return db_one("SELECT * FROM sessions WHERE token=?", (token,)) or None

# ── HTTP Handler ───────────────────────────────────────
class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args): pass  # Suppress default logs

    def get_token(self):
        cookies = self.headers.get('Cookie','')
        for c in cookies.split(';'):
            c=c.strip()
            if c.startswith('bt_session='):
                return c[11:]
        return None

    def send_json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header('Content-Type','application/json; charset=utf-8')
        self.send_header('Content-Length', len(body))
        self.end_headers()
        self.wfile.write(body)

    def send_err(self, msg, status=400):
        self.send_json({'error': msg}, status)

    def read_body(self):
        l = int(self.headers.get('Content-Length',0))
        if l: return json.loads(self.rfile.read(l))
        return {}

    def do_GET(self):
        p = urlparse(self.path).path
        # Static files
        if p == '/' or p == '/index.html':
            return self.serve_file(os.path.join(os.path.dirname(__file__),'public','index.html'),'text/html')
        # SSE endpoint
        if p == '/events':
            return self.handle_sse()
        # API
        tok = self.get_token()
        me = get_session(tok)
        if p == '/api/auth/me':
            if not me: return self.send_err('غير مصرح',401)
            return self.send_json({'id':me['user_id'],'name':me['user_name'],'email':me['user_email']})
        if not me: return self.send_err('غير مصرح',401)
        if p=='/api/materials': return self.send_json(db_all("SELECT * FROM materials ORDER BY name"))
        if p=='/api/receipts': return self.send_json(db_all("SELECT * FROM receipts ORDER BY created_at DESC"))
        if p=='/api/receipts/next-batch':
            row=db_one("SELECT value FROM counters WHERE name='receipts'")
            return self.send_json({'batch':f"RM-{str(row['value']+1).zfill(5)}"})
        if p=='/api/dispatches': return self.send_json(db_all("SELECT * FROM dispatches ORDER BY created_at DESC"))
        if p=='/api/production': return self.send_json(db_all("SELECT * FROM production ORDER BY created_at DESC"))
        if p=='/api/production/next-batch':
            row=db_one("SELECT value FROM counters WHERE name='production'")
            return self.send_json({'batch':f"FG-{str(row['value']+1).zfill(5)}"})
        self.send_err('Not found',404)

    def do_POST(self):
        p = urlparse(self.path).path
        body = self.read_body()
        # Auth — no session needed
        if p=='/api/auth/register':
            n=body.get('name','').strip(); e=body.get('email','').strip(); pw=body.get('password','')
            if not n or not e or not pw: return self.send_err('جميع الحقول مطلوبة')
            try:
                with db_lock:
                    _db.execute("INSERT INTO users(name,email,password) VALUES(?,?,?)",(n,e,hash_pw(pw)))
                    uid=_db.execute("SELECT last_insert_rowid()").fetchone()[0]
                    _db.commit()
                tok=secrets.token_hex(32)
                db_exec("INSERT INTO sessions(token,user_id,user_name,user_email) VALUES(?,?,?,?)",(tok,uid,n,e))
                self.send_response(200)
                self.send_header('Content-Type','application/json')
                self.send_header('Set-Cookie',f'bt_session={tok}; Path=/; HttpOnly; SameSite=None; Secure; Max-Age=604800')
                self.end_headers()
                self.wfile.write(json.dumps({'id':uid,'name':n,'email':e},ensure_ascii=False).encode())
            except Exception as ex:
                if 'UNIQUE' in str(ex): return self.send_err('البريد مستخدم بالفعل',409)
                return self.send_err(str(ex),500)
            return
        if p=='/api/auth/login':
            e=body.get('email',''); pw=body.get('password','')
            user=db_one("SELECT * FROM users WHERE email=? AND password=?",(e,hash_pw(pw)))
            if not user: return self.send_err('البريد أو كلمة المرور غير صحيحة',401)
            tok=secrets.token_hex(32)
            db_exec("INSERT INTO sessions(token,user_id,user_name,user_email) VALUES(?,?,?,?)",(tok,user['id'],user['name'],user['email']))
            self.send_response(200)
            self.send_header('Content-Type','application/json')
            self.send_header('Set-Cookie',f'bt_session={tok}; Path=/; HttpOnly; SameSite=None; Secure; Max-Age=604800')
            self.end_headers()
            self.wfile.write(json.dumps({'id':user['id'],'name':user['name'],'email':user['email']},ensure_ascii=False).encode())
            return
        if p=='/api/auth/logout':
            tok=self.get_token()
            if tok: db_exec("DELETE FROM sessions WHERE token=?",(tok,))
            self.send_response(200); self.send_header('Content-Type','application/json')
            self.send_header('Set-Cookie','bt_session=; Path=/; Max-Age=0')
            self.end_headers(); self.wfile.write(b'{"ok":true}'); return
        # Protected routes
        tok=self.get_token(); me=get_session(tok)
        if not me: return self.send_err('غير مصرح',401)
        uid=me['user_id']
        if p=='/api/materials':
            n=body.get('name','').strip(); u=body.get('unit','كيلو'); ml=body.get('min_level',0)
            if not n: return self.send_err('الاسم مطلوب')
            try:
                db_exec("INSERT INTO materials(name,unit,min_level) VALUES(?,?,?)",(n,u,ml))
                mat=db_one("SELECT * FROM materials WHERE name=?",(n,))
                sse_broadcast('materials:add',mat); return self.send_json(mat)
            except Exception as ex:
                if 'UNIQUE' in str(ex): return self.send_err('هذه المادة موجودة',409)
                return self.send_err(str(ex),500)
        if p=='/api/receipts':
            d=body.get('date',''); mn=body.get('material_name',''); q=body.get('qty',0)
            if not d or not mn or not q: return self.send_err('التاريخ والخامة والكمية مطلوبة')
            bn=next_batch('receipts','RM')
            db_exec("INSERT INTO receipts(date,material_name,batch_no,qty,unit,supplier,supplier_batch,production_date,expiry_date,inspection_status,receiver,notes,created_by) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (d,mn,bn,q,body.get('unit',''),body.get('supplier',''),body.get('supplier_batch',''),body.get('production_date',''),body.get('expiry_date',''),body.get('inspection_status','مقبول'),body.get('receiver',''),body.get('notes',''),uid))
            rec=db_one("SELECT * FROM receipts WHERE batch_no=?",(bn,))
            sse_broadcast('receipts:add',rec); return self.send_json(rec)
        if p=='/api/dispatches':
            d=body.get('date',''); mn=body.get('material_name',''); bn=body.get('batch_no',''); q=body.get('qty',0)
            if not d or not mn or not bn or not q: return self.send_err('البيانات الأساسية مطلوبة')
            db_exec("INSERT INTO dispatches(date,material_name,batch_no,qty,unit,issued_to,production_batch,responsible,notes,created_by) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (d,mn,bn,q,body.get('unit',''),body.get('issued_to',''),body.get('production_batch',''),body.get('responsible',''),body.get('notes',''),uid))
            with db_lock: rid=_db.execute("SELECT last_insert_rowid()").fetchone()[0]
            rec=db_one("SELECT * FROM dispatches WHERE id=?",(rid,))
            sse_broadcast('dispatches:add',rec); return self.send_json(rec)
        if p=='/api/production':
            d=body.get('date',''); pn=body.get('product_name',''); q=body.get('qty',0)
            if not d or not pn or not q: return self.send_err('التاريخ والمنتج والكمية مطلوبة')
            bn=next_batch('production','FG')
            db_exec("INSERT INTO production(date,product_name,batch_no,qty,unit,expiry_date,qc_result,responsible,notes,created_by) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (d,pn,bn,q,body.get('unit',''),body.get('expiry_date',''),body.get('qc_result','مقبول'),body.get('responsible',''),body.get('notes',''),uid))
            rec=db_one("SELECT * FROM production WHERE batch_no=?",(bn,))
            sse_broadcast('production:add',rec); return self.send_json(rec)
        self.send_err('Not found',404)

    def do_DELETE(self):
        p = urlparse(self.path).path
        tok=self.get_token(); me=get_session(tok)
        if not me: return self.send_err('غير مصرح',401)
        parts=p.strip('/').split('/')
        if len(parts)==3 and parts[0]=='api':
            tbl=parts[1]; rid=parts[2]
            tables={'materials':'materials','receipts':'receipts','dispatches':'dispatches','production':'production'}
            events={'materials':'materials:del','receipts':'receipts:del','dispatches':'dispatches:del','production':'production:del'}
            if tbl in tables:
                db_exec(f"DELETE FROM {tables[tbl]} WHERE id=?",(rid,))
                sse_broadcast(events[tbl],{'id':int(rid)})
                return self.send_json({'ok':True})
        self.send_err('Not found',404)

    def do_OPTIONS(self):
        self.send_response(200); self.send_header('Access-Control-Allow-Origin','*')
        self.send_header('Access-Control-Allow-Methods','GET,POST,DELETE,OPTIONS')
        self.send_header('Access-Control-Allow-Headers','Content-Type'); self.end_headers()

    def handle_sse(self):
        q = Queue()
        with _sub_lock: _subscribers.append(q)
        self.send_response(200)
        self.send_header('Content-Type','text/event-stream')
        self.send_header('Cache-Control','no-cache')
        self.send_header('Connection','keep-alive')
        self.send_header('Access-Control-Allow-Origin','*')
        self.end_headers()
        # Send initial ping
        try:
            self.wfile.write(b"event: ping\ndata: {}\n\n")
            self.wfile.flush()
        except: pass
        while True:
            try:
                msg = q.get(timeout=25)
                if msg is None:
                    # Heartbeat
                    self.wfile.write(b": heartbeat\n\n")
                    self.wfile.flush()
                    continue
                self.wfile.write(msg.encode('utf-8'))
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                break
            except Exception:
                break
        with _sub_lock:
            if q in _subscribers: _subscribers.remove(q)

    def serve_file(self, path, ct):
        try:
            with open(path,'rb') as f: data=f.read()
            self.send_response(200); self.send_header('Content-Type',ct+'; charset=utf-8')
            self.send_header('Content-Length',len(data)); self.end_headers(); self.wfile.write(data)
        except FileNotFoundError: self.send_err('Not found',404)

# ── Heartbeat thread ───────────────────────────────────
def heartbeat():
    while True:
        time.sleep(20)
        with _sub_lock:
            for q in list(_subscribers):
                try: q.put(None)
                except: pass
threading.Thread(target=heartbeat, daemon=True).start()

# ── Start ──────────────────────────────────────────────
if __name__ == '__main__':
    from socketserver import ThreadingMixIn
    class ThreadedServer(ThreadingMixIn, HTTPServer):
        daemon_threads = True
    srv = ThreadedServer(('0.0.0.0', PORT), Handler)
    print(f"""
╔══════════════════════════════════════════╗
║        Batch Tracker Pro ✅              ║
║    نظام تتبع الخامات والإنتاج            ║
╠══════════════════════════════════════════╣
║  🌐  http://localhost:{PORT}               ║
║  📱  شارك الرابط مع زملائك على نفس الشبكة ║
╚══════════════════════════════════════════╝
""")
    try:
        webbrowser.open(f'http://localhost:{PORT}')
    except: pass
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print('\n⛔ تم إيقاف البرنامج')
        srv.shutdown()
