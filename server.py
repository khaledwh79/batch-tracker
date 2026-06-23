#!/usr/bin/env python3
"""Batch Tracker Pro v3 — pure-stdlib HTTP server"""
import os, json, sqlite3, hashlib, secrets, threading, time, queue
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.parse import urlparse, parse_qs

PORT    = int(os.environ.get('PORT', 3000))
BASE    = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get('DB_PATH', os.path.join(BASE, 'data.db'))
PUBLIC  = os.path.join(BASE, 'public')

PREFIXES = {
    'receipts':      'RM',
    'dispatches':    'DSP',
    'pkg_receipts':  'PKG',
    'pkg_dispatches':'PKGD',
    'production':    'FG',
}

MIME = {
    '.html': 'text/html; charset=utf-8',
    '.js':   'application/javascript',
    '.css':  'text/css',
    '.ico':  'image/x-icon',
    '.png':  'image/png',
    '.json': 'application/json',
}

db_lock = threading.Lock()
sse_clients: list[queue.Queue] = []
sse_lock = threading.Lock()

# ── DB ──────────────────────────────────────────────────────────────────────────
def get_db():
    c = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=10)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA foreign_keys=ON")
    return c

def init_db():
    os.makedirs(os.path.dirname(DB_PATH) if os.path.dirname(DB_PATH) else '.', exist_ok=True)
    with db_lock:
        c = get_db()
        c.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            pw_hash TEXT NOT NULL,
            role TEXT DEFAULT 'user',
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS sessions (
            token TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            username TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS counters (
            name TEXT PRIMARY KEY,
            val  INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS materials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            unit TEXT DEFAULT 'kg',
            min_level REAL DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS pkg_materials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            unit TEXT DEFAULT 'piece',
            min_level REAL DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS receipts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_no TEXT UNIQUE,
            material_name TEXT NOT NULL,
            supplier TEXT,
            supplier_batch TEXT,
            qty REAL NOT NULL,
            unit TEXT DEFAULT 'kg',
            lot_no TEXT,
            expiry_date TEXT,
            received_date TEXT DEFAULT (date('now')),
            notes TEXT,
            created_by TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS dispatches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_no TEXT UNIQUE,
            material_name TEXT NOT NULL,
            qty REAL NOT NULL,
            unit TEXT DEFAULT 'kg',
            purpose TEXT,
            fg_batch TEXT,
            dispatch_date TEXT DEFAULT (date('now')),
            notes TEXT,
            created_by TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS pkg_receipts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_no TEXT UNIQUE,
            material_name TEXT NOT NULL,
            supplier TEXT,
            supplier_batch TEXT,
            qty REAL NOT NULL,
            unit TEXT DEFAULT 'piece',
            lot_no TEXT,
            expiry_date TEXT,
            received_date TEXT DEFAULT (date('now')),
            notes TEXT,
            created_by TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS pkg_dispatches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_no TEXT UNIQUE,
            material_name TEXT NOT NULL,
            qty REAL NOT NULL,
            unit TEXT DEFAULT 'piece',
            purpose TEXT,
            fg_batch TEXT,
            dispatch_date TEXT DEFAULT (date('now')),
            notes TEXT,
            created_by TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS production (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_no TEXT UNIQUE,
            product_name TEXT NOT NULL,
            qty REAL NOT NULL,
            unit TEXT DEFAULT 'kg',
            production_date TEXT DEFAULT (date('now')),
            expiry_date TEXT,
            notes TEXT,
            created_by TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS batch_customers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_no TEXT NOT NULL,
            customer TEXT NOT NULL,
            qty REAL,
            unit TEXT DEFAULT 'kg',
            dispatch_date TEXT DEFAULT (date('now')),
            invoice_no TEXT,
            notes TEXT,
            created_by TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        """)
        c.commit()
        ph = hashlib.sha256(b'admin').hexdigest()
        try:
            c.execute("INSERT INTO users (username,pw_hash,role) VALUES ('admin',?,'admin')", (ph,))
            c.commit()
        except sqlite3.IntegrityError:
            pass
        for nm in PREFIXES:
            try:
                c.execute("INSERT INTO counters (name,val) VALUES (?,0)", (nm,))
                c.commit()
            except sqlite3.IntegrityError:
                pass
        for sql in [
            "ALTER TABLE receipts ADD COLUMN supplier_batch TEXT",
            "ALTER TABLE dispatches ADD COLUMN fg_batch TEXT",
            "ALTER TABLE pkg_receipts ADD COLUMN supplier_batch TEXT",
            "ALTER TABLE pkg_dispatches ADD COLUMN fg_batch TEXT",
        ]:
            try: c.execute(sql); c.commit()
            except: pass
        c.close()

def db_all(sql, params=()):
    with db_lock:
        c = get_db()
        rows = c.execute(sql, params).fetchall()
        c.close()
        return [dict(r) for r in rows]

def db_one(sql, params=()):
    with db_lock:
        c = get_db()
        row = c.execute(sql, params).fetchone()
        c.close()
        return dict(row) if row else {}

def db_exec(sql, params=()):
    with db_lock:
        c = get_db()
        cur = c.execute(sql, params)
        c.commit()
        lid = cur.lastrowid
        c.close()
        return lid

def next_batch(table):
    prefix = PREFIXES.get(table, 'XX')
    with db_lock:
        c = get_db()
        c.execute("UPDATE counters SET val=val+1 WHERE name=?", (table,))
        c.commit()
        row = c.execute("SELECT val FROM counters WHERE name=?", (table,)).fetchone()
        c.close()
        return f"{prefix}-{row['val']:05d}"

def broadcast(event_type: str, data: dict):
    msg = f"event: {event_type}\ndata: {json.dumps(data)}\n\n"
    with sse_lock:
        dead = []
        for q in sse_clients:
            try: q.put_nowait(msg)
            except queue.Full: dead.append(q)
        for q in dead: sse_clients.remove(q)

def check_session(handler):
    cookie = handler.headers.get('Cookie', '')
    for part in cookie.split(';'):
        part = part.strip()
        if part.startswith('session='):
            token = part[8:]
            row = db_one("SELECT username FROM sessions WHERE token=?", (token,))
            if row: return row['username']
    return None

def set_cookie(handler, token):
    secure = handler.headers.get('X-Forwarded-Proto', '') == 'https'
    same = 'None; Secure' if secure else 'Lax'
    handler.send_header('Set-Cookie', f'session={token}; Path=/; HttpOnly; SameSite={same}')

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args): pass

    def send_json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_err(self, msg, status=400):
        self.send_json({'error': msg}, status)

    def read_body(self):
        length = int(self.headers.get('Content-Length', 0))
        return json.loads(self.rfile.read(length)) if length else {}

    def serve_static(self, path):
        if '..' in path: self.send_err('forbidden', 403); return
        fp = os.path.join(PUBLIC, path.lstrip('/'))
        if os.path.isdir(fp): fp = os.path.join(fp, 'index.html')
        if not os.path.exists(fp): self.send_err('not found', 404); return
        ext = os.path.splitext(fp)[1]
        mime = MIME.get(ext, 'application/octet-stream')
        with open(fp, 'rb') as f: body = f.read()
        self.send_response(200)
        self.send_header('Content-Type', mime)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        path   = parsed.path.rstrip('/')
        qs     = parse_qs(parsed.query)

        if path == '/api/events':
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.send_header('Cache-Control', 'no-cache')
            self.send_header('Connection', 'keep-alive')
            self.end_headers()
            q = queue.Queue(maxsize=50)
            with sse_lock: sse_clients.append(q)
            try:
                self.wfile.write(b': connected\n\n'); self.wfile.flush()
                while True:
                    try:
                        msg = q.get(timeout=25)
                        self.wfile.write(msg.encode()); self.wfile.flush()
                    except queue.Empty:
                        self.wfile.write(b': ping\n\n'); self.wfile.flush()
            except Exception: pass
            finally:
                with sse_lock:
                    if q in sse_clients: sse_clients.remove(q)
            return

        if path.startswith('/api/') and path not in ('/api/login',):
            user = check_session(self)
            if not user: self.send_err('unauthorized', 401); return
        else:
            user = check_session(self)

        if path == '/api/me':
            u = check_session(self)
            return self.send_json({'username': u} if u else {'username': None})

        if path == '/api/dashboard':
            rm_count  = db_one("SELECT COUNT(*) as c FROM receipts")['c']
            dsp_count = db_one("SELECT COUNT(*) as c FROM dispatches")['c']
            pkg_r     = db_one("SELECT COUNT(*) as c FROM pkg_receipts")['c']
            pkg_d     = db_one("SELECT COUNT(*) as c FROM pkg_dispatches")['c']
            prod      = db_one("SELECT COUNT(*) as c FROM production")['c']
            low_rm    = db_all("""
                SELECT m.name, m.min_level,
                    COALESCE((SELECT SUM(qty) FROM receipts WHERE material_name=m.name),0)
                    - COALESCE((SELECT SUM(qty) FROM dispatches WHERE material_name=m.name),0) as bal
                FROM materials m WHERE bal < m.min_level AND m.min_level > 0""")
            low_pkg   = db_all("""
                SELECT m.name, m.min_level,
                    COALESCE((SELECT SUM(qty) FROM pkg_receipts WHERE material_name=m.name),0)
                    - COALESCE((SELECT SUM(qty) FROM pkg_dispatches WHERE material_name=m.name),0) as bal
                FROM pkg_materials m WHERE bal < m.min_level AND m.min_level > 0""")
            expiry_rm = db_all("""SELECT 'خامات' as type, batch_no, material_name as name,
                expiry_date, qty, unit FROM receipts
                WHERE expiry_date!='' AND expiry_date IS NOT NULL
                AND date(expiry_date) BETWEEN date('now') AND date('now','+30 days')
                ORDER BY expiry_date""")
            expiry_fg = db_all("""SELECT 'منتج' as type, batch_no, product_name as name,
                expiry_date, qty, unit FROM production
                WHERE expiry_date!='' AND expiry_date IS NOT NULL
                AND date(expiry_date) BETWEEN date('now') AND date('now','+30 days')
                ORDER BY expiry_date""")
            return self.send_json({
                'rm_receipts': rm_count, 'rm_dispatches': dsp_count,
                'pkg_receipts': pkg_r, 'pkg_dispatches': pkg_d, 'production': prod,
                'low_rm': low_rm, 'low_pkg': low_pkg,
                'expiry_rm': expiry_rm, 'expiry_fg': expiry_fg,
            })

        if path == '/api/materials':
            return self.send_json(db_all("SELECT * FROM materials ORDER BY name"))
        if path == '/api/pkg_materials':
            return self.send_json(db_all("SELECT * FROM pkg_materials ORDER BY name"))

        if path == '/api/receipts':
            return self.send_json(db_all("SELECT * FROM receipts ORDER BY created_at DESC"))
        if path == '/api/pkg_receipts':
            return self.send_json(db_all("SELECT * FROM pkg_receipts ORDER BY created_at DESC"))

        if path == '/api/dispatches':
            return self.send_json(db_all("SELECT * FROM dispatches ORDER BY created_at DESC"))
        if path == '/api/pkg_dispatches':
            return self.send_json(db_all("SELECT * FROM pkg_dispatches ORDER BY created_at DESC"))

        if path == '/api/inventory':
            rows = db_all("SELECT material_name, MAX(unit) as unit, SUM(qty) as total_in FROM receipts GROUP BY material_name ORDER BY material_name")
            for r in rows:
                out = db_one("SELECT COALESCE(SUM(qty),0) as s FROM dispatches WHERE material_name=?", (r['material_name'],))
                ml  = db_one("SELECT min_level FROM materials WHERE name=?", (r['material_name'],))
                r['total_out'] = out.get('s', 0) or 0
                r['balance']   = (r['total_in'] or 0) - r['total_out']
                r['min_level'] = ml.get('min_level', 0) or 0
            return self.send_json(rows)

        if path == '/api/pkg_inventory':
            rows = db_all("SELECT material_name, MAX(unit) as unit, SUM(qty) as total_in FROM pkg_receipts GROUP BY material_name ORDER BY material_name")
            for r in rows:
                out = db_one("SELECT COALESCE(SUM(qty),0) as s FROM pkg_dispatches WHERE material_name=?", (r['material_name'],))
                ml  = db_one("SELECT min_level FROM pkg_materials WHERE name=?", (r['material_name'],))
                r['total_out'] = out.get('s', 0) or 0
                r['balance']   = (r['total_in'] or 0) - r['total_out']
                r['min_level'] = ml.get('min_level', 0) or 0
            return self.send_json(rows)

        if path == '/api/production':
            rows = db_all("SELECT * FROM production ORDER BY created_at DESC")
            for r in rows:
                r['customers'] = db_all("SELECT * FROM batch_customers WHERE batch_no=? ORDER BY created_at", (r['batch_no'],))
                r['dispatches_used'] = db_all("SELECT batch_no,material_name,qty,unit FROM dispatches WHERE fg_batch=?", (r['batch_no'],))
                r['pkg_used'] = db_all("SELECT batch_no,material_name,qty,unit FROM pkg_dispatches WHERE fg_batch=?", (r['batch_no'],))
            return self.send_json(rows)

        if path == '/api/batch_customers':
            batch_no = qs.get('batch_no', [''])[0]
            if batch_no:
                return self.send_json(db_all("SELECT * FROM batch_customers WHERE batch_no=? ORDER BY created_at", (batch_no,)))
            return self.send_json(db_all("SELECT * FROM batch_customers ORDER BY created_at DESC"))

        if path == '/api/report/raw':
            rows = db_all("""
                SELECT material_name,
                    COALESCE(SUM(CASE WHEN t='in' THEN qty END),0) as total_in,
                    COALESCE(SUM(CASE WHEN t='out' THEN qty END),0) as total_out,
                    MAX(unit) as unit
                FROM (
                    SELECT material_name, qty, unit, 'in' as t FROM receipts
                    UNION ALL
                    SELECT material_name, qty, unit, 'out' as t FROM dispatches
                ) GROUP BY material_name ORDER BY material_name""")
            for r in rows: r['balance'] = r['total_in'] - r['total_out']
            return self.send_json(rows)

        if path == '/api/report/pkg':
            rows = db_all("""
                SELECT material_name,
                    COALESCE(SUM(CASE WHEN t='in' THEN qty END),0) as total_in,
                    COALESCE(SUM(CASE WHEN t='out' THEN qty END),0) as total_out,
                    MAX(unit) as unit
                FROM (
                    SELECT material_name, qty, unit, 'in' as t FROM pkg_receipts
                    UNION ALL
                    SELECT material_name, qty, unit, 'out' as t FROM pkg_dispatches
                ) GROUP BY material_name ORDER BY material_name""")
            for r in rows: r['balance'] = r['total_in'] - r['total_out']
            return self.send_json(rows)

        if path == '/api/report/prod':
            rows = db_all("SELECT * FROM production ORDER BY production_date DESC")
            for r in rows:
                r['customers'] = db_all("SELECT * FROM batch_customers WHERE batch_no=?", (r['batch_no'],))
                r['total_distributed'] = sum(c['qty'] or 0 for c in r['customers'])
            return self.send_json(rows)

        # Forward Trace: RM batch/material → dispatches → FG → customers
        if path == '/api/trace/forward':
            q_val = qs.get('q', [''])[0].strip()
            if not q_val: return self.send_json({'query': '', 'type': 'forward', 'chain': []})
            receipts_found = db_all("""
                SELECT * FROM receipts
                WHERE batch_no LIKE ? OR material_name LIKE ? OR supplier_batch LIKE ?
                ORDER BY received_date DESC""",
                (f'%{q_val}%', f'%{q_val}%', f'%{q_val}%'))
            chain = []
            seen = set()
            for receipt in receipts_found:
                dispatches_found = db_all(
                    "SELECT * FROM dispatches WHERE material_name=? ORDER BY dispatch_date",
                    (receipt['material_name'],))
                for d in dispatches_found:
                    key = (receipt['batch_no'], d['batch_no'])
                    if key in seen: continue
                    seen.add(key)
                    entry = {'receipt': receipt, 'dispatch': d, 'production': None, 'customers': []}
                    if d.get('fg_batch'):
                        prod = db_one("SELECT * FROM production WHERE batch_no=?", (d['fg_batch'],))
                        if prod:
                            entry['production'] = prod
                            entry['customers'] = db_all("SELECT * FROM batch_customers WHERE batch_no=?", (d['fg_batch'],))
                    chain.append(entry)
            return self.send_json({'query': q_val, 'type': 'forward', 'chain': chain})

        # Backward Trace: FG batch/product/customer → dispatches → RM receipts
        if path == '/api/trace/backward':
            q_val = qs.get('q', [''])[0].strip()
            if not q_val: return self.send_json({'query': '', 'type': 'backward', 'chain': []})
            prods_found = db_all("""
                SELECT * FROM production WHERE batch_no LIKE ? OR product_name LIKE ?
                ORDER BY production_date DESC""",
                (f'%{q_val}%', f'%{q_val}%'))
            cust_batches = db_all(
                "SELECT DISTINCT batch_no FROM batch_customers WHERE customer LIKE ?",
                (f'%{q_val}%',))
            for cb in cust_batches:
                p = db_one("SELECT * FROM production WHERE batch_no=?", (cb['batch_no'],))
                if p and not any(x['batch_no'] == p['batch_no'] for x in prods_found):
                    prods_found.append(p)
            chain = []
            for prod in prods_found:
                customers = db_all("SELECT * FROM batch_customers WHERE batch_no=? ORDER BY created_at", (prod['batch_no'],))
                raw_dsps = db_all("SELECT * FROM dispatches WHERE fg_batch=? ORDER BY dispatch_date", (prod['batch_no'],))
                pkg_dsps = db_all("SELECT * FROM pkg_dispatches WHERE fg_batch=? ORDER BY dispatch_date", (prod['batch_no'],))
                raw_materials = []
                for d in raw_dsps:
                    recs = db_all("SELECT * FROM receipts WHERE material_name=? ORDER BY received_date DESC LIMIT 5", (d['material_name'],))
                    raw_materials.append({'dispatch': d, 'receipts': recs})
                pkg_materials = []
                for d in pkg_dsps:
                    recs = db_all("SELECT * FROM pkg_receipts WHERE material_name=? ORDER BY received_date DESC LIMIT 5", (d['material_name'],))
                    pkg_materials.append({'dispatch': d, 'receipts': recs})
                chain.append({
                    'production': prod, 'customers': customers,
                    'raw_materials': raw_materials, 'pkg_materials': pkg_materials,
                })
            return self.send_json({'query': q_val, 'type': 'backward', 'chain': chain})

        if path == '/api/material_history':
            name = qs.get('name', [''])[0].strip()
            if not name: return self.send_json({'receipts': [], 'dispatches': [], 'balance': 0})
            receipts  = db_all("SELECT * FROM receipts WHERE material_name=? ORDER BY received_date DESC", (name,))
            dispatches = db_all("SELECT * FROM dispatches WHERE material_name=? ORDER BY dispatch_date DESC", (name,))
            balance = sum(r['qty'] or 0 for r in receipts) - sum(d['qty'] or 0 for d in dispatches)
            return self.send_json({'name': name, 'receipts': receipts, 'dispatches': dispatches, 'balance': balance})

        if path == '/api/trace':
            q_val = qs.get('q', [''])[0].strip()
            if not q_val: return self.send_json({'results': []})
            results = []
            for r in db_all("SELECT * FROM receipts WHERE batch_no LIKE ? OR material_name LIKE ? OR supplier_batch LIKE ?", (f'%{q_val}%', f'%{q_val}%', f'%{q_val}%')):
                results.append({'type': 'receipt', 'data': r})
            for d in db_all("SELECT * FROM dispatches WHERE batch_no LIKE ? OR material_name LIKE ? OR fg_batch LIKE ?", (f'%{q_val}%', f'%{q_val}%', f'%{q_val}%')):
                results.append({'type': 'dispatch', 'data': d})
            for p in db_all("SELECT * FROM production WHERE batch_no LIKE ? OR product_name LIKE ?", (f'%{q_val}%', f'%{q_val}%')):
                p['customers'] = db_all("SELECT * FROM batch_customers WHERE batch_no=?", (p['batch_no'],))
                results.append({'type': 'production', 'data': p})
            for bc in db_all("SELECT * FROM batch_customers WHERE customer LIKE ?", (f'%{q_val}%',)):
                results.append({'type': 'customer', 'data': bc})
            return self.send_json({'results': results})

        self.serve_static(path or '/')

    def do_POST(self):
        parsed = urlparse(self.path)
        path   = parsed.path.rstrip('/')
        user   = check_session(self)

        if path == '/api/login':
            b = self.read_body()
            row = db_one("SELECT id,pw_hash FROM users WHERE username=?", (b.get('username',''),))
            if row and row['pw_hash'] == hashlib.sha256(b.get('password','').encode()).hexdigest():
                tok = secrets.token_hex(32)
                db_exec("INSERT INTO sessions (token,user_id,username) VALUES (?,?,?)", (tok, row['id'], b['username']))
                self.send_response(200)
                set_cookie(self, tok)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'ok': True, 'username': b['username']}).encode())
            else:
                self.send_err('invalid credentials', 401)
            return

        if not user: self.send_err('unauthorized', 401); return
        b = self.read_body()

        if path == '/api/logout':
            cookie = self.headers.get('Cookie', '')
            for part in cookie.split(';'):
                part = part.strip()
                if part.startswith('session='): db_exec("DELETE FROM sessions WHERE token=?", (part[8:],))
            return self.send_json({'ok': True})

        if path == '/api/materials':
            db_exec("INSERT OR IGNORE INTO materials (name,unit,min_level) VALUES (?,?,?)",
                    (b['name'], b.get('unit','kg'), b.get('min_level',0)))
            broadcast('materials_update', {})
            return self.send_json({'ok': True})

        if path == '/api/pkg_materials':
            db_exec("INSERT OR IGNORE INTO pkg_materials (name,unit,min_level) VALUES (?,?,?)",
                    (b['name'], b.get('unit','piece'), b.get('min_level',0)))
            broadcast('materials_update', {})
            return self.send_json({'ok': True})

        if path == '/api/receipts':
            bn = next_batch('receipts')
            db_exec("""INSERT INTO receipts
                (batch_no,material_name,supplier,supplier_batch,qty,unit,lot_no,expiry_date,received_date,notes,created_by)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (bn, b['material_name'], b.get('supplier',''), b.get('supplier_batch',''),
                 b['qty'], b.get('unit','kg'), b.get('lot_no',''),
                 b.get('expiry_date',''), b.get('received_date',''), b.get('notes',''), user))
            db_exec("INSERT OR IGNORE INTO materials (name,unit) VALUES (?,?)", (b['material_name'], b.get('unit','kg')))
            broadcast('receipts_update', {'batch_no': bn})
            return self.send_json({'ok': True, 'batch_no': bn})

        if path == '/api/pkg_receipts':
            bn = next_batch('pkg_receipts')
            db_exec("""INSERT INTO pkg_receipts
                (batch_no,material_name,supplier,supplier_batch,qty,unit,lot_no,expiry_date,received_date,notes,created_by)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (bn, b['material_name'], b.get('supplier',''), b.get('supplier_batch',''),
                 b['qty'], b.get('unit','piece'), b.get('lot_no',''),
                 b.get('expiry_date',''), b.get('received_date',''), b.get('notes',''), user))
            db_exec("INSERT OR IGNORE INTO pkg_materials (name,unit) VALUES (?,?)", (b['material_name'], b.get('unit','piece')))
            broadcast('pkg_receipts_update', {'batch_no': bn})
            return self.send_json({'ok': True, 'batch_no': bn})

        if path == '/api/dispatches':
            bn = next_batch('dispatches')
            db_exec("""INSERT INTO dispatches
                (batch_no,material_name,qty,unit,purpose,fg_batch,dispatch_date,notes,created_by)
                VALUES (?,?,?,?,?,?,?,?,?)""",
                (bn, b['material_name'], b['qty'], b.get('unit','kg'),
                 b.get('purpose',''), b.get('fg_batch',''),
                 b.get('dispatch_date',''), b.get('notes',''), user))
            broadcast('dispatches_update', {'batch_no': bn})
            return self.send_json({'ok': True, 'batch_no': bn})

        if path == '/api/pkg_dispatches':
            bn = next_batch('pkg_dispatches')
            db_exec("""INSERT INTO pkg_dispatches
                (batch_no,material_name,qty,unit,purpose,fg_batch,dispatch_date,notes,created_by)
                VALUES (?,?,?,?,?,?,?,?,?)""",
                (bn, b['material_name'], b['qty'], b.get('unit','piece'),
                 b.get('purpose',''), b.get('fg_batch',''),
                 b.get('dispatch_date',''), b.get('notes',''), user))
            broadcast('pkg_dispatches_update', {'batch_no': bn})
            return self.send_json({'ok': True, 'batch_no': bn})

        if path == '/api/production':
            bn = next_batch('production')
            db_exec("""INSERT INTO production
                (batch_no,product_name,qty,unit,production_date,expiry_date,notes,created_by)
                VALUES (?,?,?,?,?,?,?,?)""",
                (bn, b['product_name'], b['qty'], b.get('unit','kg'),
                 b.get('production_date',''), b.get('expiry_date',''), b.get('notes',''), user))
            broadcast('production_update', {'batch_no': bn})
            return self.send_json({'ok': True, 'batch_no': bn})

        if path == '/api/batch_customers':
            db_exec("""INSERT INTO batch_customers
                (batch_no,customer,qty,unit,dispatch_date,invoice_no,notes,created_by)
                VALUES (?,?,?,?,?,?,?,?)""",
                (b['batch_no'], b['customer'], b.get('qty'), b.get('unit','kg'),
                 b.get('dispatch_date',''), b.get('invoice_no',''), b.get('notes',''), user))
            broadcast('production_update', {'batch_no': b['batch_no']})
            return self.send_json({'ok': True})

        self.send_err('not found', 404)

    def do_DELETE(self):
        parsed = urlparse(self.path)
        path   = parsed.path.rstrip('/')
        user   = check_session(self)
        if not user: self.send_err('unauthorized', 401); return
        parts = path.split('/')
        if len(parts) >= 4:
            table = parts[2]; rid = parts[3]
            tables = {
                'receipts': 'receipts', 'dispatches': 'dispatches',
                'pkg_receipts': 'pkg_receipts', 'pkg_dispatches': 'pkg_dispatches',
                'production': 'production', 'materials': 'materials',
                'pkg_materials': 'pkg_materials', 'batch_customers': 'batch_customers',
            }
            if table in tables:
                db_exec(f"DELETE FROM {tables[table]} WHERE id=?", (rid,))
                broadcast(f'{table}_update', {})
                return self.send_json({'ok': True})
        self.send_err('not found', 404)

class ThreadedServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True

def run():
    init_db()
    server = ThreadedServer(('0.0.0.0', PORT), Handler)
    print(f"Batch Tracker Pro v3 — http://0.0.0.0:{PORT}")
    server.serve_forever()

if __name__ == '__main__':
    run()
