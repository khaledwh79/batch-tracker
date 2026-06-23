#!/usr/bin/env python3
"""Batch Tracker Pro v2 – pure-stdlib HTTP server"""
import os, json, sqlite3, hashlib, secrets, threading, time, queue
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.parse import urlparse, parse_qs

# ── Config ──────────────────────────────────────────────────────────────────
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

# ── DB ───────────────────────────────────────────────────────────────────────
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
            dispatch_date TEXT DEFAULT (date('now')),
            notes TEXT,
            created_by TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS production (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_no TEXT UNIQUE,
            product_name TEXT NOT NULL,
            customer TEXT,
            qty REAL NOT NULL,
            unit TEXT DEFAULT 'kg',
            production_date TEXT DEFAULT (date('now')),
            expiry_date TEXT,
            notes TEXT,
            created_by TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        );
        """)
        c.commit()
        # default admin
        ph = hashlib.sha256(b'admin').hexdigest()
        try:
            c.execute("INSERT INTO users (username,pw_hash,role) VALUES ('admin',?,'admin')", (ph,))
            c.commit()
        except sqlite3.IntegrityError:
            pass
        # counters
        for nm in PREFIXES:
            try:
                c.execute("INSERT INTO counters (name,val) VALUES (?,0)", (nm,))
                c.commit()
            except sqlite3.IntegrityError:
                pass
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

# ── SSE ──────────────────────────────────────────────────────────────────────
def broadcast(event_type: str, data: dict):
    msg = f"event: {event_type}\ndata: {json.dumps(data)}\n\n"
    with sse_lock:
        dead = []
        for q in sse_clients:
            try:
                q.put_nowait(msg)
            except queue.Full:
                dead.append(q)
        for q in dead:
            sse_clients.remove(q)

# ── Auth ─────────────────────────────────────────────────────────────────────
def check_session(handler):
    cookie = handler.headers.get('Cookie', '')
    for part in cookie.split(';'):
        part = part.strip()
        if part.startswith('session='):
            token = part[8:]
            row = db_one("SELECT username FROM sessions WHERE token=?", (token,))
            if row:
                return row['username']
    return None

def set_cookie(handler, token):
    secure = handler.headers.get('X-Forwarded-Proto', '') == 'https'
    same = 'None; Secure' if secure else 'Lax'
    handler.send_header('Set-Cookie',
        f'session={token}; Path=/; HttpOnly; SameSite={same}')

# ── Handler ──────────────────────────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # silence logs

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
        if '..' in path:
            self.send_err('forbidden', 403); return
        fp = os.path.join(PUBLIC, path.lstrip('/'))
        if os.path.isdir(fp):
            fp = os.path.join(fp, 'index.html')
        if not os.path.exists(fp):
            self.send_err('not found', 404); return
        ext = os.path.splitext(fp)[1]
        mime = MIME.get(ext, 'application/octet-stream')
        with open(fp, 'rb') as f:
            body = f.read()
        self.send_response(200)
        self.send_header('Content-Type', mime)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # ── GET ──────────────────────────────────────────────────────────────────
    def do_GET(self):
        parsed = urlparse(self.path)
        path   = parsed.path.rstrip('/')
        qs     = parse_qs(parsed.query)

        # SSE
        if path == '/api/events':
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.send_header('Cache-Control', 'no-cache')
            self.send_header('Connection', 'keep-alive')
            self.end_headers()
            q = queue.Queue(maxsize=50)
            with sse_lock:
                sse_clients.append(q)
            try:
                self.wfile.write(b': connected\n\n')
                self.wfile.flush()
                while True:
                    try:
                        msg = q.get(timeout=25)
                        self.wfile.write(msg.encode())
                        self.wfile.flush()
                    except queue.Empty:
                        self.wfile.write(b': ping\n\n')
                        self.wfile.flush()
            except Exception:
                pass
            finally:
                with sse_lock:
                    if q in sse_clients:
                        sse_clients.remove(q)
            return

        # Auth check for API
        if path.startswith('/api/') and path not in ('/api/login',):
            user = check_session(self)
            if not user:
                self.send_err('unauthorized', 401); return
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
                FROM materials m
                WHERE bal < m.min_level AND m.min_level > 0""")
            low_pkg   = db_all("""
                SELECT m.name, m.min_level,
                    COALESCE((SELECT SUM(qty) FROM pkg_receipts WHERE material_name=m.name),0)
                    - COALESCE((SELECT SUM(qty) FROM pkg_dispatches WHERE material_name=m.name),0) as bal
                FROM pkg_materials m
                WHERE bal < m.min_level AND m.min_level > 0""")
            return self.send_json({
                'rm_receipts': rm_count, 'rm_dispatches': dsp_count,
                'pkg_receipts': pkg_r, 'pkg_dispatches': pkg_d,
                'production': prod,
                'low_rm': low_rm, 'low_pkg': low_pkg,
            })

        # Materials
        if path == '/api/materials':
            return self.send_json(db_all("SELECT * FROM materials ORDER BY name"))
        if path == '/api/pkg_materials':
            return self.send_json(db_all("SELECT * FROM pkg_materials ORDER BY name"))

        # Receipts
        if path == '/api/receipts':
            return self.send_json(db_all("SELECT * FROM receipts ORDER BY created_at DESC"))
        if path == '/api/pkg_receipts':
            return self.send_json(db_all("SELECT * FROM pkg_receipts ORDER BY created_at DESC"))

        # Dispatches
        if path == '/api/dispatches':
            return self.send_json(db_all("SELECT * FROM dispatches ORDER BY created_at DESC"))
        if path == '/api/pkg_dispatches':
            return self.send_json(db_all("SELECT * FROM pkg_dispatches ORDER BY created_at DESC"))

        # Inventory
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

        # Production
        if path == '/api/production':
            return self.send_json(db_all("SELECT * FROM production ORDER BY created_at DESC"))

        # Reports
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
                )
                GROUP BY material_name ORDER BY material_name""")
            for r in rows:
                r['balance'] = r['total_in'] - r['total_out']
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
                )
                GROUP BY material_name ORDER BY material_name""")
            for r in rows:
                r['balance'] = r['total_in'] - r['total_out']
            return self.send_json(rows)

        if path == '/api/report/prod':
            return self.send_json(db_all(
                "SELECT product_name, customer, SUM(qty) as total_qty, MAX(unit) as unit, COUNT(*) as batches "
                "FROM production GROUP BY product_name, customer ORDER BY product_name"))

        # Trace
        if path == '/api/trace':
            q_val = qs.get('q', [''])[0].strip()
            if not q_val:
                return self.send_json({'results': []})
            like = f'%{q_val}%'
            results = []
            for tbl, label in [('receipts','استلام خام'), ('dispatches','صرف خام'),
                                ('pkg_receipts','استلام تعبئة'), ('pkg_dispatches','صرف تعبئة'),
                                ('production','إنتاج')]:
                cols = "batch_no, material_name" if tbl != 'production' else "batch_no, product_name as material_name"
                rws = db_all(f"SELECT {cols}, created_at FROM {tbl} WHERE batch_no LIKE ? OR material_name LIKE ?", (like, like))
                for r in rws:
                    r['source'] = label
                results.extend(rws)
            return self.send_json({'results': results})

        # Static files
        if not path.startswith('/api'):
            return self.serve_static(path or '/')

        self.send_err('not found', 404)

    # ── POST ─────────────────────────────────────────────────────────────────
    def do_POST(self):
        path = urlparse(self.path).path.rstrip('/')
        data = self.read_body()

        # Login
        if path == '/api/login':
            u = data.get('username', '').strip()
            p = data.get('password', '')
            ph = hashlib.sha256(p.encode()).hexdigest()
            row = db_one("SELECT id,username FROM users WHERE username=? AND pw_hash=?", (u, ph))
            if not row:
                return self.send_err('بيانات الدخول غير صحيحة', 401)
            token = secrets.token_hex(32)
            db_exec("INSERT INTO sessions (token,user_id,username) VALUES (?,?,?)",
                    (token, row['id'], row['username']))
            self.send_response(200)
            set_cookie(self, token)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'ok': True, 'username': row['username']}).encode())
            return

        # Auth required for everything else
        user = check_session(self)
        if not user:
            return self.send_err('unauthorized', 401)

        if path == '/api/logout':
            cookie = self.headers.get('Cookie', '')
            for part in cookie.split(';'):
                part = part.strip()
                if part.startswith('session='):
                    db_exec("DELETE FROM sessions WHERE token=?", (part[8:],))
            return self.send_json({'ok': True})

        # Materials
        if path == '/api/materials':
            nm = data.get('name', '').strip()
            if not nm: return self.send_err('name required')
            db_exec("INSERT OR IGNORE INTO materials (name,unit,min_level) VALUES (?,?,?)",
                    (nm, data.get('unit','kg'), float(data.get('min_level',0))))
            broadcast('update', {'type':'materials'})
            return self.send_json({'ok': True})

        if path == '/api/pkg_materials':
            nm = data.get('name', '').strip()
            if not nm: return self.send_err('name required')
            db_exec("INSERT OR IGNORE INTO pkg_materials (name,unit,min_level) VALUES (?,?,?)",
                    (nm, data.get('unit','piece'), float(data.get('min_level',0))))
            broadcast('update', {'type':'pkg_materials'})
            return self.send_json({'ok': True})

        # Raw receipts
        if path == '/api/receipts':
            bn = next_batch('receipts')
            db_exec("""INSERT INTO receipts (batch_no,material_name,supplier,qty,unit,lot_no,expiry_date,received_date,notes,created_by)
                       VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (bn, data['material_name'], data.get('supplier',''),
                     float(data['qty']), data.get('unit','kg'),
                     data.get('lot_no',''), data.get('expiry_date',''),
                     data.get('received_date', ''), data.get('notes',''), user))
            broadcast('update', {'type':'receipts','batch_no':bn})
            return self.send_json({'ok': True, 'batch_no': bn})

        # Raw dispatches
        if path == '/api/dispatches':
            bn = next_batch('dispatches')
            db_exec("""INSERT INTO dispatches (batch_no,material_name,qty,unit,purpose,dispatch_date,notes,created_by)
                       VALUES (?,?,?,?,?,?,?,?)""",
                    (bn, data['material_name'], float(data['qty']),
                     data.get('unit','kg'), data.get('purpose',''),
                     data.get('dispatch_date',''), data.get('notes',''), user))
            broadcast('update', {'type':'dispatches','batch_no':bn})
            return self.send_json({'ok': True, 'batch_no': bn})

        # Pkg receipts
        if path == '/api/pkg_receipts':
            bn = next_batch('pkg_receipts')
            db_exec("""INSERT INTO pkg_receipts (batch_no,material_name,supplier,qty,unit,lot_no,expiry_date,received_date,notes,created_by)
                       VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (bn, data['material_name'], data.get('supplier',''),
                     float(data['qty']), data.get('unit','piece'),
                     data.get('lot_no',''), data.get('expiry_date',''),
                     data.get('received_date',''), data.get('notes',''), user))
            broadcast('update', {'type':'pkg_receipts','batch_no':bn})
            return self.send_json({'ok': True, 'batch_no': bn})

        # Pkg dispatches
        if path == '/api/pkg_dispatches':
            bn = next_batch('pkg_dispatches')
            db_exec("""INSERT INTO pkg_dispatches (batch_no,material_name,qty,unit,purpose,dispatch_date,notes,created_by)
                       VALUES (?,?,?,?,?,?,?,?)""",
                    (bn, data['material_name'], float(data['qty']),
                     data.get('unit','piece'), data.get('purpose',''),
                     data.get('dispatch_date',''), data.get('notes',''), user))
            broadcast('update', {'type':'pkg_dispatches','batch_no':bn})
            return self.send_json({'ok': True, 'batch_no': bn})

        # Production
        if path == '/api/production':
            bn = next_batch('production')
            db_exec("""INSERT INTO production (batch_no,product_name,customer,qty,unit,production_date,expiry_date,notes,created_by)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    (bn, data['product_name'], data.get('customer',''),
                     float(data['qty']), data.get('unit','kg'),
                     data.get('production_date',''), data.get('expiry_date',''),
                     data.get('notes',''), user))
            broadcast('update', {'type':'production','batch_no':bn})
            return self.send_json({'ok': True, 'batch_no': bn})

        self.send_err('not found', 404)

    # ── DELETE ────────────────────────────────────────────────────────────────
    def do_DELETE(self):
        path = urlparse(self.path).path.rstrip('/')
        user = check_session(self)
        if not user:
            return self.send_err('unauthorized', 401)

        parts = path.split('/')
        # /api/<table>/<id>
        if len(parts) == 4 and parts[1] == 'api':
            tbl = parts[2]
            rid = parts[3]
            allowed = ['receipts','dispatches','pkg_receipts','pkg_dispatches','production','materials','pkg_materials']
            if tbl not in allowed:
                return self.send_err('forbidden', 403)
            db_exec(f"DELETE FROM {tbl} WHERE id=?", (rid,))
            broadcast('update', {'type': tbl})
            return self.send_json({'ok': True})

        self.send_err('not found', 404)

# ── Main ─────────────────────────────────────────────────────────────────────
class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True

def main():
    init_db()
    server = ThreadedHTTPServer(('0.0.0.0', PORT), Handler)
    print(f"Batch Tracker Pro v2 running on port {PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass

if __name__ == '__main__':
    main()
