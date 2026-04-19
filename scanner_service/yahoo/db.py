# db.py
import psycopg2
import psycopg2.extras
from psycopg2 import pool

DSN = "postgresql://user:password@localhost:5432/yourdb"

_pool = pool.SimpleConnectionPool(1, 10, DSN)

def get_conn():
    return _pool.getconn()

def release_conn(conn):
    _pool.putconn(conn)

def init_db():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS universe (
                    symbol          TEXT PRIMARY KEY,
                    float_shares    BIGINT,
                    next_earnings   DATE,
                    last_earnings   DATE,
                    updated_at      TIMESTAMPTZ DEFAULT NOW()
                )
            """)
        conn.commit()
    finally:
        release_conn(conn)

def upsert_symbol(symbol, float_shares=None, next_earnings=None, last_earnings=None):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO universe (symbol, float_shares, next_earnings, last_earnings, updated_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (symbol) DO UPDATE SET
                    float_shares  = COALESCE(EXCLUDED.float_shares, universe.float_shares),
                    next_earnings = COALESCE(EXCLUDED.next_earnings, universe.next_earnings),
                    last_earnings = COALESCE(EXCLUDED.last_earnings, universe.last_earnings),
                    updated_at    = NOW()
            """, (symbol, float_shares, next_earnings, last_earnings))
        conn.commit()
    finally:
        release_conn(conn)

def get_symbol(symbol):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM universe WHERE symbol = %s", (symbol,))
            return cur.fetchone()
    finally:
        release_conn(conn)

def get_all_symbols():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT symbol FROM universe")
            return [r[0] for r in cur.fetchall()]
    finally:
        release_conn(conn)