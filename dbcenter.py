from dotenv import load_dotenv
import os

# Suporta tanto Supabase (recomendado) quanto um pool psycopg2 tradicional.
# Se SUPABASE_URL e SUPABASE_KEY estiverem presentes no .env, usaremos o Supabase.
# Caso contrário, usamos psycopg2 (conexão direta ao Postgres).

load_dotenv()

# Supabase configuration
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

# Fallback Postgres configuration (antigo)
DB_USER = os.getenv('DB_USER') or os.getenv('user')
DB_PASSWORD = os.getenv('DB_PASSWORD') or os.getenv('password')
DB_HOST = os.getenv('DB_HOST') or os.getenv('host') or 'localhost'
DB_PORT = os.getenv('DB_PORT') or os.getenv('port') or '5432'
DB_NAME = os.getenv('DB_NAME') or os.getenv('dbname')

# Lazy imports
_supabase = None
_pool = None


def init_supabase():
    global _supabase
    if _supabase is None:
        try:
            from supabase import create_client
        except Exception:
            raise RuntimeError('Pacote "supabase" não está instalado. Execute: pip install supabase')
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise RuntimeError('SUPABASE_URL e SUPABASE_KEY devem estar configurados no .env')
        _supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _supabase


def init_pool(minconn=1, maxconn=5):
    global _pool
    if _pool is None:
        try:
            import psycopg2
            from psycopg2 import pool
        except Exception:
            raise RuntimeError('Pacote "psycopg2-binary" não está instalado. Execute: pip install psycopg2-binary')
        _pool = pool.SimpleConnectionPool(
            minconn,
            maxconn,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
        )


def get_conn():
    if _pool is None:
        init_pool()
    return _pool.getconn()


def put_conn(conn):
    if _pool is not None:
        _pool.putconn(conn)


def close_pool():
    global _pool
    if _pool is not None:
        _pool.closeall()
        _pool = None


# --- helpers ---

def create_clients_table():
    """Verifica se a tabela clients existe. Se estiver usando Supabase, apenas testa uma SELECT.
    Preferível criar a tabela pelo painel SQL do Supabase com o SQL fornecido abaixo."""
    if SUPABASE_URL and SUPABASE_KEY:
        sb = init_supabase()
        # tenta um select simples para verificar existência
        try:
            res = sb.table('clients').select('id').limit(1).execute()
            # se sucesso, ok
            return True
        except Exception:
            # tabela pode não existir — retorne False para que o operador decida criar
            return False
    else:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                CREATE TABLE IF NOT EXISTS clients (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    address TEXT,
                    phone TEXT,
                    created_at TIMESTAMPTZ DEFAULT now()
                );
                """)
                conn.commit()
                return True
        finally:
            put_conn(conn)


def insert_client(name, address, phone):
    """Insere cliente. Se SUPABASE configurado, utiliza a tabela "clients" do Supabase.
    Retorna um dict com id e created_at quando disponível."""
    if SUPABASE_URL and SUPABASE_KEY:
        sb = init_supabase()
        payload = {"name": name, "address": address, "phone": phone}
        res = sb.table('clients').insert(payload).execute()
        # res pode ser um objeto com .data ou um dict
        data = None
        try:
            data = getattr(res, 'data', None) or res.get('data')
        except Exception:
            data = None
        if not data:
            # tenta inspecionar erro detalhado
            err = getattr(res, 'error', None) or res.get('error')
            raise RuntimeError('Falha ao inserir no Supabase: ' + str(err))
        row = data[0]
        return {"id": row.get('id'), "created_at": row.get('created_at')}

    else:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO clients (name, address, phone) VALUES (%s, %s, %s) RETURNING id, created_at",
                    (name, address, phone),
                )
                row = cur.fetchone()
                conn.commit()
                return {"id": row[0], "created_at": row[1]}
        finally:
            put_conn(conn)


def get_clients(limit=100):
    """Retorna lista de clientes (mais recentes primeiro)."""
    if SUPABASE_URL and SUPABASE_KEY:
        sb = init_supabase()
        res = sb.table('clients').select('*').order('created_at', desc=True).limit(limit).execute()
        data = None
        try:
            data = getattr(res, 'data', None) or res.get('data')
        except Exception:
            data = None
        if data is None:
            err = getattr(res, 'error', None) or res.get('error')
            raise RuntimeError('Falha ao listar clientes no Supabase: ' + str(err))
        return data
    else:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, name, address, phone, created_at FROM clients ORDER BY created_at DESC LIMIT %s",
                    (limit,),
                )
                rows = cur.fetchall()
                result = []
                for r in rows:
                    result.append({
                        'id': r[0],
                        'name': r[1],
                        'address': r[2],
                        'phone': r[3],
                        'created_at': r[4],
                    })
                return result
        finally:
            put_conn(conn)
