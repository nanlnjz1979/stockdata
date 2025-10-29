import os
from typing import Optional

_pool = None

def _init_pool():
    global _pool
    if _pool is not None:
        return _pool
    try:
        import psycopg2
        from psycopg2.pool import ThreadedConnectionPool
    except Exception as e:
        raise RuntimeError(f"psycopg2 未安装或不可用: {e}")

    host = os.getenv('QDB_HOST', 'localhost')
    port = int(os.getenv('QDB_PORT', '8812'))
    user = os.getenv('QDB_USER', 'admin')
    password = os.getenv('QDB_PASS', 'quest')
    dbname = os.getenv('QDB_DB', 'qdb')

    minconn = int(os.getenv('QDB_POOL_MIN', '1'))
    maxconn = int(os.getenv('QDB_POOL_MAX', '10'))

    try:
        _pool = ThreadedConnectionPool(
            minconn=minconn,
            maxconn=maxconn,
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=dbname,
        )
    except Exception as e:
        _pool = None
        raise RuntimeError(f"初始化 QuestDB 连接池失败: {e}")
    return _pool


def get_conn():
    """从连接池获取一个连接。"""
    pool = _init_pool()
    try:
        return pool.getconn()
    except Exception as e:
        raise RuntimeError(f"从连接池获取连接失败: {e}")


def put_conn(conn: Optional[object]):
    """归还连接到连接池。"""
    if conn is None:
        return
    try:
        pool = _init_pool()
        pool.putconn(conn)
    except Exception:
        try:
            conn.close()
        except Exception:
            pass


def close_pool():
    """关闭连接池（在应用关闭时调用）。"""
    global _pool
    if _pool is not None:
        try:
            _pool.closeall()
        finally:
            _pool = None