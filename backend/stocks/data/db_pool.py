"""
此文件已被修改为使用ClickHouse连接池，而非旧的QuestDB连接池。
所有调用将转发到全局的ClickHouse连接池。
"""

# 首先添加必要的路径到sys.path，确保可以正确导入
try:
    import sys
    import os
    
    # 添加backend目录到sys.path
    current_dir = os.path.dirname(os.path.abspath(__file__))
    backend_dir = os.path.dirname(os.path.dirname(current_dir))
    if backend_dir not in sys.path:
        sys.path.insert(0, backend_dir)
    
except Exception as e:
    # 导入失败时记录日志，但不中断执行
    import logging
    logging.error(f"添加sys.path失败: {e}")

# 直接从全局ClickHouse连接池导入函数并立即重命名，避免递归调用
try:
    from backend.db.db_pool import get_conn as ch_get_conn
    from backend.db.db_pool import put_conn as ch_put_conn
    from backend.db.db_pool import close_pool as ch_close_pool
except ImportError:
    # 如果无法直接导入，尝试相对路径
    try:
        from db.db_pool import get_conn as ch_get_conn
        from db.db_pool import put_conn as ch_put_conn
        from db.db_pool import close_pool as ch_close_pool
    except Exception as e:
        import logging
        logging.error(f"无法导入ClickHouse连接池: {e}")
        raise

# 保持与旧API兼容的变量
_pool = None


def _init_pool():
    """兼容旧API，实际使用全局ClickHouse连接池"""
    global _pool
    if _pool is None:
        # 这里不实际初始化，只是标记已初始化
        _pool = True
    return _pool


def get_conn():
    """从连接池获取一个连接（转发到ClickHouse连接池）。"""
    return ch_get_conn()


def put_conn(conn):
    """归还连接到连接池（转发到ClickHouse连接池）。"""
    return ch_put_conn(conn)


def close_pool():
    """关闭连接池（转发到ClickHouse连接池）。"""
    return ch_close_pool()