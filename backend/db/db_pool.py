import psycopg2
import threading
import logging
import time
from typing import Optional, Dict, Any

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class QuestDBConnectionPool:
    """QuestDB 连接池管理类"""
    
    def __init__(self, min_connections: int = 5, max_connections: int = 20,
                 connection_timeout: float = 30.0, max_connection_age: float = 3600.0):
        """
        初始化连接池
        
        Args:
            min_connections: 最小连接数
            max_connections: 最大连接数
            connection_timeout: 连接超时时间（秒）
            max_connection_age: 连接最大存活时间（秒）
        """
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.connection_timeout = connection_timeout
        self.max_connection_age = max_connection_age
        
        # 连接配置缓存
        self.connection_config: Optional[Dict[str, Any]] = None
        # 可用连接池
        self.available_connections: list = []
        # 总连接数计数器
        self.total_connections = 0
        # 锁，保证线程安全
        self.lock = threading.RLock()
        # 上次清理时间
        self.last_cleanup_time = time.time()
    
    def _create_connection(self, **kwargs) -> psycopg2.extensions.connection:
        """
        创建新的数据库连接
        
        Returns:
            数据库连接对象
        """
        try:
            conn = psycopg2.connect(**kwargs)
            conn.autocommit = True
            logger.debug(f"创建新的QuestDB连接，当前总连接数: {self.total_connections + 1}")
            return conn
        except Exception as e:
            logger.error(f"创建QuestDB连接失败: {str(e)}")
            raise
    
    def _is_valid_connection(self, conn_info: dict) -> bool:
        """
        检查连接是否有效
        
        Args:
            conn_info: 连接信息字典，包含conn（连接对象）和created_at（创建时间）
            
        Returns:
            连接是否有效
        """
        conn = conn_info['conn']
        created_at = conn_info['created_at']
        
        # 检查连接是否过期
        if time.time() - created_at > self.max_connection_age:
            logger.debug("连接已过期")
            return False
        
        # 检查连接是否有效
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True
        except Exception:
            logger.debug("连接无效")
            return False
    
    def _cleanup_connections(self):
        """
        清理无效或过期的连接
        """
        current_time = time.time()
        # 每60秒清理一次，避免频繁清理
        if current_time - self.last_cleanup_time < 60:
            return
        
        logger.debug("开始清理过期或无效的连接")
        self.last_cleanup_time = current_time
        
        valid_connections = []
        for conn_info in self.available_connections:
            if self._is_valid_connection(conn_info):
                valid_connections.append(conn_info)
            else:
                # 关闭无效连接
                try:
                    conn_info['conn'].close()
                    self.total_connections -= 1
                    logger.debug(f"关闭无效连接，当前总连接数: {self.total_connections}")
                except Exception:
                    pass
        
        self.available_connections = valid_connections
    
    def get_connection(self, **kwargs) -> psycopg2.extensions.connection:
        """
        获取数据库连接
        
        Returns:
            数据库连接对象
        """
        with self.lock:
            # 缓存连接配置
            if not self.connection_config:
                self.connection_config = kwargs
            
            # 清理过期连接
            self._cleanup_connections()
            
            # 从可用连接池获取连接
            while self.available_connections:
                conn_info = self.available_connections.pop()
                if self._is_valid_connection(conn_info):
                    logger.debug("从连接池获取有效连接")
                    return conn_info['conn']
                else:
                    # 关闭无效连接
                    try:
                        conn_info['conn'].close()
                        self.total_connections -= 1
                        logger.debug(f"关闭无效连接，当前总连接数: {self.total_connections}")
                    except Exception:
                        pass
            
            # 如果连接数未达到上限，创建新连接
            if self.total_connections < self.max_connections:
                conn = self._create_connection(**kwargs)
                self.total_connections += 1
                return conn
            
            # 如果连接数已达到上限，等待可用连接
            start_time = time.time()
            while time.time() - start_time < self.connection_timeout:
                # 短暂休眠，避免CPU占用过高
                time.sleep(0.1)
                
                # 再次尝试获取连接
                self._cleanup_connections()
                if self.available_connections:
                    conn_info = self.available_connections.pop()
                    if self._is_valid_connection(conn_info):
                        logger.debug("等待后从连接池获取有效连接")
                        return conn_info['conn']
                    else:
                        # 关闭无效连接
                        try:
                            conn_info['conn'].close()
                            self.total_connections -= 1
                            logger.debug(f"关闭无效连接，当前总连接数: {self.total_connections}")
                        except Exception:
                            pass
            
            # 超时抛出异常
            raise TimeoutError("获取数据库连接超时")
    
    def return_connection(self, conn: psycopg2.extensions.connection):
        """
        归还数据库连接到连接池
        
        Args:
            conn: 数据库连接对象
        """
        with self.lock:
            if conn:
                try:
                    # 检查连接是否仍然有效
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    cursor.close()
                    
                    # 将连接信息添加到可用连接池
                    conn_info = {
                        'conn': conn,
                        'created_at': time.time()
                    }
                    self.available_connections.append(conn_info)
                    logger.debug(f"连接已归还到连接池，当前可用连接数: {len(self.available_connections)}")
                except Exception:
                    # 如果连接无效，关闭它
                    try:
                        conn.close()
                        self.total_connections -= 1
                        logger.debug(f"连接无效，已关闭，当前总连接数: {self.total_connections}")
                    except Exception:
                        pass
    
    def close_all_connections(self):
        """
        关闭所有连接
        """
        with self.lock:
            logger.debug("关闭所有连接")
            for conn_info in self.available_connections:
                try:
                    conn_info['conn'].close()
                except Exception:
                    pass
            
            self.available_connections = []
            self.total_connections = 0
            logger.debug("所有连接已关闭")
    
    def get_stats(self) -> dict:
        """
        获取连接池状态信息
        
        Returns:
            连接池状态字典
        """
        with self.lock:
            return {
                'available_connections': len(self.available_connections),
                'total_connections': self.total_connections,
                'min_connections': self.min_connections,
                'max_connections': self.max_connections
            }


# 全局连接池实例
_global_pool: Optional[QuestDBConnectionPool] = None
_pool_lock = threading.RLock()


def get_pool() -> QuestDBConnectionPool:
    """
    获取全局连接池实例
    
    Returns:
        连接池实例
    """
    global _global_pool
    
    with _pool_lock:
        if _global_pool is None:
            _global_pool = QuestDBConnectionPool()
        return _global_pool


def get_conn(**kwargs) -> psycopg2.extensions.connection:
    """
    获取数据库连接
    
    Returns:
        数据库连接对象
    """
    # 如果kwargs为空，添加默认的数据库连接参数
    if not kwargs:
        kwargs = {
            'host': 'localhost',
            'port': 8812,
            'user': 'admin',
            'password': 'quest',
            'dbname': 'qdb'
        }
    pool = get_pool()
    # 判断连接是否已关闭，若关闭则重新获取一个有效连接
    conn = pool.get_connection(**kwargs)
    if conn.closed:
        logger.warning("检测到连接已关闭，重新获取有效连接")
        conn = pool.get_connection(**kwargs)
    return conn
    


def put_conn(conn: psycopg2.extensions.connection):
    """
    归还数据库连接
    
    Args:
        conn: 数据库连接对象
    """
    pool = get_pool()
    pool.return_connection(conn)


def close_pool():
    """
    关闭连接池
    """
    global _global_pool
    
    with _pool_lock:
        if _global_pool is not None:
            _global_pool.close_all_connections()
            _global_pool = None


def get_pool_stats() -> dict:
    """
    获取连接池状态
    
    Returns:
        连接池状态字典
    """
    pool = get_pool()
    return pool.get_stats()


# 模块清理时关闭连接池
def __del__():
    close_pool()