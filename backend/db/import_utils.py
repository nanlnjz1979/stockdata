import sys
import os
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def add_project_paths():
    """
    添加所有必要的项目路径到sys.path，确保模块可以正确导入
    """
    # 获取当前文件所在目录
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # 获取backend目录
    backend_dir = os.path.dirname(current_dir)
    
    # 获取项目根目录
    project_root = os.path.dirname(backend_dir)
    
    # 添加backend目录（确保db模块可被正确导入）
    if backend_dir not in sys.path:
        sys.path.insert(0, backend_dir)
        logger.debug(f"已添加backend目录到sys.path: {backend_dir}")
    
    # 添加项目根目录
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
        logger.debug(f"已添加项目根目录到sys.path: {project_root}")


def import_with_fallback(import_func, error_message=None):
    """
    带回退机制的导入函数
    
    Args:
        import_func: 尝试导入的函数
        error_message: 错误信息
        
    Returns:
        导入的模块或None
    """
    try:
        return import_func()
    except Exception as e:
        if error_message:
            logger.error(f"{error_message}: {str(e)}")
        else:
            logger.warning(f"导入失败: {str(e)}")
        return None


def get_db_pool_functions():
    """
    获取数据库连接池的核心函数
    
    Returns:
        tuple: (get_conn, put_conn) 函数对，如果导入失败则返回 (None, None)
    """
    try:
        # 尝试导入连接池函数
        from db.db_pool import get_conn, put_conn
        logger.debug("成功导入数据库连接池函数")
        return get_conn, put_conn
    except ImportError as e:
        logger.warning(f"数据库连接池模块导入失败: {str(e)}")
        # 尝试直接导入db_pool模块
        try:
            import db.db_pool as db_pool_module
            return db_pool_module.get_conn, db_pool_module.put_conn
        except Exception:
            logger.error("无法导入数据库连接池模块")
            return None, None


def check_pool_availability():
    """
    检查连接池是否可用
    
    Returns:
        bool: 连接池是否可用
    """
    put_conn, get_conn = get_db_pool_functions()
    return put_conn is not None and get_conn is not None


def safe_get_conn(**kwargs):
    """
    安全获取数据库连接，带错误处理
    
    Args:
        **kwargs: 连接参数
        
    Returns:
        数据库连接对象或None
    """
    put_conn, get_conn = get_db_pool_functions()
    if get_conn:
        try:
            return get_conn(**kwargs)
        except Exception as e:
            logger.error(f"从连接池获取连接失败: {str(e)}")
    return None


def safe_put_conn(conn):
    """
    安全归还数据库连接
    
    Args:
        conn: 数据库连接对象
    """
    put_conn, get_conn = get_db_pool_functions()
    if put_conn:
        try:
            put_conn(conn)
            return True
        except Exception as e:
            logger.error(f"归还连接到连接池失败: {str(e)}")
    
    # 如果连接池不可用，尝试直接关闭连接
    try:
        if conn:
            conn.close()
            logger.debug("直接关闭数据库连接")
        return True
    except Exception as e:
        logger.error(f"关闭数据库连接失败: {str(e)}")
        return False


# 初始化时添加项目路径
try:
    add_project_paths()
except Exception as e:
    logger.error(f"添加项目路径失败: {str(e)}")


# 提供一个简单的测试函数，验证连接池是否配置正确
def test_pool_import():
    """
    测试连接池导入是否成功
    
    Returns:
        dict: 测试结果
    """
    result = {
        'paths_added': False,
        'pool_available': False,
        'error': None
    }
    
    try:
        # 检查路径是否已添加
        current_dir = os.path.dirname(os.path.abspath(__file__))
        backend_dir = os.path.dirname(current_dir)
        project_root = os.path.dirname(backend_dir)
        
        result['paths_added'] = backend_dir in sys.path and project_root in sys.path
        
        # 检查连接池是否可用
        result['pool_available'] = check_pool_availability()
        
    except Exception as e:
        result['error'] = str(e)
    
    return result