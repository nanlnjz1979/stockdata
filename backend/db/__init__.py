"""数据库连接池模块"""

# 从db_pool导入核心函数以便直接从db包访问
from .db_pool import (
    get_pool,
    get_conn,
    put_conn,
    close_pool,
    get_pool_stats,
    QuestDBConnectionPool
)

# 从import_utils导入辅助函数
from .import_utils import (
    add_project_paths,
    import_with_fallback,
    get_db_pool_functions,
    check_pool_availability,
    safe_get_conn,
    safe_put_conn,
    test_pool_import
)

# Redis连接池已移除

# 模块版本信息
__version__ = "1.0.0"
__all__ = [
    # db_pool functions
    'get_pool',
    'get_conn',
    'put_conn',
    'close_pool',
    'get_pool_stats',
    'QuestDBConnectionPool',
    # import_utils functions
    'add_project_paths',
    'import_with_fallback',
    'get_db_pool_functions',
    'check_pool_availability',
    'safe_get_conn',
    'safe_put_conn',
    'test_pool_import',
    # Redis连接池已移除
]

# 初始化时添加项目路径
try:
    add_project_paths()
except Exception:
    # 静默失败，允许其他模块继续工作
    pass