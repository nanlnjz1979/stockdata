import sys
import os
import logging
import json

# 添加项目路径到sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'backend'))

# 导入数据库连接池工具和全局工具函数
try:
    from db.import_utils import get_db_pool_functions
except ImportError:
    # 如果导入失败，定义fallback函数
    def get_db_pool_functions():
        return None, None

# 从配置文件读取数据库连接参数
def get_db_config():
    """从配置文件读取数据库连接参数。"""
    config_path = os.path.join(os.path.dirname(__file__), '..', 'backend', 'global_config', 'config.json')
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        return config.get('database', {})
    except Exception as e:
        logging.error(f"读取配置文件失败: {e}")
        # 返回默认配置
        return {
            'host': 'localhost',
            'port': 9000,
            'user': 'default',
            'password': '',
            'database': 'default'
        }

def get_db_connection():
    """获取数据库连接，支持ClickHouse和QuestDB。"""
    try:
        # 从配置文件读取数据库连接参数
        db_config = get_db_config()
        host = db_config.get('host', 'localhost')
        port = db_config.get('port', 9000)
        user = db_config.get('user', 'default')
        password = db_config.get('password', '')
        database = db_config.get('database', 'default')
        
        # 尝试使用连接池获取连接
        get_conn, put_conn = get_db_pool_functions()
        if get_conn:
            try:
                conn = get_conn(
                    host=host,
                    port=port,
                    user=user,
                    password=password,
                    database=database
                )
                return conn
            except Exception as e:
                logging.warning(f"使用连接池失败: {e}")
                return None
        return None
    except Exception as e:
        logging.error(f"获取数据库连接失败: {e}")
        return None


# 修改：允许复用连接
def ensure_tables(conn=None):
    conn = conn or get_db_connection()
    if not conn:
        return False
    try:
        # ClickHouse客户端对象直接支持execute方法，不需要cursor
        if hasattr(conn, 'execute'):  # ClickHouse客户端对象
            # 创建stock_daily表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS stock_daily (
                    code String,
                    date DateTime,
                    open Float64,
                    close Float64,
                    high Float64,
                    low Float64,
                    volume Int64,
                    amount Float64,
                    turnover Float64,
                    outstanding_share Float64
                ) ENGINE = MergeTree()
                PARTITION BY toDate(date)
                ORDER BY (code, date);
            """)
            
            # 创建tasks表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id String,
                    task_type String,
                    task_desc String,
                    task_params String,
                    priority Int32,
                    status String,
                    created_at DateTime,
                    started_at DateTime,
                    ended_at DateTime
                ) ENGINE = MergeTree()
                PARTITION BY toDate(created_at)
                ORDER BY (task_id);
            """)

            # 创建inst_trading_tracker表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS inst_trading_tracker (
                    ingest_date DateTime,
                    code String,
                    name String,
                    buy_amount Float64,
                    buy_times Int32,
                    sell_amount Float64,
                    sell_times Int32,
                    net_amount Float64,
                    query_type Int32
                ) ENGINE = MergeTree()
                PARTITION BY toDate(ingest_date)
                ORDER BY (code, ingest_date, query_type);
            """)

            # 创建schedule_configs表
            conn.execute("""
                CREATE TABLE IF NOT EXISTS schedule_configs (
                    id String,
                    name String,
                    task_desc String,
                    params String,
                    schedule_time DateTime,
                    enabled Int32
                ) ENGINE = MergeTree()
                ORDER BY (id);
            """)
        elif hasattr(conn, 'cursor'):  # 传统数据库连接
            cursor = conn.cursor()
            # 这里可以添加传统数据库的表创建逻辑
            cursor.close()
        else:
            logging.error("无法识别的数据库连接类型")
            return False
        
        logging.info("成功创建表（如果不存在）")
        return True
    except Exception as e:
        logging.error(f"创建表失败: {e}")
        # 使用连接池归还连接而不是直接关闭
        try:
            put_conn, _ = get_db_pool_functions()
            if put_conn:
                put_conn(conn)
            else:
                if hasattr(conn, 'disconnect'):  # ClickHouse客户端对象
                    conn.disconnect()
                else:  # 传统数据库连接
                    conn.close()
        except Exception as close_e:
            logging.error(f"关闭连接失败: {close_e}")
        return False
