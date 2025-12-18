import sys
import os
import logging

# 添加项目路径到sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'backend'))

# 导入数据库连接池工具和全局工具函数
try:
    from db.import_utils import get_db_pool_functions
except ImportError:
    # 如果导入失败，定义fallback函数
    def get_db_pool_functions():
        return None, None

# 新增：QuestDB PG 连接
try:
    import psycopg2
except Exception:
    psycopg2 = None

def qdb_connect():
    """连接 QuestDB（PG wire），从环境变量读取连接信息。"""
    if not psycopg2:
        return None
    import os
    host = os.getenv('QDB_HOST', 'localhost')
    port = int(os.getenv('QDB_PORT', '8812'))
    user = os.getenv('QDB_USER', 'admin')
    password = os.getenv('QDB_PASS', 'quest')
    dbname = os.getenv('QDB_DB', 'qdb')
    try:
        # 尝试使用连接池获取连接
        get_conn, put_conn = get_db_pool_functions()
        if get_conn:
            try:
                conn = get_conn(
                    host=host,
                    port=port,
                    user=user,
                    password=password,
                    dbname=dbname
                )
                return conn
            except Exception as e:
                logging.warning(f"使用连接池失败，回退到直接连接: {e}")
        # 如果连接池不可用，直接创建连接
        conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)
        conn.autocommit = True
        return conn
    except Exception:
        return None


# 修改：允许复用连接
def qdb_ensure_tables(conn=None):
    conn = conn or qdb_connect()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute("""
          create table if not exists stock_daily (
            code symbol index,     --股票代码
            trade_date timestamp,  --交易日期时间戳
            adjust_type symbol,    --复权类型
            open double,           --开盘价
            close double,          --收盘价
            high double,           --最高价
            low double,            --最低价
            volume long,           --成交量
            amount double,         --成交额
            turnover double,       --换手率
            outstanding_share double --流通股本
          ) TIMESTAMP(trade_date) PARTITION BY DAY  DEDUP UPSERT KEYS(code, trade_date, adjust_type); --按交易日按天分区
        """)
        #目前就用questdb做存储，然后做个检查程序，如果某一天的任务状态status都变成已完成，就把这个分区删掉，分区通过创建时间分区
        cur.execute("""
          create table if not exists tasks (
            task_id string,           --任务唯一标识符(UUID格式)
            task_type symbol,         --任务类型，如下载、更新等    例如LHB_InstituteTrack
            task_desc string,         --任务描述信息
            task_params string,       --任务参数，JSON格式
            priority int,             --任务优先级，数字越大优先级越高
            status symbol,            --任务状态，如待处理、处理中、已完成等
            created_at timestamp,     --任务创建时间
            started_at timestamp,     --任务开始执行时间
            ended_at timestamp        --任务结束时间
          ) TIMESTAMP(created_at) PARTITION BY DAY; --按任务创建时间按天分区
        """)

        cur.execute(    
            """
            create table if not exists inst_trading_tracker (   --机构席位追踪表
              ingest_date timestamp,     --查询的日期时间,大部分是入库的时间戳
              code symbol,          --股票的代码
              name string,          --股票名称
              buy_amount double,    -- 累计买入额(单位: 万)
              buy_times int,        -- 累计买入次数
              sell_amount double,   -- 累计卖出额(单位: 万)
              sell_times int,       -- 累计卖出次数
              net_amount double,    -- 净额(单位: 万)（net_amount = buy_amount - sell_amount）
              query_type int        -- 查询类型（5/10/30/60天）
            ) TIMESTAMP(ingest_date) PARTITION BY DAY ; --按入库时间按天分区
            """
        )

        # 新增：参数配置表（计划任务）
        cur.execute("""
          create table if not exists schedule_configs (
            id   symbol ,           --任务类型，例如:LHB_InstituteTrack
            name string,            --任务名称
            task_desc string,       --任务描述
            params string,          --Download_Full_Daily(下载日线数据)/ Institutional_Trading_Tracking(机构席位追踪)
            schedule_time timestamp,--例如每天16点
            enabled int             --是否开启
          );
        """)

        if conn is not qdb_connect:
            pass
        return True
    except Exception:
            # 使用连接池归还连接而不是直接关闭
            try:
                put_conn, _ = get_db_pool_functions()
                if put_conn:
                    put_conn(conn)
                else:
                    conn.close()
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass
            return False
