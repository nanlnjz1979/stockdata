import json
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta

from .base import BaseTask

# 复用现有的数据处理和写入函数
from .download_daily import _insert_daily, _num, _int
from backend.global_config.utils import is_all_holiday
# 依赖：Akshare 数据源
try:
    from global_config.data_fetch import AkshareFetcher
except Exception:
    AkshareFetcher = None

logger = logging.getLogger(__name__)

#得到一个股票的最后更新日期
def _get_last_update_date(code: str, conn=None) -> Optional[datetime]:
    """
    从stock_daily表获取指定股票代码的最后更新日期。
    如果没有找到数据，则返回None。
    """
    if not conn:
        return None
    try:
        cur = conn.cursor()
        # 查找该股票代码的最大日期
        cur.execute(
            """
            select max(trade_date) as last_date 
            from stock_daily 
            where code = %s
            """,
            (code,)
        )
        row = cur.fetchone()
        if row and row[0]:
            # 确保返回datetime对象
            last_date = row[0]
            if isinstance(last_date, str):
                try:
                    last_date = datetime.strptime(last_date, '%Y-%m-%d')
                except Exception:
                    return None
            return last_date
    except Exception as e:
        logger.warning("获取最后更新日期失败: code=%s, error=%s", code, e)
    return None


def _get_stock_basic_from_db(code: str, conn=None) -> Dict[str, Any]:
    """
    尝试从数据库获取股票基本信息（如市场）。
    如果未找到，则返回空字典。
    """
    if not conn:
        return {}
    try:
        cur = conn.cursor()
        # 尝试从stock_basic或类似表中获取市场信息
        # 注意：这里假设有stock_basic表且结构类似Django ORM模型
        cur.execute(
            """
            select market 
            from stock_basic 
            where stock_code = %s
            """,
            (code,)
        )
        row = cur.fetchone()
        if row and row[0]:
            return {'market': row[0]}
    except Exception:
        # 忽略错误，返回默认值
        pass
    return {}


def _get_all_stocks_last_trade_date(conn=None) -> Dict[str, datetime]:
    """
    获取所有股票的最后交易日日期。
    使用QuestDB的LATEST BY语法高效获取每个股票代码的最新交易日期。
    
    Args:
        conn: 数据库连接对象
        
    Returns:
        Dict[str, datetime]: 股票代码到最后交易日日期的映射字典
    """
    result = {}
    if not conn:
        return result
    
    try:
        cur = conn.cursor()
        # 使用QuestDB的LATEST BY语法获取每个股票代码的最新交易日期
        cur.execute(
            """
            SELECT code, trade_date 
            FROM stock_daily 
            LATEST BY code
            """
        )
        
        # 处理查询结果
        for row in cur.fetchall():
            if row and len(row) >= 2 and row[0]:
                code = row[0]
                trade_date = row[1]
                
                # 确保trade_date是datetime对象
                if isinstance(trade_date, str):
                    try:
                        trade_date = datetime.strptime(trade_date, '%Y-%m-%d')
                    except Exception:
                        # 解析失败则跳过该记录
                        continue
                
                result[code] = trade_date
        
        logger.info(f"成功获取{len(result)}只股票的最后交易日日期")
    except Exception as e:
        logger.error(f"获取所有股票最后交易日失败: {e}")
    
    return result


class IncrementalUpdateTask(BaseTask):
    """
    增量更新任务：根据stock_daily表中已有的数据，只更新最后日期到今天的数据。
    
    params 支持：
      - code: 单个股票代码（字符串，必填）
      - codes: 多个股票代码（列表，可选，与code二选一）
      - market: 市场标识（可选，'SH'/'SZ'/'BJ'，默认会尝试从数据库获取）
      - adjust: 复权类型（可选，''/空表示不复权，'qfq'前复权，'hfq'后复权，'all'全部）
    
    执行逻辑：
    1. 检查指定股票代码在stock_daily表中的最后更新日期
    2. 如果有最后更新日期，则从该日期的次日开始拉取数据
    3. 如果没有历史数据，则从较早日期开始拉取（可配置默认起始日期）
    4. 使用ak.stock_zh_a_hist获取数据并写入QuestDB
    """

    def __init__(self, orm):
        # 仅要求传入orm，其余字段在generate()时设置
        super().__init__(orm, task_type="", task_desc="", params=None, priority=0)

    def generate(self, task_type: str, task_desc: str = "", params: Optional[Dict[str, Any]] = None, priority: int = 1,conn=None) -> str:
        # 在生成前配置必要字段
        self.task_type = task_type
        self.task_desc = task_desc
        self.params_str = self._ensure_json_str(params)
        self.priority = priority

        # 获取所有股票的最后交易日日期
        
        last_trade_dates = _get_all_stocks_last_trade_date(conn=conn)
        
        # 循环生成每个股票的增量更新任务
        task_ids = []
        for code, last_date in last_trade_dates.items():
            
            # 计算起始日期（最后交易日的次日）
            if last_date:
                start_date = (last_date + timedelta(days=1)).strftime('%Y%m%d')
            else:
                # 如果没有历史数据，使用默认起始日期
                start_date = '20200101'

            # 今天的日期作为结束日期
            end_date = datetime.now().strftime('%Y%m%d')

            # 如果起始日期已经是今天或未来，跳过
            if start_date > end_date:
                logger.info("股票 %s 已是最新数据，跳过生成任务", code)
                continue
            
            # 判断起止日期是否均为非交易日，若是则跳过
            if is_all_holiday(start_date, end_date):
                logger.info("股票 %s 的起止日期(%s~%s)均为非交易日，跳过生成任务", code, start_date, end_date)
                continue

            # 构造任务参数
            task_params = {
                'code': code,
                'start_date': start_date,
                'end_date': end_date,
                'adjust': 'all'   # 默认不复权
            }

           
        
            self.task_type = task_type
            self.task_desc = f"增量更新股票 {code} 从 {start_date} 到 {end_date}",
            self.priority = priority
            self.params_str = self._ensure_json_str(task_params)
            self.priority=priority
            # 生成任务ID并记录
            
            task_id = super().generate()
            task_ids.append(task_id)
            

        logger.info("成功生成 %d 个增量更新任务", len(task_ids))
        return ""


    def _parse_params(self) -> Dict[str, Any]:
        try:
            return json.loads(self.params_str or '{}')
        except Exception:
            return {}

    @classmethod
    def taskID(cls) -> str:
        return f"STOCK_Update"
    def run(self, conn=None) -> bool:
        #一次只处理一个任务
        
        params = self._parse_params()
        
        code = params.get('code')
        start_date = params.get('start_date')
        end_date = params.get('end_date')
        adjust = params.get('adjust', 'all')

        if not code or not start_date or not end_date:
            logger.error("增量更新任务缺少必要参数: %s", params)
            return False

      
            # 使用 akshare 获取日线数据
        adjust_all = ['', 'qfq', 'hfq'] if adjust == 'all' else [adjust]

        # 初始化AkshareFetcher
        fetcher = AkshareFetcher()
        
        for adj in adjust_all:
            try:
                # 使用AkshareFetcher获取日线数据
                df = fetcher.fetch_stock_daily(
                    code=code,
                    start_date=start_date,
                    end_date=end_date,
                    adjust=adj
                )
                if df is None or df.empty:
                    logger.info("股票 %s 在 %s~%s 无数据，跳过", code, start_date, end_date)
                    continue

                # 写入 QuestDB
                _insert_daily(code, df, adj, conn=conn)
                logger.info("增量更新成功: %s [%s ~ %s] adjust=%s", code, start_date, end_date, adj)

            except Exception as e:
                logger.exception("增量更新失败: %s [%s ~ %s] adjust=%s, error: %s", code, start_date, end_date, adj, e)
                return False

        return True
          