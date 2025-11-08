import json
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta

from .base import BaseTask

# 复用现有的数据处理和写入函数
from .download_daily import _insert_daily, _num, _int
from backend.global_config.utils import make_symbol

# 依赖：Akshare 数据源
try:
    import akshare as ak
except Exception:
    ak = None

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
    4. 使用ak.stock_zh_a_daily获取数据并写入QuestDB
    """

    def __init__(self, orm):
        # 仅要求传入orm，其余字段在generate()时设置
        super().__init__(orm, task_type="", task_desc="", params=None, priority=0)

    def generate(self, task_type: str, task_desc: str = "", params: Optional[Dict[str, Any]] = None, priority: int = 1) -> str:
        # 在生成前配置必要字段
        self.task_type = task_type
        self.task_desc = task_desc
        self.params_str = self._ensure_json_str(params)
        self.priority = priority
        return super().generate()

    def _parse_params(self) -> Dict[str, Any]:
        try:
            return json.loads(self.params_str or '{}')
        except Exception:
            return {}

    @classmethod
    def taskID(cls) -> str:
        return f"STOCK_Update"
    def run(self, conn=None) -> bool:
        # 检查依赖
        if not ak:
            logger.error("依赖不可用：akshare 未导入")
            return False
        
        params = self._parse_params()
        
        # 收集目标代码列表
        codes: List[str] = []
        code_single = params.get('code')
        codes_multi = params.get('codes')
        
        if isinstance(code_single, str) and code_single.strip():
            codes.append(code_single.strip())
        if isinstance(codes_multi, list):
            for c in codes_multi:
                if isinstance(c, str) and c.strip():
                    codes.append(c.strip())
        
        # 去重
        codes = list(dict.fromkeys(codes))
        if not codes:
            logger.warning("未提供有效的股票代码（params 需包含 'code' 或 'codes'）")
            return False
        
        # 获取配置的市场和复权类型
        market = (params.get('market') or '').upper()
        adjust = (params.get('adjust') or '').lower()
        
        # 默认起始日期（如果没有历史数据）
        default_start_date = params.get('default_start_date', '20200101')
        
        conn_local = conn
        if not conn_local:
            logger.error("数据库连接失败")
            return False
        
        try:
            total_saved = 0
            
            for code in codes:
                # 1. 获取最后更新日期
                last_date = _get_last_update_date(code, conn=conn_local)
                
                # 2. 计算起始日期（最后日期的次日）
                if last_date:
                    start_date = (last_date + timedelta(days=1)).strftime('%Y%m%d')
                    logger.info("股票 %s 的最后更新日期为 %s，将从 %s 开始更新", code, last_date.strftime('%Y-%m-%d'), start_date)
                else:
                    start_date = default_start_date
                    logger.info("股票 %s 无历史数据，将从默认日期 %s 开始更新", code, start_date)
                
                # 3. 今天的日期作为结束日期
                end_date = datetime.now().strftime('%Y%m%d')
                
                # 如果起始日期已经是今天或未来，跳过
                if start_date > end_date:
                    logger.info("股票 %s 已是最新数据，无需更新", code)
                    continue
                
                # 4. 获取市场信息（如果未提供）
                stock_basic = _get_stock_basic_from_db(code, conn=conn_local)
                stock_market = stock_basic.get('market', market)
                
                # 5. 构建akshare需要的symbol (使用全局导入的make_symbol函数)
                symbol = make_symbol(code, stock_market)
                
                # 6. 获取并写入数据
                adjust_all = ['', 'qfq', 'hfq'] if adjust == "all" else [adjust]
                for adj in adjust_all:
                    try:
                        logger.info("开始获取数据: code=%s, symbol=%s, start=%s, end=%s, adjust=%s", 
                                    code, symbol, start_date, end_date, adj)
                        df = ak.stock_zh_a_daily(
                            symbol=symbol, 
                            start_date=start_date, 
                            end_date=end_date, 
                            adjust=adj
                        )
                        
                        if df is not None and not df.empty:
                            logger.info("获取成功，共 %d 条记录: code=%s, adjust=%s", 
                                        len(df), code, adj)
                            
                            # 写入数据库
                            saved = _insert_daily(code, df, adj, conn=conn_local)
                            total_saved += int(saved or 0)
                            logger.info("写入完成，保存 %d 条记录: code=%s, adjust=%s", 
                                        saved, code, adj)
                        else:
                            logger.info("无新数据: code=%s, adjust=%s", code, adj)
                    except Exception as e:
                        logger.exception("获取或写入失败: code=%s, adjust=%s, error=%s", 
                                        code, adj, e)
            
            logger.info("增量更新任务完成：处理 %d 只股票，共保存 %d 条记录", 
                        len(codes), total_saved)
            return total_saved >= 0  # 即使没有新数据也视为成功
        finally:
            if conn is None:
                try:
                    conn_local.close()
                except Exception:
                    pass