import json
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime
from backend.global_config import norm_date
from backend.global_config.utils import _num, _int, make_symbol
from .base import BaseTask

def _insert_daily(code, df, adj, conn=None):
    if df is None or getattr(df, 'empty', True):
        return 0
    conn_local = conn 
    if not conn_local:
        return 0
    try:
        cur = conn_local.cursor()
        cols = {
            '日期': 'date',
            '开盘': 'open',
            '收盘': 'close',
            '最高': 'high',
            '最低': 'low',
            '成交量': 'volume',
            '成交额': 'amount',
            '换手率': 'turnover',
            '流通股本': 'outstanding_share',
        }
        df = df.rename(columns=cols)
        values = []
        for _, r in df.iterrows():
            d = r.get('date')
            if not d:
                continue
            try:
                trade_date = d if isinstance(d, datetime) else datetime.strptime(str(d), '%Y-%m-%d')
            except Exception:
                continue
            adj_norm = adj if (adj and str(adj).strip()) else None
            values.append((
                code,
                trade_date,
                adj_norm,
                _num(r.get('open')),
                _num(r.get('close')),
                _num(r.get('high')),
                _num(r.get('low')),
                _int(r.get('volume')),
                _num(r.get('amount')),
                _num(r.get('turnover')),
                _num(r.get('outstanding_share')),
            ))
        if values:
            try:
                cur.executemany(
                    """
                    insert into stock_daily (
                      code, trade_date, adjust_type, open, close, high, low, volume, amount, turnover, outstanding_share
                    ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    values
                )
            except Exception:
                return 0
        if conn is None:
            try:
                conn_local.close()
            except Exception:
                pass
        return len(values)
    except Exception:
        try:
            if conn is None:
                conn_local.close()
        except Exception:
            pass
        return 0

# 依赖：Akshare 数据源
try:
    import akshare as ak
except Exception:
    ak = None

logger = logging.getLogger(__name__)


class DownloadDailyTask(BaseTask):
    """
    下载所有股票日期的数据，一般是第一次运行时使用。
    从 BaseTask 派生的任务：下载并写入股票日线数据到 QuestDB。
    params 支持：
      - code: 单个股票代码（字符串）
      - codes: 多个股票代码（列表）
      - market: 市场标识（可选，'SH'/'SZ'/'BJ'，默认空）
      - start_date: 开始日期（'YYYYMMDD'或'YYYY-MM-DD'）
      - end_date: 结束日期（'YYYYMMDD'或'YYYY-MM-DD'）
    执行逻辑：为每个股票调用 ak.stock_zh_a_hist 并写入 QuestDB。
    """

    def __init__(self, orm):
        # 仅要求传入 orm，其余字段在 generate() 时设置
        super().__init__(orm, task_type="", task_desc="", params=None, priority=0)

    def generate(self, task_type: str, task_desc: str = "", params: Optional[Dict[str, Any]] = None, priority: int = 0,conn=None) -> str:
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
        return "Download_Full_Daily"
    def run(self, conn=None,params_str: str = None) -> bool:
        # 检查依赖
        if not (ak):
            logger.error("依赖不可用：akshare 未导入")
            return False
        if params_str:
            self.params_str = params_str
            
        params = self._parse_params()
        market = (params.get('market') or '').upper()
        
        
        adjust = (params.get('adjust') or '').lower()
        start_date = norm_date(params.get('start_date')) or '19900101'
        end_date = norm_date(params.get('end_date')) or datetime.now().strftime('%Y%m%d')

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

        # make_symbol已从backend.global_config.utils导入

        conn_local = conn 
        if not conn_local:
            logger.error("QuestDB 连接失败")
            return False
        try:
            total_saved = 0
            latest_date: Optional[datetime] = None
            for code in codes:
                symbol = make_symbol(code, market)
                adjust_all = ['', 'qfq', 'hfq'] if adjust == "all" else [adjust]
                for adj in adjust_all:
                    try:
                        df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start_date, end_date=end_date, adjust=adj)
                    except Exception as e:
                        logger.exception("akshare 拉取失败: code=%s, symbol=%s, adj=%s, error=%s", code, symbol, adj, e)
                        df = None
                    try:
                        saved = _insert_daily(code, df, adj, conn=conn_local)
                        total_saved += int(saved or 0)
                    except Exception as e:
                        logger.exception("写入失败: code=%s, adj=%s, error=%s", code, adj, e)
            logger.info("任务完成：codes=%s, total_saved=%s", codes, total_saved)
            return total_saved > 0
        finally:
            if conn is None:
                try:
                    conn_local.close()
                except Exception:
                    pass


