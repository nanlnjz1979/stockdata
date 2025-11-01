import logging
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta

from .base import BaseTask

# 数据源：Akshare（尽量兼容不同接口）
try:
    import akshare as ak
except Exception:
    ak = None

# QuestDB 连接（沿用 data_pipeline.collector 的连接方式）
try:
    from data_pipeline.collector import qdb_connect
except Exception:
    qdb_connect = None

logger = logging.getLogger(__name__)


def _num(x) -> Optional[float]:
    try:
        return float(x) if x not in (None, "") else None
    except Exception:
        return None




def _fetch_lhb_day(date_s: str):
    """尽可能使用 Akshare 拉取指定日期的龙虎榜明细。返回 DataFrame 或 None。"""
    if not ak:
        return None
    # 兼容多种可能的函数名/参数格式
    for fn_name in [
        "stock_lhb_detail_em",  # EastMoney
        "stock_lhb_detail_sina",  # Sina（若可用）
    ]:
        try:
            fn = getattr(ak, fn_name, None)
            if not fn:
                continue
            # 兼容 YYYYMMDD / YYYY-MM-DD 两种日期格式
            try:
                df = fn(date=date_s)
            except Exception:
                try:
                    d = datetime.strptime(date_s, "%Y%m%d").strftime("%Y-%m-%d")
                    df = fn(date=d)
                except Exception:
                    df = None
            if df is not None and not getattr(df, "empty", True):
                return df
        except Exception:
            continue
    return None


def _aggregate_lhb(codes: List[str], start_date: datetime, end_date: datetime) -> List[Tuple[str, str, float, float, float, float, float]]:
    """
    在 [start_date, end_date] 区间内按代码聚合：
    返回列表项为 (code, name, buy_amount, buy_times, sell_amount, sell_times, net_amount)
    金额单位按“万”处理（多数 Akshare 接口已是万元）。
    """
    # 规范代码列表
    codes = [c.strip() for c in codes if isinstance(c, str) and c.strip()]
    if not codes:
        return []

    accum: Dict[str, Dict[str, Any]] = {}
    day = start_date
    while day <= end_date:
        ds = day.strftime("%Y%m%d")
        df = _fetch_lhb_day(ds)
        if df is not None and not df.empty:
            # 兼容常见列名
            # 期望列：股票代码 / 股票简称 / 买入金额(万元) / 卖出金额(万元) / 净额(万元)
            code_col = None
            name_col = None
            buy_col = None
            sell_col = None
            net_col = None
            cols = list(df.columns)
            # 中文列
            for c in cols:
                if ("代码" in str(c)) and (code_col is None):
                    code_col = c
                if ("名称" in str(c) or "简称" in str(c)) and (name_col is None):
                    name_col = c
                if ("买入" in str(c)) and ("万" in str(c)) and (buy_col is None):
                    buy_col = c
                if ("卖出" in str(c)) and ("万" in str(c)) and (sell_col is None):
                    sell_col = c
                if ("净额" in str(c)) and ("万" in str(c)) and (net_col is None):
                    net_col = c
            # 备选英文或无单位列名
            if not buy_col:
                for c in cols:
                    if "buy" in str(c).lower():
                        buy_col = c; break
            if not sell_col:
                for c in cols:
                    if "sell" in str(c).lower():
                        sell_col = c; break
            if not net_col:
                for c in cols:
                    if "net" in str(c).lower():
                        net_col = c; break
            # 遍历并按 codes 过滤
            for _, r in df.iterrows():
                cd = str(r.get(code_col) or "").strip()
                if cd not in codes:
                    continue
                name = str(r.get(name_col) or cd)
                buy_amt = _num(r.get(buy_col)) or 0.0
                sell_amt = _num(r.get(sell_col)) or 0.0
                net_amt = _num(r.get(net_col)) if net_col else (buy_amt - sell_amt)
                item = accum.get(cd)
                if not item:
                    item = {
                        "name": name,
                        "buy_amount": 0.0,
                        "buy_times": 0.0,
                        "sell_amount": 0.0,
                        "sell_times": 0.0,
                        "net_amount": 0.0,
                    }
                    accum[cd] = item
                item["name"] = name or item["name"]
                item["buy_amount"] += float(buy_amt)
                item["sell_amount"] += float(sell_amt)
                item["net_amount"] += float(net_amt or (buy_amt - sell_amt))
                if buy_amt and buy_amt > 0:
                    item["buy_times"] += 1.0
                if sell_amt and sell_amt > 0:
                    item["sell_times"] += 1.0
        day += timedelta(days=1)

    out: List[Tuple[str, str, float, float, float, float, float]] = []
    for cd, v in accum.items():
        out.append(
            (
                cd,
                str(v.get("name") or cd),
                float(v.get("buy_amount") or 0.0),
                float(v.get("buy_times") or 0.0),
                float(v.get("sell_amount") or 0.0),
                float(v.get("sell_times") or 0.0),
                float(v.get("net_amount") or 0.0),
            )
        )
    return out


def _insert_inst_trading(rows: List[Tuple[str, str, float, float, float, float, float]], query_type: int, conn=None) -> int:
    if not rows:
        return 0
    conn_local = conn or (qdb_connect() if qdb_connect else None)
    if not conn_local:
        return 0
    try:
        cur = conn_local.cursor()
        ingest_date = datetime.now().date()
        values = [
            (
                ingest_date,
                cd,
                name,
                _num(buy_amt),
                _num(buy_times),
                _num(sell_amt),
                _num(sell_times),
                _num(net_amt),
                int(query_type),
            )
            for (cd, name, buy_amt, buy_times, sell_amt, sell_times, net_amt) in rows
        ]
        try:
            cur.executemany(
                """
                insert into inst_trading_tracker (
                  ingest_date, code, name, buy_amount, buy_times, sell_amount, sell_times, net_amount, query_type
                ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                values,
            )
        except Exception as e:
            logger.exception("批量写入失败: %s", e)
            if conn is None:
                try:
                    conn_local.close()
                except Exception:
                    pass
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


class DTBInstTradingTrackerTask(BaseTask):
    """
    机构交易龙虎榜跟踪任务：
    - 下载指定时间窗（query_type=5/10/30/60）的每日龙虎榜明细
    - 按代码聚合为：累积买入额/卖出额/净额（单位: 万）及买入/卖出次数
    - 写入 QuestDB 表 inst_trading_tracker（不使用 sqlite）

    params:
      - codes: ["000001", "600000", ...]（或提供单个 'code'）
      - query_type: 5/10/30/60（默认 5）
      - end_date: 结束日期（可选，YYYYMMDD/YYYY-MM-DD；默认今天）
    """

    def __init__(self, orm):
        super().__init__(orm, task_type="", task_desc="", params=None, priority=0)

    def generate(self, task_type: str, task_desc: str = "", params: Optional[Dict[str, Any]] = None, priority: int = 0) -> str:
        self.task_type = task_type
        self.task_desc = task_desc
        self.params_str = self._ensure_json_str(params)
        self.priority = priority
        return super().generate()

    def _parse_params(self) -> Dict[str, Any]:
        try:
            import json
            return json.loads(self.params_str or "{}")
        except Exception:
            return {}

    def run(self, conn=None) -> bool:
        if not ak:
            logger.error("依赖不可用：akshare 未导入")
            return False
        params = self._parse_params()
        # 收集代码
        codes: List[str] = []
        code_single = params.get("code")
        codes_multi = params.get("codes")
        if isinstance(code_single, str) and code_single.strip():
            codes.append(code_single.strip())
        if isinstance(codes_multi, list):
            for c in codes_multi:
                if isinstance(c, str) and c.strip():
                    codes.append(c.strip())
        codes = list(dict.fromkeys(codes))
        if not codes:
            logger.warning("未提供有效的股票代码（params 需包含 'code' 或 'codes'）")
            return False

        # 时间窗
        qt = int(params.get("query_type") or 5)
        if qt not in (5, 10, 30, 60):
            qt = 5
        # 结束日期（默认今天）
        end_s = params.get("end_date")
        try:
            if end_s:
                if "-" in str(end_s):
                    end_date = datetime.strptime(str(end_s), "%Y-%m-%d")
                else:
                    end_date = datetime.strptime(str(end_s), "%Y%m%d")
            else:
                end_date = datetime.now()
        except Exception:
            end_date = datetime.now()
        start_date = end_date - timedelta(days=qt - 1)

        # 聚合
        agg_rows = _aggregate_lhb(codes, start_date=start_date, end_date=end_date)
        if not agg_rows:
            logger.warning("在时间窗内未聚合到龙虎榜数据: codes=%s, query_type=%s", codes, qt)
            return False

        conn_local = conn
        if not conn_local:
            logger.error("QuestDB 连接失败")
            return False
        try:
            saved = _insert_inst_trading(agg_rows, query_type=qt, conn=conn_local)
            logger.info(
                "任务完成：codes=%s, query_type=%s, total_saved=%s",
                codes,
                qt,
                saved,
            )
            return bool(saved)
        finally:
            if conn is None:
                try:
                    conn_local.close()
                except Exception:
                    pass