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

def _aggregate_lhb(day: str) -> List[Tuple[str, str, float, float, float, float, float]]:
    """
    返回列表项为 (code, name, buy_amount, buy_times, sell_amount, sell_times, net_amount)
    金额单位按“万”处理（多数 Akshare 接口已是万元）。
    """
    # 规范代码列表
    codes = [c.strip() for c in codes if isinstance(c, str) and c.strip()]
    if not codes:
        return []

    try:
        df = ak.stock_lhb_jgzz_sina(day)
        if df is None or df.empty:
            logger.warning("ak.stock_lhb_jgzz_sina(%s) 返回空数据", query_type)
            return False
    except Exception as e:
        logger.exception("获取龙虎榜数据失败: %s", e)
        return False
    # 按 DataFrame 列名聚合
    accum: Dict[str, Dict[str, float]] = {}
    for _, row in df.iterrows():
        code = str(row["股票代码"]).zfill(6)
        name = str(row["股票名称"])
        buy_amt = float(row["累积买入额"])
        buy_cnt = int(row["买入次数"])
        sell_amt = float(row["累积卖出额"])
        sell_cnt = int(row["卖出次数"])
        net_amt = float(row["净额"])

        if code not in accum:
            accum[code] = {"name": name, "buy_amount": 0.0, "buy_times": 0,
                           "sell_amount": 0.0, "sell_times": 0, "net_amount": 0.0}

        accum[code]["buy_amount"] += buy_amt
        accum[code]["buy_times"] += buy_cnt
        accum[code]["sell_amount"] += sell_amt
        accum[code]["sell_times"] += sell_cnt
        accum[code]["net_amount"] += net_amt

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


def _insert_inst_trading(rows: List[Tuple[str, str, float, float, float, float, float]], day: int, conn=None) -> int:
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
        self.priority = priority
        self.params_str = self._ensure_json_str(params)
        
        last_update_date = self._get_last_update_date()
        
        self.params_str=self._ensure_json_str({
            "last_update_date": str(last_update_date),
        })
        return super().generate()

    def _get_last_update_date(self) -> Optional[datetime.date]:
        """
        返回龙虎榜机构追踪表（inst_trading_tracker）中最后一次更新的日期。
        若表为空或查询失败，返回 None。
        """
        conn = None
        try:
            conn = qdb_connect() if qdb_connect else None
            if not conn:
                logger.warning("QuestDB 连接失败，无法获取最后更新日期")
                return None
            cur = conn.cursor()
            cur.execute("SELECT max(ingest_date) FROM inst_trading_tracker")
            row = cur.fetchone()
            if row and row[0]:
                # QuestDB 返回的 date 类型可直接使用
                return row[0]
            return None
        except Exception as e:
            logger.exception("查询最后更新日期失败: %s", e)
            return None
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
        

    def _parse_params(self) -> Dict[str, Any]:
        try:
            import json
            return json.loads(self.params_str or "{}")
        except Exception:
            return {}
    def taskID(self) -> str:
        return "LHB_InstituteTrack"

    def run(self, conn=None) -> bool:
        # 读取任务参数
        if conn is None:
            conn = qdb_connect()
        from tasksOrm import QtasksOrm
        orm = QtasksOrm(conn)
        cfgs = orm.list_tasks(self.taskID()) # task_id, task_type, task_desc, task_params, priority, status

        for item in cfgs:
            task_params = item.get("task_params", "{}")
            last_update_date = task_params.get("last_update_date", None)
            # 仅当 last_update_date 早于今天时才继续拉取数据

            if last_update_date and last_update_date <= datetime.now().date():
                continue
            # 拉取龙虎榜数据
            for day in ["5", "10", "30", "60"]:
                
                aggregated = _aggregate_lhb(day)

                if not aggregated:
                    logger.info("在 %s未找到指定天数 %s 的龙虎榜数据", last_update_date, day)

            # 写入 QuestDB
            inserted = _insert_inst_trading(aggregated, int(day), conn=conn)
            logger.info("成功写入 %s 条机构龙虎榜聚合记录", inserted)
            return inserted > 0
