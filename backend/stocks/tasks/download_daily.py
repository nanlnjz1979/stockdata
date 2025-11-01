import json
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime

from .base import BaseTask

def _num(x):
    try:
        return float(x) if x not in (None, '') else None
    except Exception:
        return None


def _int(x):
    try:
        return int(x) if x not in (None, '') else None
    except Exception:
        return None

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
                trade_date.date(),
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
    从 BaseTask 派生的任务：下载并写入股票日线数据到 QuestDB。
    params 支持：
      - code: 单个股票代码（字符串）
      - codes: 多个股票代码（列表）
      - market: 市场标识（可选，'SH'/'SZ'/'BJ'，默认空）
      - start_date: 开始日期（'YYYYMMDD'或'YYYY-MM-DD'）
      - end_date: 结束日期（'YYYYMMDD'或'YYYY-MM-DD'）
    执行逻辑：为每个股票调用 ak.stock_zh_a_daily 并写入 QuestDB。
    """

    def __init__(self, orm):
        # 仅要求传入 orm，其余字段在 generate() 时设置
        super().__init__(orm, task_type="", task_desc="", params=None, priority=0)

    def generate(self, task_type: str, task_desc: str = "", params: Optional[Dict[str, Any]] = None, priority: int = 0) -> str:
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

    def run(self, conn=None) -> bool:
        # 检查依赖
        if not (ak):
            logger.error("依赖不可用：akshare 未导入")
            return False
        params = self._parse_params()
        market = (params.get('market') or '').upper()
        adjust = (params.get('adjust') or '').lower()
        # 归一化日期
        def _norm_date(d):
            if not d:
                return None
            if isinstance(d, datetime):
                return d.strftime("%Y%m%d")
            s = str(d)
            try:
                if '-' in s:
                    s = s.replace('-', '')
                return s
            except Exception:
                return None
        start_date = _norm_date(params.get('start_date')) or '19900101'
        end_date = _norm_date(params.get('end_date')) or datetime.now().strftime('%Y%m%d')

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

        def make_symbol(c: str, m: str) -> str:
            m = (m or '').upper()
            if m == 'SH':
                return 'sh' + c
            if m == 'SZ':
                return 'sz' + c
            if m == 'BJ':
                return 'bj' + c
            return 'sz' + c

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
                        df = ak.stock_zh_a_daily(symbol=symbol, start_date=start_date, end_date=end_date, adjust=adj)
                    except Exception as e:
                        logger.exception("akshare 拉取失败: code=%s, symbol=%s, adj=%s, error=%s", code, symbol, adj, e)
                        df = None
                    try:
                        saved = _insert_daily(code, df, adj, conn=conn_local)
                        total_saved += int(saved or 0)
                        if df is not None and not df.empty:
                            df2 = df.rename(columns={'日期': 'date'})
                            for _, r in df2.iterrows():
                                d = r.get('date')
                                if not d:
                                    continue
                                try:
                                    td = d if isinstance(d, datetime) else datetime.strptime(str(d), '%Y-%m-%d')
                                    latest_date = max(latest_date or td, td)
                                except Exception:
                                    pass
                    except Exception as e:
                        logger.exception("写入失败: code=%s, adj=%s, error=%s", code, adj, e)
            logger.info("任务完成：codes=%s, total_saved=%s, latest_date=%s", codes, total_saved, latest_date.date() if latest_date else None)
            return total_saved > 0
        finally:
            if conn is None:
                try:
                    conn_local.close()
                except Exception:
                    pass

# 迁移自 qdb_orm.py：提供 QuestDB 任务表的轻量 ORM 适配器
try:
    from data_pipeline.collector import qdb_connect
except Exception:
    qdb_connect = None


class QdbOrm:
    """
    最简 QuestDB ORM 适配器：提供任务表的常用操作，供 BaseTask 使用。
    提供：
      - insert_task(task_id, task_type, task_desc, task_params, priority, status)
      - update_task_status(task_id, status)
      - get_task(task_id)
      - list_tasks(status=None, task_type=None, limit=100, offset=0)
      - create_task(...)
      - update_task(...)
      - delete_task(task_id)
      - next_pending_task(task_type=None)
      - claim_task(task_id)
      - complete_task(task_id, success)
    可复用外部连接或内部创建连接。
    """

    def __init__(self, conn: Optional[object] = None) -> None:
        self._external_conn = conn
        self._conn = conn or (qdb_connect() if qdb_connect else None)
        if not self._conn:
            raise RuntimeError("QuestDB 连接不可用")
        

    def close(self) -> None:
        if not self._external_conn and self._conn:
            try:
                self._conn.close()
            except Exception:
                pass

    # 基础插入/更新
    def insert_task(self, task_id: str, task_type: str, task_desc: str, task_params: str, priority: int, status: str) -> None:
        cur = self._conn.cursor()
        cur.execute(
            """
            insert into tasks (task_id, task_type, task_desc, task_params, priority, status, created_at)
            values (%s, %s, %s, %s, %s, %s, now())
            """,
            (task_id, task_type, task_desc, task_params, int(priority or 0), status),
        )

    def update_task_status(self, task_id: str, status: str) -> None:
        cur = self._conn.cursor()
        # 当置为"处理中"时设置 started_at；当置为"成功/失败/已取消"时设置 ended_at
        cur.execute(
            """
            update tasks
            set status=%s,
                started_at = (case when %s='处理中' and started_at is null then now() else started_at end),
                ended_at   = (case when %s in ('成功','失败','已取消') then now() else ended_at end)
            where task_id=%s
            """,
            (status, status, status, task_id),
        )

    # 查询/列表
    def _row_to_dict(self, cur, row):
        try:
            cols = [d[0] for d in (cur.description or [])]
            return {cols[i]: row[i] for i in range(len(cols))}
        except Exception:
            return None

    def get_task(self, task_id: str):
        cur = self._conn.cursor()
        cur.execute(
            "select task_id, task_type, task_desc, task_params, priority, status from tasks where task_id=%s limit 1",
            (task_id,),
        )
        row = cur.fetchone()
        return self._row_to_dict(cur, row) if row else None

    def list_tasks(self, status: Optional[str] = None, task_type: Optional[str] = None, limit: int = 100, offset: int = 0):
        cur = self._conn.cursor()
        where = []
        params: List[object] = []
        if status:
            where.append("status=%s")
            params.append(status)
        if task_type:
            where.append("task_type=%s")
            params.append(task_type)
        where_sql = (" where " + " and ".join(where)) if where else ""
        # QuestDB 不支持 OFFSET，移除 offset，仅保留 limit
        cur.execute(
            f"select task_id, task_type, task_desc, task_params, priority, status from tasks{where_sql} order by priority desc limit %s",
            (*params, int(limit or 100)),
        )
        rows = cur.fetchall() or []
        return [self._row_to_dict(cur, r) for r in rows]

    # 便捷创建/更新/删除
    def create_task(self, task_type: str, task_desc: str = "", task_params: str = "{}", priority: int = 0, status: str = "待处理", task_id: Optional[str] = None) -> str:
        import uuid
        tid = uuid.uuid4().hex
        self.insert_task(tid, task_type, task_desc, task_params, priority, status)
        return tid

    def update_task(self, task_id: str, task_desc: Optional[str] = None, task_params: Optional[str] = None, priority: Optional[int] = None, status: Optional[str] = None) -> None:
        sets = []
        params: List[object] = []
        if task_desc is not None:
            sets.append("task_desc=%s")
            params.append(task_desc)
        if task_params is not None:
            sets.append("task_params=%s")
            params.append(task_params)
        if priority is not None:
            sets.append("priority=%s")
            params.append(int(priority))
        if status is not None:
            sets.append("status=%s")
            params.append(status)
        if not sets:
            return
        params.append(task_id)
        cur = self._conn.cursor()
        cur.execute(f"update tasks set {', '.join(sets)} where task_id=%s", tuple(params))

    def delete_task(self, task_id: str) -> None:
        cur = self._conn.cursor()
        cur.execute("delete from tasks where task_id=%s", (task_id,))

    # 选择与认领/完成
    def next_pending_task(self, task_type: Optional[str] = None):
        cur = self._conn.cursor()
        where = ["status=%s"]
        params: List[object] = ["待处理"]
        if task_type:
            where.append("task_type=%s")
            params.append(task_type)
        where_sql = " and ".join(where)
        cur.execute(
            f"select task_id, task_type, task_desc, task_params, priority, status from tasks where {where_sql} order by priority desc limit 1",
            tuple(params),
        )
        row = cur.fetchone()
        return self._row_to_dict(cur, row) if row else None

    def claim_task(self, task_id: str) -> bool:
        cur = self._conn.cursor()
        # 乐观锁式认领：仅当当前为待处理时更新为处理中，并记录开始时间
        cur.execute("update tasks set status=%s, started_at=(case when started_at is null then now() else started_at end) where task_id=%s and status=%s", ("处理中", task_id, "待处理"))
        try:
            # 再读一遍确认
            t = self.get_task(task_id)
            return bool(t and t.get("status") == "处理中")
        except Exception:
            return True

    def complete_task(self, task_id: str, success: bool) -> None:
        # 完成时同时写入结束时间
        self.update_task_status(task_id, "成功" if success else "失败")