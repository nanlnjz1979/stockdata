import logging
from typing import Optional, List

try:
    from data_pipeline.collector import qdb_connect, qdb_ensure_tables
except Exception:
    qdb_connect = None
    qdb_ensure_tables = None

logger = logging.getLogger(__name__)


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
        try:
            if qdb_ensure_tables:
                qdb_ensure_tables(self._conn)
        except Exception:
            pass

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
            insert into tasks (task_id, task_type, task_desc, task_params, priority, status)
            values (%s, %s, %s, %s, %s, %s)
            """,
            (task_id, task_type, task_desc, task_params, int(priority or 0), status),
        )

    def update_task_status(self, task_id: str, status: str) -> None:
        cur = self._conn.cursor()
        cur.execute("update tasks set status=%s where task_id=%s", (status, task_id))

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
        # 乐观锁式认领：仅当当前为待处理时更新为处理中
        cur.execute("update tasks set status=%s where task_id=%s and status=%s", ("处理中", task_id, "待处理"))
        try:
            # 再读一遍确认
            t = self.get_task(task_id)
            return bool(t and t.get("status") == "处理中")
        except Exception:
            return True

    def complete_task(self, task_id: str, success: bool) -> None:
        self.update_task(task_id, status=("成功" if success else "失败"))
