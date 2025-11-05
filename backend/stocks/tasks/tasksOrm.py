from typing import Optional, List
import threading
import logging
import sys
import os

# 添加项目路径以便导入连接池
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# 迁移自 qdb_orm.py：提供 QuestDB 任务表的轻量 ORM 适配器
try:
    from data_pipeline.collector import qdb_connect
except Exception:
    qdb_connect = None


class QtasksOrm:
    """
    线程安全的 QuestDB ORM 适配器：提供任务表的常用操作，供 BaseTask 使用。
    实现为单例模式，确保全局唯一实例，避免多线程创建多个实例导致资源浪费。
    增加了线程安全机制，避免多线程并发操作任务表时的数据不一致问题。
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
    # 单例模式实现
    _instance = None
    _instance_lock = threading.RLock()
    
    def __new__(cls, conn: Optional[object] = None) -> 'QtasksOrm':
        # 使用双重检查锁定模式实现线程安全的单例
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    # 初始化连接和锁，只在第一次创建实例时执行
                    cls._instance._initialize(conn)
        # 注意：如果提供了外部连接，不更新现有实例的连接，以保持单例的一致性
        return cls._instance
    
    def _initialize(self, conn: Optional[object] = None) -> None:
        """
        初始化实例变量，只在第一次创建实例时调用
        """
        self._external_conn = conn
        if conn:
            self._conn = conn
        else:
            # 尝试使用连接池获取连接
            try:
                from db.db_pool import get_conn
                self._conn = get_conn()
            except Exception as e:
                logging.warning(f"连接池获取失败，回退到默认连接: {e}")
                self._conn = qdb_connect() if qdb_connect else None
        
        if not self._conn:
            raise RuntimeError("QuestDB 连接不可用")
        # 添加线程锁，保证关键操作的线程安全
        self._lock = threading.RLock()
        # 添加任务级别的锁字典，用于细粒度控制
        self._task_locks = {}
        self._task_locks_lock = threading.RLock()
    
    def __init__(self, conn: Optional[object] = None) -> None:
        # 重写__init__避免重复初始化
        pass
        

    def close(self) -> None:
        """
        关闭数据库连接并清理资源
        
        注意：由于是单例模式，调用close后实例仍然存在，但连接会被关闭。
        再次使用该实例时需要重新初始化连接。
        """
        with self._lock:
            if not self._external_conn and hasattr(self, '_conn') and self._conn:
                try:
                    # 尝试使用连接池归还连接
                    from db.db_pool import put_conn
                    put_conn(self._conn)
                except Exception:
                    # 回退到直接关闭连接
                    try:
                        self._conn.close()
                    except Exception:
                        pass
                self._conn = None
            
            # 清理任务锁资源
            with self._task_locks_lock:
                self._task_locks.clear()

    # 基础插入/更新
    def insert_task(self, task_id: str, task_type: str, task_desc: str, task_params: str, priority: int, status: str) -> None:
        """
        线程安全的任务插入方法
        """
        with self._get_task_lock(task_id):
            cur = self._conn.cursor()
            try:
                self._conn.autocommit = False
                cur.execute(
                    """
                    insert into tasks (task_id, task_type, task_desc, task_params, priority, status, created_at)
                    values (%s, %s, %s, %s, %s, %s, now())
                    """,
                    (task_id, task_type, task_desc, task_params, int(priority or 0), status),
                )
                try:
                    self._conn.commit()
                except Exception:
                    pass
            except Exception as e:
                try:
                    self._conn.rollback()
                except Exception:
                    pass
                raise e
            finally:
                try:
                    self._conn.autocommit = True
                except Exception:
                    pass

    def update_task_status(self, task_id: str, status: str) -> None:
        """
        线程安全的任务状态更新方法
        """
        with self._get_task_lock(task_id):
            cur = self._conn.cursor()
            try:
                self._conn.autocommit = False
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
                try:
                    self._conn.commit()
                except Exception:
                    pass
            except Exception as e:
                try:
                    self._conn.rollback()
                except Exception:
                    pass
                raise e
            finally:
                try:
                    self._conn.autocommit = True
                except Exception:
                    pass

    # 查询/列表
    def _row_to_dict(self, cur, row):
        try:
            cols = [d[0] for d in (cur.description or [])]
            return {cols[i]: row[i] for i in range(len(cols))}
        except Exception:
            return None

    def get_task(self, task_id: str):
        with self._lock:
            if not hasattr(self, '_conn') or  self._conn.closed:
                # 连接已关闭，重新初始化
                self._initialize()
                
            cur = self._conn.cursor()
            cur.execute(
                "select task_id, task_type, task_desc, task_params, priority, status from tasks where task_id=%s limit 1",
                (task_id,),
            )
            row = cur.fetchone()
            return self._row_to_dict(cur, row) if row else None

    def list_tasks(self, status: Optional[str] = None, task_type: Optional[str] = None, limit: int = 100, offset: int = 0):
        
        if self._conn.closed or not hasattr(self, '_conn') or not self._conn:
            # 连接已关闭，重新初始化
            self._initialize()
                
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
        """
        线程安全的任务更新方法
        """
        if not any([task_desc is not None, task_params is not None, priority is not None, status is not None]):
            return
            
        with self._lock:
            if not hasattr(self, '_conn') or not self._conn.closed:
                # 连接已关闭，重新初始化
                self._initialize()
                
        with self._get_task_lock(task_id):
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
            try:
                self._conn.autocommit = False
                cur.execute(f"update tasks set {', '.join(sets)} where task_id=%s", tuple(params))
                try:
                    self._conn.commit()
                except Exception:
                    pass
            except Exception as e:
                try:
                    self._conn.rollback()
                except Exception:
                    pass
                raise e
            finally:
                try:
                    self._conn.autocommit = True
                except Exception:
                    pass

    def delete_task(self, task_id: str) -> None:
        """
        线程安全的任务删除方法
        """
        with self._lock:
            if not hasattr(self, '_conn') or self._conn.closed:
                # 连接已关闭，重新初始化
                self._initialize()
                
        with self._get_task_lock(task_id):
            cur = self._conn.cursor()
            try:
                self._conn.autocommit = False
                cur.execute("delete from tasks where task_id=%s", (task_id,))
                try:
                    self._conn.commit()
                except Exception:
                    pass
                
                # 删除任务后清理对应的任务锁
                if hasattr(self, '_task_locks_lock') and hasattr(self, '_task_locks'):
                    with self._task_locks_lock:
                        if task_id in self._task_locks:
                            del self._task_locks[task_id]
                            
            except Exception as e:
                try:
                    self._conn.rollback()
                except Exception:
                    pass
                raise e
            finally:
                try:
                    self._conn.autocommit = True
                except Exception:
                    pass

    # 选择与认领/完成
    def next_pending_task(self, task_type: Optional[str] = None):
        """
        线程安全的获取下一个待处理任务方法
        使用全局锁避免多个线程同时获取同一个任务
        """
        with self._lock:
            if not hasattr(self, '_conn') or not self._conn.closed:
                # 连接已关闭，重新初始化
                self._initialize()
                
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

    def _get_task_lock(self, task_id: str) -> threading.RLock:
        """获取特定任务ID的锁，用于细粒度的任务级并发控制"""
        # 确保锁字典和锁对象存在
        if not hasattr(self, '_task_locks'):
            self._task_locks = {}
        if not hasattr(self, '_task_locks_lock'):
            self._task_locks_lock = threading.RLock()
            
        with self._task_locks_lock:
            if task_id not in self._task_locks:
                self._task_locks[task_id] = threading.RLock()
            return self._task_locks[task_id]
    
    def claim_task(self, task_id: str) -> bool:
        """
        线程安全的任务认领方法，使用任务级锁和数据库事务确保并发安全
        """
        with self._lock:
            if not hasattr(self, '_conn') or self._conn.closed:
                # 连接已关闭，重新初始化
                self._initialize()
                
        # 使用任务级别的锁，避免同一任务被多个线程同时认领
        with self._get_task_lock(task_id):
            try:
                cur = self._conn.cursor()
                # 开启事务（如果数据库支持）
                try:
                    self._conn.autocommit = False
                except Exception:
                    pass  # 忽略不支持事务的数据库
                
                # 乐观锁式认领：仅当当前为待处理时更新为处理中，并记录开始时间
                cur.execute(
                    "update tasks set status=%s, started_at=(case when started_at is null then now() else started_at end) "
                    "where task_id=%s and status=%s", 
                    ("处理中", task_id, "待处理")
                )
                
                # 获取受影响的行数，确认是否成功更新
                affected_rows = cur.rowcount
                
                # 提交事务
                try:
                    self._conn.commit()
                except Exception:
                    pass
                finally:
                    # 恢复自动提交模式
                    try:
                        self._conn.autocommit = True
                    except Exception:
                        pass
                
                # 验证认领结果
                if affected_rows > 0:
                    return True
                
                # 再次检查任务状态以确保结果准确性
                t = self.get_task(task_id)
                return bool(t and t.get("status") == "处理中")
                
            except Exception as e:
                # 发生异常时回滚事务
                try:
                    self._conn.rollback()
                except Exception:
                    pass
                finally:
                    try:
                        self._conn.autocommit = True
                    except Exception:
                        pass
                
                logging.error(f"Claim task {task_id} failed: {e}")
                return False

    def complete_task(self, task_id: str, success: bool) -> None:
        """
        线程安全的任务完成方法
        """
        with self._lock:
            if not hasattr(self, '_conn') or self._conn.closed:
                # 连接已关闭，重新初始化
                self._initialize()
                
        with self._get_task_lock(task_id):
            # 完成时同时写入结束时间
            self.update_task_status(task_id, "成功" if success else "失败")
            
            # 当任务完成时，可以考虑清理任务锁以避免内存泄漏
            # 但需要小心处理，确保没有其他线程正在使用该锁
            # 这里简化处理，保留任务锁