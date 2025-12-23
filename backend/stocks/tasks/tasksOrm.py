from typing import Optional, List, Dict, Any
import threading
import logging
import sys
import os
import datetime

# 添加项目路径以便导入连接池
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# 导入 MongoDB 连接池和配置工具
from db import get_mongo_conn, put_mongo_conn
from global_config import FileConfig


class QtasksOrm:
    """
    线程安全的 MongoDB ORM 适配器：提供任务表的常用操作，供 BaseTask 使用。
    实现为单例模式，确保全局唯一实例，避免多线程创建多个实例导致资源浪费。
    增加了线程安全机制，避免多线程并发操作任务表时的数据不一致问题。
    提供：
      - insert_task(task_id, task_type, task_desc, task_params, priority, status)
      - update_task_status(task_id, status)
      - get_task(task_id)
      - list_tasks(status=None, task_type=None, limit=10000, offset=0)
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
    
    def __new__(cls) -> 'QtasksOrm':
        # 使用双重检查锁定模式实现线程安全的单例
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    # 初始化连接和锁，只在第一次创建实例时执行
                    cls._instance._initialize()
        # 忽略传入的conn参数，始终使用连接池
        return cls._instance
    
    @classmethod
    def get_instance(cls) -> 'QtasksOrm':
        """
        类方法：获取单例实例
        """
        return cls()
    
    def _initialize(self) -> None:
        """
        初始化实例变量，只在第一次创建实例时调用
        直接使用 MongoDB 连接池获取连接
        """
        # 忽略外部传入的conn参数，始终使用连接池
        self._external_conn = False
        
        # 使用 MongoDB 连接池获取连接
        try:
            self._conn = get_mongo_conn()
        except Exception as e:
            logging.error(f"创建MongoDB连接失败: {e}")
            self._conn = None
        
        if not self._conn:
            raise RuntimeError("MongoDB 连接不可用")
        
        # 获取 MongoDB 数据库和集合
        self._db = self._conn[FileConfig.get('mongodb').get('database', 'stockdata')]
        self._tasks_col = self._db['tasks']
        
        # 添加线程锁，保证关键操作的线程安全
        self._lock = threading.RLock()
        # 添加任务级别的锁字典，用于细粒度控制
        self._task_locks = {}
        self._task_locks_lock = threading.RLock()
    
    def __init__(self) -> None:
        # 重写__init__避免重复初始化
        pass
        

    def close(self) -> None:
        """
        关闭数据库连接并清理资源
        
        注意：由于是单例模式，调用close后实例仍然存在，但连接会被关闭。
        再次使用该实例时需要重新初始化连接。
        """
        with self._lock:
            if hasattr(self, '_conn') and self._conn:
                try:
                    # 使用连接池归还连接
                    put_mongo_conn(self._conn)
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
            try:
                # 准备插入的数据
                now = datetime.datetime.now()
                task_data = {
                    '_id': task_id,
                    'task_id': task_id,
                    'task_type': task_type,
                    'task_desc': task_desc,
                    'task_params': task_params,
                    'priority': int(priority or 0),
                    'status': status,
                    'created_at': now,
                    'started_at': None,
                    'ended_at': None
                }
                
                # 插入到 MongoDB 集合
                self._tasks_col.insert_one(task_data)
                logging.debug(f"任务 {task_id} 已插入到 MongoDB")
            except Exception as e:
                logging.error(f"插入任务 {task_id} 失败: {str(e)}")
                raise e

    def update_task_status(self, task_id: str, status: str) -> None:
        """
        线程安全的任务状态更新方法
        """
        # 首先检查并确保连接有效
        with self._lock:
            if not hasattr(self, '_conn') or not self._conn:
                # 连接不存在或已关闭，重新初始化
                self._initialize()
                if not hasattr(self, '_conn') or not self._conn:
                    logging.error(f"无法获取有效数据库连接，任务 {task_id} 状态更新失败")
                    raise RuntimeError("数据库连接不可用")
        
        with self._get_task_lock(task_id):
            try:
                # 获取当前时间
                now = datetime.datetime.now()
                
                # 准备更新操作
                update_fields = {
                    '$set': {
                        'status': status
                    }
                }
                
                # 根据状态更新开始时间和结束时间
                if status == '处理中':
                    update_fields['$setOnInsert'] = {
                        'started_at': now
                    }
                elif status in ['成功', '失败', '已取消']:
                    update_fields['$set']['ended_at'] = now
                
                # 执行更新操作
                result = self._tasks_col.update_one(
                    {'task_id': task_id},  # 过滤条件
                    update_fields,         # 更新操作
                    upsert=False           # 不插入新文档
                )
                
                # 检查是否有文档受到影响
                if result.matched_count == 0:
                    logging.warning(f"未找到任务 {task_id} 或更新失败")
                else:
                    logging.debug(f"任务 {task_id} 状态已更新为 {status}")
                    
            except Exception as e:
                # 记录详细错误信息
                error_msg = f"更新任务 {task_id} 状态时出错: {str(e)}"
                logging.error(error_msg)
                print(error_msg)
                raise

    # 查询/列表
    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            if not hasattr(self, '_conn') or not self._conn:
                # 连接已关闭，重新初始化
                self._initialize()
                
            # 使用 MongoDB 查询获取单个任务
            task = self._tasks_col.find_one(
                {'task_id': task_id},
                {'_id': 0, 'task_id': 1, 'task_type': 1, 'task_desc': 1, 'task_params': 1, 'priority': 1, 'status': 1, 'created_at': 1, 'started_at': 1, 'ended_at': 1}
            )
            return task if task else None

    def list_tasks(self, status: Optional[str] = None, task_type: Optional[str] = None, task_params: Optional[str] = None, limit: int = 10000) -> List[Dict[str, Any]]:
        
        if not hasattr(self, '_conn') or not self._conn:
            # 连接不存在或已关闭，重新初始化
            self._initialize()
                
        # 构建 MongoDB 查询条件
        query = {}
        if status:
            query['status'] = status
        if task_type:
            query['task_type'] = task_type
        if task_params:
            query['task_params'] = task_params
        
        # 构建排序条件
        sort_order = [
            ('priority', -1),  # 按优先级降序
            ('started_at', -1)  # 按开始时间降序
        ]
        
        # 执行查询
        tasks = self._tasks_col.find(
            query,
            {'_id': 0, 'task_id': 1, 'task_type': 1, 'task_desc': 1, 'task_params': 1, 'priority': 1, 'status': 1, 'created_at': 1, 'started_at': 1, 'ended_at': 1}
        ).sort(sort_order).limit(int(limit or 100))
        
        # 将 MongoDB 游标转换为列表
        return list(tasks)

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
            if not hasattr(self, '_conn') or not self._conn:
                # 连接不存在或已关闭，重新初始化
                self._initialize()
                
        with self._get_task_lock(task_id):
            # 准备更新字段
            update_fields = {
                '$set': {}
            }
            
            if task_desc is not None:
                update_fields['$set']['task_desc'] = task_desc
            if task_params is not None:
                update_fields['$set']['task_params'] = task_params
            if priority is not None:
                update_fields['$set']['priority'] = int(priority)
            if status is not None:
                update_fields['$set']['status'] = status
            
            if not update_fields['$set']:
                return
            
            # 执行更新操作
            result = self._tasks_col.update_one(
                {'task_id': task_id},
                update_fields,
                upsert=False
            )
            
            # 检查是否有文档受到影响
            if result.matched_count == 0:
                logging.warning(f"未找到任务 {task_id} 或更新失败")
            else:
                logging.debug(f"任务 {task_id} 已更新")

    def delete_task(self, task_id: str) -> None:
        """
        线程安全的任务删除方法
        """
        with self._lock:
            if not hasattr(self, '_conn') or not self._conn:
                # 连接已关闭，重新初始化
                self._initialize()
                
        with self._get_task_lock(task_id):
            # 执行删除操作
            result = self._tasks_col.delete_one({'task_id': task_id})
            
            # 检查是否有文档受到影响
            if result.deleted_count == 0:
                logging.warning(f"未找到任务 {task_id} 或删除失败")
            else:
                logging.debug(f"任务 {task_id} 已删除")
                
            # 删除任务后清理对应的任务锁
            if hasattr(self, '_task_locks_lock') and hasattr(self, '_task_locks'):
                with self._task_locks_lock:
                    if task_id in self._task_locks:
                        del self._task_locks[task_id]

    # 选择与认领/完成
    def next_pending_task(self, task_type: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        线程安全的获取下一个待处理任务方法
        使用全局锁避免多个线程同时获取同一个任务
        """
        with self._lock:
            if not hasattr(self, '_conn') or not self._conn:
                # 连接不存在或已关闭，重新初始化
                self._initialize()
                
            # 构建查询条件
            query = {'status': '待处理'}
            if task_type:
                query['task_type'] = task_type
            
            # 构建排序条件
            sort_order = [
                ('priority', -1),  # 按优先级降序
                ('started_at', 1)  # 按开始时间升序
            ]
            
            # 执行查询
            cursor = self._tasks_col.find(
                query,
                {'_id': 0, 'task_id': 1, 'task_type': 1, 'task_desc': 1, 'task_params': 1, 'priority': 1, 'status': 1, 'created_at': 1, 'started_at': 1, 'ended_at': 1}
            ).sort(sort_order).limit(1)
            
            # 获取第一个结果
            task = next(cursor, None)
            return task if task else None

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
            if not hasattr(self, '_conn') or not self._conn:
                # 连接已关闭，重新初始化
                self._initialize()
                
        # 使用任务级别的锁，避免同一任务被多个线程同时认领
        with self._get_task_lock(task_id):
            try:
                # 获取当前时间
                now = datetime.datetime.now()
                
                # 乐观锁式认领：仅当当前为待处理时更新为处理中，并记录开始时间
                result = self._tasks_col.update_one(
                    {
                        'task_id': task_id,
                        'status': '待处理'  # 乐观锁条件
                    },
                    {
                        '$set': {
                            'status': '处理中',
                            'started_at': now
                        }
                    }
                )
                
                # 检查是否有文档受到影响
                if result.modified_count > 0:
                    logging.debug(f"任务 {task_id} 已成功认领")
                    return True
                
                # 再次检查任务状态以确保结果准确性
                t = self.get_task(task_id)
                if t and t.get("status") == "处理中":
                    logging.debug(f"任务 {task_id} 已被其他线程认领")
                    return True
                
                logging.warning(f"任务 {task_id} 认领失败，可能已被其他线程处理")
                return False
                
            except Exception as e:
                logging.error(f"认领任务 {task_id} 失败: {str(e)}")
                return False

    def complete_task(self, task_id: str, success: bool) -> None:
        """
        线程安全的任务完成方法
        """
        with self._lock:
            if not hasattr(self, '_conn') or not self._conn:
                # 连接已关闭，重新初始化
                self._initialize()
                
        with self._get_task_lock(task_id):
            # 完成时同时写入结束时间
            self.update_task_status(task_id, "成功" if success else "失败")
            
            # 当任务完成时，可以考虑清理任务锁以避免内存泄漏
            # 但需要小心处理，确保没有其他线程正在使用该锁
            # 这里简化处理，保留任务锁