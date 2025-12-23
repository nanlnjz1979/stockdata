from django.conf import settings
from django.utils import timezone
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView
import threading
import time
import logging
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from db.db_pool import get_conn, put_conn

# 将项目根目录添加到系统路径
project_root = Path(settings.BASE_DIR).parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

# 配置日志
logger = logging.getLogger(__name__)

# 队列更新独立控制器
_queue_ctrl = {
    'thread_pool': None,
    'stop_event': threading.Event(),
    'state': {
        'running': False,
        'paused': False,
        'stopped': False,
        'updated_count': 0,
        'total_codes': 0,
        'current_codes': [],  # 改为列表，支持多线程显示
        'started_at': None,
        'ended_at': None,
    },
    'active_threads': 0,
    'thread_lock': threading.Lock()  # 用于线程安全操作
}

def get_queue_ctrl():
    """获取队列控制器（供外部使用）"""
    return _queue_ctrl


def _worker_thread():
    """多线程任务处理函数"""
    from stocks.tasks import DTBInstTradingTrackerTask,DownloadDailyTask, IncrementalUpdateTask,QtasksOrm
    
    # 每个线程独立获取连接
    conn = None
    thread_id = threading.get_ident()
    current_task_code = None
    
    try:
        # 从连接池获取连接
        conn = get_conn()
        orm = QtasksOrm.get_instance()
        
        while not _queue_ctrl['stop_event'].is_set():
            # 检查是否暂停
            while _queue_ctrl['state']['paused']:
                if _queue_ctrl['stop_event'].is_set():
                    break
                time.sleep(0.2)
            if _queue_ctrl['stop_event'].is_set():
                break
            
            # 取下一个待处理任务
            item = orm.next_pending_task()
            if not item:
                break
                
            tid = item.get('task_id')
            taskType = item.get('task_type')
            
            # 认领任务
            try:
                claimed = orm.claim_task(tid)#变成"处理中"
            except Exception:
                claimed = True

            if not claimed:
                # 未成功认领，稍后重试
                time.sleep(0.2)
                continue
                
            # 解析参数，获取代码信息
            try:
                import json
                params = json.loads(item.get('task_params') or '{}')
            except Exception:
                params = {}

            code = params.get('code')
            if not code and isinstance(params.get('codes'), list) and params.get('codes'):
                code = params.get('codes')[0]
            current_task_code = code or item.get('task_type')
            
            # 更新当前处理的代码列表
            with _queue_ctrl['thread_lock']:
                if current_task_code not in _queue_ctrl['state']['current_codes']:
                    _queue_ctrl['state']['current_codes'].append(current_task_code)
            
            # 根据类型构造任务
            task = None
            if taskType == DownloadDailyTask.taskID():
                task = DownloadDailyTask()
            elif taskType == DTBInstTradingTrackerTask.taskID():
                task = DTBInstTradingTrackerTask()
            elif taskType == IncrementalUpdateTask.taskID():
                task = IncrementalUpdateTask()
            else:
                # 更新任务状态为失败
                orm.update_task_status(tid, "失败")
                with _queue_ctrl['thread_lock']:
                    _queue_ctrl['state']['updated_count'] += 1
                    if current_task_code in _queue_ctrl['state']['current_codes']:
                        _queue_ctrl['state']['current_codes'].remove(current_task_code)
                continue
            
            # 设置任务属性
            task.task_id = item.get('task_id')
            task.task_type = item.get('task_type')
            task.task_desc = item.get('task_desc')
            task.params_str = item.get('task_params') or '{}'
            task.priority = item.get('priority') or 0
            
            # 执行任务
            ok = False
            try:
                ok = task.run(conn=conn)
            except Exception as e:
                logger.error(f"执行任务 {task.task_id} 失败: {str(e)}")
                ok = False
            
            # 更新任务状态
            try:
                orm.update_task_status(task.task_id, "成功" if ok else "失败")
                logger.info(f"任务 {task.task_id} 状态已更新为 {'成功' if ok else '失败'}")
            except Exception as e:
                logger.error(f"更新任务 {task.task_id} 状态失败: {str(e)}")
            
            # 更新状态统计
            with _queue_ctrl['thread_lock']:
                _queue_ctrl['state']['updated_count'] += 1
                if current_task_code in _queue_ctrl['state']['current_codes']:
                    _queue_ctrl['state']['current_codes'].remove(current_task_code)
            
            # 短暂休眠，避免CPU占用过高
            time.sleep(0.01)
    except Exception as e:
        logger.error(f"线程 {thread_id} 执行失败: {str(e)}")
    finally:
        # 清理资源
        if conn:
            try:
                put_conn(conn)
            except Exception as e:
                logger.error(f"归还连接到连接池失败: {str(e)}")
        
        # 移除当前代码
        if current_task_code:
            with _queue_ctrl['thread_lock']:
                if current_task_code in _queue_ctrl['state']['current_codes']:
                    _queue_ctrl['state']['current_codes'].remove(current_task_code)
                    
        # 更新活动线程数
        with _queue_ctrl['thread_lock']:
            _queue_ctrl['active_threads'] -= 1
            # 如果所有线程都完成，更新控制器状态
            if _queue_ctrl['active_threads'] == 0:
                _queue_ctrl['state']['running'] = False
                _queue_ctrl['state']['stopped'] = _queue_ctrl['stop_event'].is_set()
                _queue_ctrl['state']['ended_at'] = timezone.now()
                _queue_ctrl['thread_pool'] = None


def _start_queue_update_thread():
    """启动队列更新多线程处理"""
    with _queue_ctrl['thread_lock']:
        if _queue_ctrl['thread_pool'] or _queue_ctrl['state']['running']:
            return False
            
        _queue_ctrl['stop_event'].clear()
        _queue_ctrl['state'].update({
            'running': True,
            'paused': False,
            'stopped': False,
            'updated_count': 0,
            'total_codes': 0,
            'current_codes': [],
            'started_at': timezone.now(),
            'ended_at': None,
        })
        _queue_ctrl['active_threads'] = 0
    
    from stocks.tasks import QtasksOrm
    
    try:
        orm = QtasksOrm.get_instance()
        # 获取待处理任务总数
        pending = orm.list_tasks(status="待处理", limit=100000)
        total_tasks = len(pending)
        
        with _queue_ctrl['thread_lock']:
            _queue_ctrl['state']['total_codes'] = total_tasks
        
        # 创建线程池，根据任务数量动态调整线程数
        max_workers = min(10, total_tasks)  # 最多10个线程，或根据任务数量
        if max_workers < 2:
            max_workers = 2  # 至少2个线程
        
        # 创建线程池
        executor = ThreadPoolExecutor(max_workers=max_workers)
        
        with _queue_ctrl['thread_lock']:
            _queue_ctrl['thread_pool'] = executor
        
        # 提交任务
        for _ in range(max_workers):
            with _queue_ctrl['thread_lock']:
                _queue_ctrl['active_threads'] += 1
            executor.submit(_worker_thread)
        
        return True
    except Exception as e:
        logger.error(f"启动队列更新线程池失败: {str(e)}")
        with _queue_ctrl['thread_lock']:
            _queue_ctrl['state']['running'] = False
            _queue_ctrl['state']['ended_at'] = timezone.now()
        return False


class QueueUpdateStartView(APIView):
    """队列更新启动视图"""
    def post(self, request):

        started = _start_queue_update_thread()
        return Response({
            'started': started,
            'started_at': _queue_ctrl['state']['started_at'],
            'total_codes': _queue_ctrl['state']['total_codes'],
            'note': '后台执行 任务队列更新（多线程消费待处理任务，可暂停/继续/停止）'
        })