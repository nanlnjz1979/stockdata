from django.conf import settings
from django.utils import timezone
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView
import threading
import time
import logging
import sys
import os
import random
from collections import deque
from datetime import timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from db.db_pool import get_conn, put_conn

# 将项目根目录添加到系统路径
project_root = Path(settings.BASE_DIR).parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

# 配置日志
logger = logging.getLogger(__name__)

DEFAULT_QUEUE_MAX_WORKERS = 10
DEFAULT_REQUEST_DELAY_MIN = 0.0
DEFAULT_REQUEST_DELAY_MAX = 0.0
QUEUE_SPEED_WINDOW_SECONDS = 300


def _get_int_setting(name, default):
    value = getattr(settings, name, os.getenv(name, default))
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _get_float_setting(name, default):
    value = getattr(settings, name, os.getenv(name, default))
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _get_queue_max_workers(total_tasks):
    configured = _get_int_setting("QUEUE_UPDATE_MAX_WORKERS", DEFAULT_QUEUE_MAX_WORKERS)
    configured = max(1, configured)
    return max(1, min(configured, total_tasks))


def _get_request_delay_range():
    delay_min = _get_float_setting("QUEUE_UPDATE_DELAY_MIN", DEFAULT_REQUEST_DELAY_MIN)
    delay_max = _get_float_setting("QUEUE_UPDATE_DELAY_MAX", DEFAULT_REQUEST_DELAY_MAX)
    delay_min = max(0.0, delay_min)
    delay_max = max(0.0, delay_max)
    if delay_max < delay_min:
        delay_min, delay_max = delay_max, delay_min
    return delay_min, delay_max


def _wait_random_request_delay(task_type, code):
    delay_min, delay_max = _get_request_delay_range()
    if delay_max <= 0:
        return False

    delay = random.uniform(delay_min, delay_max)
    logger.debug("[队列限速] type=%s code=%s 请求前随机等待 %.2fs", task_type, code or "-", delay)
    return _queue_ctrl['stop_event'].wait(delay)

# 队列更新独立控制器
_queue_ctrl = {
    'thread_pool': None,
    'stop_event': threading.Event(),
    'state': {
        'running': False,
        'paused': False,
        'stopped': False,
        'updated_count': 0,
        'success_count': 0,
        'failed_count': 0,
        'failure_rate': 0.0,
        'total_codes': 0,
        'initial_pending_count': 0,
        'pending_count': 0,
        'processing_count': 0,
        'current_codes': [],  # 改为列表，支持多线程显示
        'started_at': None,
        'ended_at': None,
        'stopping': False,
        'message': '空闲',
    },
    'active_threads': 0,
    'started_monotonic': None,
    'completion_times': deque(maxlen=5000),
    'thread_lock': threading.Lock()  # 用于线程安全操作
}

def get_queue_ctrl():
    """获取队列控制器（供外部使用）"""
    return _queue_ctrl


def get_queue_performance(pending_count=None, processing_count=None):
    """根据最近一段时间的完成速度估算队列剩余时间。"""
    now_monotonic = time.monotonic()
    with _queue_ctrl['thread_lock']:
        state = _queue_ctrl['state']
        running = bool(state.get('running'))
        paused = bool(state.get('paused'))
        stopping = bool(state.get('stopping'))
        updated_count = int(state.get('updated_count') or 0)
        started_monotonic = _queue_ctrl.get('started_monotonic')
        completion_times = list(_queue_ctrl.get('completion_times') or [])
        state_pending = int(state.get('pending_count') or 0)
        state_processing = int(state.get('processing_count') or 0)

    pending = state_pending if pending_count is None else max(0, int(pending_count or 0))
    processing = state_processing if processing_count is None else max(0, int(processing_count or 0))
    remaining_count = pending + processing
    elapsed_seconds = max(0.0, now_monotonic - started_monotonic) if started_monotonic else 0.0

    window_start = max(
        started_monotonic or now_monotonic,
        now_monotonic - QUEUE_SPEED_WINDOW_SECONDS,
    )
    recent_completions = sum(1 for completed_at in completion_times if completed_at >= window_start)
    sample_seconds = max(0.0, now_monotonic - window_start)
    speed_per_second = recent_completions / sample_seconds if recent_completions and sample_seconds >= 1 else 0.0
    speed_per_minute = round(speed_per_second * 60, 2)

    eta_seconds = None
    estimated_finish_at = None
    if running and not paused and not stopping and remaining_count > 0 and speed_per_second > 0:
        eta_seconds = max(0, int(round(remaining_count / speed_per_second)))
        estimated_finish_at = timezone.now() + timedelta(seconds=eta_seconds)

    return {
        'speed_per_minute': speed_per_minute,
        'eta_seconds': eta_seconds,
        'estimated_finish_at': estimated_finish_at,
        'remaining_count': remaining_count,
        'elapsed_seconds': int(round(elapsed_seconds)),
        'speed_sample_count': recent_completions,
        'estimating': bool(running and not paused and not stopping and remaining_count > 0 and updated_count == 0),
    }


def _refresh_failure_rate_locked():
    processed = (_queue_ctrl['state'].get('success_count') or 0) + (_queue_ctrl['state'].get('failed_count') or 0)
    failed = _queue_ctrl['state'].get('failed_count') or 0
    _queue_ctrl['state']['failure_rate'] = round(failed / processed, 4) if processed else 0.0


def _shutdown_executor(executor):
    if not executor:
        return
    try:
        executor.shutdown(wait=False, cancel_futures=True)
    except TypeError:
        executor.shutdown(wait=False)
    except Exception as e:
        logger.warning("[队列自愈] 关闭旧线程池失败: %s", e)


def _reset_stale_queue_state(message):
    """重置只剩内存状态、数据库已无处理中任务的旧队列。"""
    with _queue_ctrl['thread_lock']:
        old_executor = _queue_ctrl.get('thread_pool')
        _queue_ctrl['stop_event'].set()
        _queue_ctrl['state'].update({
            'running': False,
            'paused': False,
            'stopped': True,
            'processing_count': 0,
            'current_codes': [],
            'ended_at': timezone.now(),
            'stopping': False,
            'message': message,
        })
        _queue_ctrl['thread_pool'] = None
        _queue_ctrl['active_threads'] = 0

    _shutdown_executor(old_executor)


def reconcile_queue_state(pending_count=None, processing_count=None):
    """修正队列内存状态和数据库任务状态不一致导致的假运行。"""
    from stocks.tasks import QtasksOrm

    with _queue_ctrl['thread_lock']:
        running = bool(_queue_ctrl['state'].get('running'))

    if processing_count is None:
        try:
            processing_count = QtasksOrm.get_instance().count_tasks(status="处理中")
        except Exception as e:
            logger.warning("[队列自愈] 检查处理中任务失败: %s", e)
            return False

    if pending_count is None:
        try:
            from stocks.tasks import QtasksOrm
            pending_count = QtasksOrm.get_instance().count_tasks(status="待处理")
        except Exception:
            pending_count = 0

    if not running:
        if processing_count > 0:
            recovered_count = QtasksOrm.get_instance().reset_processing_tasks_to_pending()
            if recovered_count > 0:
                logger.warning("[队列自愈] 队列未运行，但数据库残留处理中=%s，已回收到待处理", recovered_count)
                return True
        return False

    if processing_count <= 0 and pending_count > 0:
        logger.warning("[队列自愈] 控制器显示运行中，但数据库处理中=0、待处理=%s，已重置为可重新启动", pending_count)
        _reset_stale_queue_state('检测到旧队列状态已失效，可重新启动任务队列')
        return True

    return False


def _worker_thread():
    """多线程任务处理函数"""
    import logging
    logger = logging.getLogger(__name__)
    from stocks.tasks import DTBInstTradingTrackerTask,DownloadDailyTask, IncrementalUpdateTask,QtasksOrm
    
    # 每个线程独立获取连接
    conn = None
    orm = None
    thread_id = threading.get_ident()
    current_task_code = None
    current_task_started_at = None
    current_task_started_wall = None
    
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
            
            # 原子认领下一个待处理任务；返回时任务状态已是“处理中”。
            item = orm.next_pending_task()
            if not item:
                break
                
            tid = item.get('task_id')
            taskType = item.get('task_type')
            
            # 解析参数获取股票代码（用于日志）
            try:
                import json
                params = json.loads(item.get('task_params') or '{}')
            except Exception:
                params = {}
            log_code = params.get('code', 'N/A')

            current_task_started_at = time.perf_counter()
            current_task_started_wall = time.strftime("%Y-%m-%d %H:%M:%S")
            logger.debug("[队列任务] thread=%s task_id=%s type=%s code=%s 认领成功", thread_id, tid, taskType, log_code)
                
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
                _queue_ctrl['state']['processing_count'] = len(_queue_ctrl['state']['current_codes'])
            
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
                    _queue_ctrl['state']['failed_count'] += 1
                    _queue_ctrl['completion_times'].append(time.monotonic())
                    _refresh_failure_rate_locked()
                    if current_task_code in _queue_ctrl['state']['current_codes']:
                        _queue_ctrl['state']['current_codes'].remove(current_task_code)
                    _queue_ctrl['state']['processing_count'] = len(_queue_ctrl['state']['current_codes'])
                continue
            
            # 设置任务属性
            task.task_id = item.get('task_id')
            task.task_type = item.get('task_type')
            task.task_desc = item.get('task_desc')
            task.params_str = item.get('task_params') or '{}'
            task.priority = item.get('priority') or 0

            if _wait_random_request_delay(taskType, current_task_code):
                orm.update_task_status(tid, "待处理")
                with _queue_ctrl['thread_lock']:
                    if current_task_code in _queue_ctrl['state']['current_codes']:
                        _queue_ctrl['state']['current_codes'].remove(current_task_code)
                    _queue_ctrl['state']['processing_count'] = len(_queue_ctrl['state']['current_codes'])
                logger.info("[队列任务] type=%s code=%s 请求前收到结束信号，任务已回到待处理", taskType, current_task_code)
                break
            
            # 执行任务
            ok = False
            logger.debug(
                "[队列任务开始] time=%s thread=%s task_id=%s type=%s code=%s",
                current_task_started_wall,
                thread_id,
                task.task_id,
                task.task_type,
                current_task_code,
            )
            try:
                ok = task.run(conn=conn)
            except Exception as e:
                logger.error("[队列任务] thread=%s task_id=%s type=%s code=%s 执行失败: %s", thread_id, task.task_id, task.task_type, log_code, str(e))
                ok = False
            
            # 更新任务状态
            try:
                orm.update_task_status(task.task_id, "成功" if ok else "失败")
                elapsed = (time.perf_counter() - current_task_started_at) if current_task_started_at else 0
                task_ended_wall = time.strftime("%Y-%m-%d %H:%M:%S")
                logger.debug(
                    "[队列任务结束] start=%s end=%s elapsed=%.2fs thread=%s task_id=%s type=%s code=%s status=%s",
                    current_task_started_wall or "-",
                    task_ended_wall,
                    elapsed,
                    thread_id,
                    task.task_id,
                    task.task_type,
                    current_task_code,
                    "成功" if ok else "失败",
                )
            except Exception as e:
                logger.error("[队列任务] thread=%s task_id=%s 更新状态失败: %s", thread_id, task.task_id, str(e))
            finally:
                current_task_started_at = None
                current_task_started_wall = None
            
            # 更新状态统计
            with _queue_ctrl['thread_lock']:
                _queue_ctrl['state']['updated_count'] += 1
                _queue_ctrl['completion_times'].append(time.monotonic())
                if ok:
                    _queue_ctrl['state']['success_count'] += 1
                else:
                    _queue_ctrl['state']['failed_count'] += 1
                _refresh_failure_rate_locked()
                if current_task_code in _queue_ctrl['state']['current_codes']:
                    _queue_ctrl['state']['current_codes'].remove(current_task_code)
                _queue_ctrl['state']['processing_count'] = len(_queue_ctrl['state']['current_codes'])
            
            # 短暂休眠，避免CPU占用过高
            time.sleep(0.01)
    except Exception as e:
        logger.error("[队列任务] thread=%s 工作线程执行失败: %s", thread_id, str(e))
    finally:
        # 清理资源
        if conn:
            try:
                put_conn(conn)
            except Exception as e:
                logger.error("[队列任务] thread=%s 归还连接到连接池失败: %s", thread_id, str(e))
        
        # 移除当前代码
        if current_task_code:
            with _queue_ctrl['thread_lock']:
                if current_task_code in _queue_ctrl['state']['current_codes']:
                    _queue_ctrl['state']['current_codes'].remove(current_task_code)
                _queue_ctrl['state']['processing_count'] = len(_queue_ctrl['state']['current_codes'])
                    
        # 更新活动线程数
        with _queue_ctrl['thread_lock']:
            _queue_ctrl['active_threads'] = max(0, _queue_ctrl['active_threads'] - 1)
            # 如果所有线程都完成，更新控制器状态
            if _queue_ctrl['active_threads'] == 0:
                _queue_ctrl['state']['running'] = False
                _queue_ctrl['state']['stopped'] = _queue_ctrl['stop_event'].is_set()
                _queue_ctrl['state']['stopping'] = False
                _queue_ctrl['state']['ended_at'] = timezone.now()
                _queue_ctrl['state']['processing_count'] = 0
                pending_count = _queue_ctrl['state'].get('pending_count') or 0
                try:
                    if orm:
                        pending_count = orm.count_tasks(status="待处理")
                        _queue_ctrl['state']['pending_count'] = pending_count
                except Exception as e:
                    logger.warning("[队列结束] 获取待处理任务数失败: %s", e)

                stopped_with_pending = _queue_ctrl['state']['stopped'] and pending_count > 0
                _queue_ctrl['state']['message'] = (
                    f'队列已停止，剩余 {pending_count} 个待处理，点击启动可继续'
                    if stopped_with_pending
                    else '队列执行完成'
                )
                _queue_ctrl['thread_pool'] = None
                started_at = _queue_ctrl['state'].get('started_at')
                ended_at = _queue_ctrl['state'].get('ended_at')
                elapsed = (ended_at - started_at).total_seconds() if started_at and ended_at else 0
                logger.info(
                    "%s total=%d processed=%d success=%d failed=%d pending=%d elapsed=%.2fs",
                    "[队列停止]" if stopped_with_pending else "[队列完成]",
                    _queue_ctrl['state'].get('total_codes') or 0,
                    _queue_ctrl['state'].get('updated_count') or 0,
                    _queue_ctrl['state'].get('success_count') or 0,
                    _queue_ctrl['state'].get('failed_count') or 0,
                    pending_count,
                    elapsed,
                )


def _start_queue_update_thread():
    """启动队列更新多线程处理"""
    import logging
    logger = logging.getLogger(__name__)

    from stocks.tasks import QtasksOrm

    orm = QtasksOrm.get_instance()

    with _queue_ctrl['thread_lock']:
        if _queue_ctrl['thread_pool'] or _queue_ctrl['state']['running']:
            if _queue_ctrl['state'].get('stopping'):
                return {
                    'started': False,
                    'already_running': True,
                    'stopping': True,
                    'message': '任务队列正在结束，请稍候',
                }
            try:
                db_processing_count = orm.count_tasks(status="处理中")
            except Exception as e:
                logger.warning("[队列启动] 检查数据库处理中任务失败: %s", e)
                db_processing_count = len(_queue_ctrl['state'].get('current_codes') or [])

            if db_processing_count <= 0:
                logger.warning("[队列自愈] 控制器显示运行中，但数据库无处理中任务，重置后重新启动")
                needs_stale_reset = True
            else:
                needs_stale_reset = False

            if needs_stale_reset:
                pass
            else:
                logger.info("[队列启动] 队列已在运行中，跳过启动")
                return {
                    'started': False,
                    'already_running': True,
                    'message': f'任务队列已在运行中，当前处理中 {db_processing_count} 个',
                    'processing_count': db_processing_count,
                }

    if _queue_ctrl['thread_pool'] or _queue_ctrl['state']['running']:
        _reset_stale_queue_state('检测到旧队列状态已失效，已自动重置')

    if reconcile_queue_state():
        logger.info("[队列启动] 已回收孤儿处理中任务，准备重新消费")

    with _queue_ctrl['thread_lock']:
        if _queue_ctrl['thread_pool'] or _queue_ctrl['state']['running']:
            logger.info("[队列启动] 队列已在运行中，跳过启动")
            return {
                'started': False,
                'already_running': True,
                'message': '任务队列已在运行中',
            }
            
        _queue_ctrl['stop_event'].clear()
        _queue_ctrl['state'].update({
            'running': True,
            'paused': False,
            'stopped': False,
            'stopping': False,
            'updated_count': 0,
            'success_count': 0,
            'failed_count': 0,
            'failure_rate': 0.0,
            'total_codes': 0,
            'initial_pending_count': 0,
            'pending_count': 0,
            'processing_count': 0,
            'current_codes': [],
            'started_at': timezone.now(),
            'ended_at': None,
            'message': '队列启动中',
        })
        _queue_ctrl['active_threads'] = 0
        _queue_ctrl['started_monotonic'] = time.monotonic()
        _queue_ctrl['completion_times'].clear()
    
    try:
        # 获取待处理任务总数
        total_tasks = orm.count_tasks(status="待处理")

        if total_tasks <= 0:
            now = timezone.now()
            with _queue_ctrl['thread_lock']:
                _queue_ctrl['state'].update({
                    'running': False,
                    'paused': False,
                    'stopped': False,
                    'updated_count': 0,
                    'success_count': 0,
                    'failed_count': 0,
                    'failure_rate': 0.0,
                    'total_codes': 0,
                    'initial_pending_count': 0,
                    'pending_count': 0,
                    'processing_count': 0,
                    'current_codes': [],
                    'started_at': None,
                    'ended_at': now,
                    'stopping': False,
                    'message': '没有待处理任务',
                })
                _queue_ctrl['thread_pool'] = None
                _queue_ctrl['active_threads'] = 0
                _queue_ctrl['started_monotonic'] = None
                _queue_ctrl['completion_times'].clear()
            logger.info("[队列启动] 没有待处理任务，未启动")
            return {
                'started': False,
                'no_pending': True,
                'message': '没有待处理任务',
                'total_codes': 0,
                'pending_count': 0,
                'processing_count': 0,
            }
        
        # 创建线程池，根据配置动态调整线程数；默认 10，数据源在每只股票下载时随机错开。
        max_workers = _get_queue_max_workers(total_tasks)
        
        # 创建线程池
        executor = ThreadPoolExecutor(max_workers=max_workers)
        
        with _queue_ctrl['thread_lock']:
            _queue_ctrl['state'].update({
                'total_codes': total_tasks,
                'initial_pending_count': total_tasks,
                'pending_count': total_tasks,
                'processing_count': 0,
                'message': '队列运行中',
            })
            _queue_ctrl['thread_pool'] = executor
        
        # 提交任务
        for _ in range(max_workers):
            with _queue_ctrl['thread_lock']:
                _queue_ctrl['active_threads'] += 1
            executor.submit(_worker_thread)
        
        delay_min, delay_max = _get_request_delay_range()
        logger.info(
            "[队列启动] 待处理任务=%d 工作线程=%d 请求随机延迟=%.1f~%.1fs",
            total_tasks,
            max_workers,
            delay_min,
            delay_max,
        )
        return {
            'started': True,
            'message': '任务队列已启动',
            'total_codes': total_tasks,
            'pending_count': total_tasks,
            'processing_count': 0,
            'max_workers': max_workers,
            'request_delay_min': delay_min,
            'request_delay_max': delay_max,
        }
    except Exception as e:
        logger.error(f"启动队列更新线程池失败: {str(e)}")
        with _queue_ctrl['thread_lock']:
            _queue_ctrl['state']['running'] = False
            _queue_ctrl['state']['ended_at'] = timezone.now()
            _queue_ctrl['state']['message'] = f"启动失败: {str(e)}"
            _queue_ctrl['thread_pool'] = None
            _queue_ctrl['active_threads'] = 0
        return {
            'started': False,
            'message': f"启动失败: {str(e)}",
        }


def request_queue_stop():
    """请求优雅结束队列，不强杀正在执行的下载线程。"""
    with _queue_ctrl['thread_lock']:
        if not _queue_ctrl['state']['running']:
            return {
                'stopped': False,
                'already_stopped': True,
                'message': '任务队列当前未运行',
            }

        _queue_ctrl['stop_event'].set()
        _queue_ctrl['state']['paused'] = False
        _queue_ctrl['state']['stopping'] = True
        _queue_ctrl['state']['message'] = '正在结束队列'
        return {
            'stopped': True,
            'message': '已请求结束队列，当前任务完成后停止',
            'processing_count': _queue_ctrl['state'].get('processing_count') or 0,
        }


class QueueUpdateStartView(APIView):
    """队列更新启动视图"""
    def post(self, request):

        result = _start_queue_update_thread()
        started = bool(result.get('started')) if isinstance(result, dict) else bool(result)
        return Response({
            'started': started,
            'already_running': bool(result.get('already_running')) if isinstance(result, dict) else False,
            'no_pending': bool(result.get('no_pending')) if isinstance(result, dict) else False,
            'message': result.get('message') if isinstance(result, dict) else '',
            'started_at': _queue_ctrl['state']['started_at'],
            'total_codes': _queue_ctrl['state']['total_codes'],
            'pending_count': result.get('pending_count', _queue_ctrl['state'].get('pending_count') or 0) if isinstance(result, dict) else (_queue_ctrl['state'].get('pending_count') or 0),
            'processing_count': result.get('processing_count', _queue_ctrl['state'].get('processing_count') or 0) if isinstance(result, dict) else (_queue_ctrl['state'].get('processing_count') or 0),
            'max_workers': result.get('max_workers') if isinstance(result, dict) else None,
            'request_delay_min': result.get('request_delay_min') if isinstance(result, dict) else None,
            'request_delay_max': result.get('request_delay_max') if isinstance(result, dict) else None,
            'note': '后台执行 任务队列更新（多线程消费待处理任务，可暂停/继续/停止）'
        })
