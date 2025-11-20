from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.utils import timezone
import logging
import traceback
from django_q.models import Task, Schedule
from typing import List, Dict, Any
from datetime import datetime


def get_all_tasks(status: str = None, limit: int = 100) -> List[Dict[str, Any]]:
    """
    获取Django Q的所有任务，支持按状态过滤
    
    Args:
        status: 任务状态过滤，可选值：'success', 'failed', 'started', 'queued'
        limit: 返回的最大任务数量
    
    Returns:
        任务列表，每个任务包含详细信息
    """
    # 获取任务，按id倒序
    query = Task.objects.all().order_by('-id')
    
    # 状态过滤 - 基于存在的属性进行过滤
    if status:
        if status == 'queued':
            query = query.filter(started=None, stopped=None)
        elif status == 'started':
            query = query.filter(started__isnull=False, stopped=None)
        elif status == 'success':
            query = query.filter(success=True)
        elif status == 'failed':
            query = query.filter(success=False, stopped__isnull=False)
    
    # 限制返回数量
    query = query[:limit]
    
    # 转换为字典列表
    tasks = []
    for task in query:
        # 安全地处理可能为None的字段
        started_iso = task.started.isoformat() if task.started else None
        stopped_iso = task.stopped.isoformat() if task.stopped else None
        
        task_dict = {
            'id': task.id,
            'name': task.name or 'Unnamed Task',
            'func': task.func,
            'status': get_task_status_display(task),
            'started': started_iso,
            'stopped': stopped_iso,
            'priority': task.priority,
            'result':task.result,
        }
        tasks.append(task_dict)
    
    return tasks


def get_all_schedules() -> List[Dict[str, Any]]:
    """
    获取所有定时任务
    
    Returns:
        定时任务列表
    """
    schedules = []
    for sched in Schedule.objects.all():
        # 获取repeats的可读状态描述
        if sched.repeats == -1:
            repeats_status = "无限重复"
        elif sched.repeats == 0:
            repeats_status = "已禁用"
        else:
            repeats_status = f"剩余{sched.repeats}次"
        
        tasks = Task.objects.filter(name=sched.name).order_by('-stopped')
        # 确保所有值都可JSON序列化，并处理last_run可能是function的情况
        schedules.append({
            'id': sched.id,
            'name': str(sched.name) if sched.name else None,
            'func': str(sched.func) if sched.func else None,
            'args': str(sched.args) if sched.args else None,
            'kwargs': str(sched.kwargs) if sched.kwargs else None,
            'schedule_type': get_schedule_type_display(sched),
            'minutes': sched.minutes,
            'repeats': sched.repeats,  # 保留原始数值
            'repeats_status': repeats_status,  # 添加可读状态描述
            'next_run': sched.next_run.isoformat() if sched.next_run and not callable(sched.next_run) else None,
            'last_run': str(sched.last_run) if sched.last_run else None,  # 不直接调用isoformat，避免function对象问题
            'cluster': str(sched.cluster) if sched.cluster else None,
        })
    
    return schedules


def get_task_status_display(task: Task) -> str:
    """获取任务状态的可读显示名称"""
    # 基于任务的属性推断状态
    if task.started is None and task.stopped is None:
        return '排队中'
    elif task.started is not None and task.stopped is None:
        return '运行中'
    elif task.success:
        return '成功'
    else:
        return '失败'


def get_schedule_type_display(schedule: Schedule) -> str:
    """获取调度类型的可读显示名称"""
    # 使用正确的常量名称，移除不存在的QTYEARLY
    type_map = {
        Schedule.ONCE: '一次性',
        Schedule.HOURLY: '每小时',
        Schedule.DAILY: '每日',
        Schedule.WEEKLY: '每周',
        Schedule.MONTHLY: '每月',
        Schedule.YEARLY: '每年',
        Schedule.MINUTES: '每分钟'
    }
    return type_map.get(schedule.schedule_type, '未知')


def get_task_statistics():
    """
    获取任务统计信息
    
    Returns:
        包含各种统计数据的字典
    """
    # 获取任务统计
    total = Task.objects.count()
    queued = Task.objects.filter(started=None, stopped=None).count()
    started = Task.objects.filter(started__isnull=False, stopped=None).count()
    success = Task.objects.filter(success=True).count()
    failed = Task.objects.filter(success=False, stopped__isnull=False).count()
    
    # 获取调度统计
    schedules = Schedule.objects.count()
    
    return {
        'total': total,
        'queued': queued,
        'started': started,
        'success': success,
        'failed': failed,
        'schedules': schedules
    }


def get_recent_tasks(limit: int = 50) -> List[Dict[str, Any]]:
    """
    获取最近的任务
    
    Args:
        limit: 返回的最大任务数量
    
    Returns:
        任务列表
    """
    # 获取最近的任务，按id倒序
    tasks = Task.objects.order_by('-id')[:limit]
    
    result = []
    for task in tasks:
        # 安全地处理可能为None的字段
        started_iso = task.started.isoformat() if task.started else None
        stopped_iso = task.stopped.isoformat() if task.stopped else None
        
        # 创建任务信息字典，不包含不存在的priority属性
        task_info = {
            'id': task.id,
            'name': task.name or 'Unnamed Task',
            'func': task.func,
            'status': get_task_status_display(task),
            'started': started_iso,
            'stopped': stopped_iso,
            'result': str(task.result) if task.result else None
        }
        result.append(task_info)
    
    return result

class TaskMonitorView(APIView):
    """
    任务监控主视图，提供任务统计信息
    """
    def get(self, request):
        try:
            logger = logging.getLogger(__name__)
            
            # 获取任务统计信息
            logger.info("尝试获取任务统计信息")
            stats = get_task_statistics()
            
            return Response({
                'success': True,
                'statistics': stats,
                'timestamp': timezone.now()
            })
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.error(f"获取任务统计信息失败: {str(e)}")
            logger.error(traceback.format_exc())
            return Response({
                'success': False,
                'error': str(e),
                'trace': traceback.format_exc()[:200]  # 限制错误信息长度
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class QTaskListView(APIView):
    """
    任务列表视图，支持分页和状态过滤
    """
    def get(self, request):
        try:
            # 获取查询参数
            status_filter = request.GET.get('status', None)
            limit = int(request.GET.get('limit', 100))
            
            # 获取任务列表
            tasks = get_all_tasks(status=status_filter, limit=min(limit, 500))  # 限制最大返回数量
            
            return Response({
                'success': True,
                'tasks': tasks,
                'count': len(tasks)
            })
        except Exception as e:
            return Response({
                'success': False,
                'error': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class ScheduleListView(APIView):
    """
    调度任务列表视图
    """
    def get(self, request):
        try:
            logger = logging.getLogger(__name__)
            
            logger.info("尝试获取调度任务列表")
            # 获取调度任务列表
            schedules = get_all_schedules()
            
            return Response({
                'success': True,
                'schedules': schedules,
                'count': len(schedules)
            })
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.error(f"获取调度任务列表失败: {str(e)}")
            logger.error(traceback.format_exc())
            return Response({
                'success': False,
                'error': str(e),
                'trace': traceback.format_exc()[:200]
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class RecentTasksView(APIView):
    """
    最近任务视图
    """
    def get(self, request):
        try:
            logger = logging.getLogger(__name__)
            
            logger.info("尝试获取最近任务")
            # 获取查询参数
            limit = int(request.GET.get('limit', 50))
            
            # 获取最近任务
            tasks = get_recent_tasks(limit=min(limit, 200))  # 限制最大范围
            
            return Response({
                'success': True,
                'tasks': tasks,
                'count': len(tasks)
            })
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.error(f"获取最近任务失败: {str(e)}")
            logger.error(traceback.format_exc())
            return Response({
                'success': False,
                'error': str(e),
                'trace': traceback.format_exc()[:200]
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)