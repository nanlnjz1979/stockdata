from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.utils import timezone
import logging
import traceback

# 导入任务相关函数（需要根据实际情况调整导入路径）
from stocks.tasks.task_monitor import get_task_statistics, get_all_tasks, get_all_schedules, get_recent_tasks

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