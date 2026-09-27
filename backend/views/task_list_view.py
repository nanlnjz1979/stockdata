from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
import logging
import traceback
import json
from stocks.tasks.tasksOrm import QtasksOrm
logger = logging.getLogger(__name__)

class TaskListView(APIView):
    """
    任务列表视图，支持分页、类型和状态过滤
    """
    def get(self, request):
        try:
            # 获取查询参数
            task_type = request.GET.get('task_type', '').strip()
            status_filter = request.GET.get('status', '').strip()
            param_contains = request.GET.get('param_contains', '').strip()
            page = max(1, int(request.GET.get('page', 1)))
            page_size = min(500, max(1, int(request.GET.get('page_size', 50))))
            
            # 计算偏移量
            offset = (page - 1) * page_size
            
            # 实例化QtasksOrm
            orm = QtasksOrm()

            status_arg = status_filter if status_filter else None
            task_type_arg = task_type if task_type else None
            param_arg = param_contains if param_contains else None

            # 数据库侧计数和分页，避免 list_tasks 默认 10000 条上限影响 total 和分页。
            total = orm.count_tasks(
                status=status_arg,
                task_type=task_type_arg,
                param_contains=param_arg,
            )
            total_pages = max(1, (total + page_size - 1) // page_size)
            if page > total_pages:
                page = total_pages
                offset = (page - 1) * page_size

            items = orm.list_tasks(
                status=status_arg,
                task_type=task_type_arg,
                limit=page_size,
                offset=offset,
                param_contains=param_arg,
                sort_order=[('created_at', -1), ('priority', -1)],
            )
            
            # 转换task_params为JSON对象
            for item in items:
                task_params = item.get('task_params')
                if isinstance(task_params, str) and task_params.strip():
                    try:
                        item['task_params'] = json.loads(task_params)
                    except json.JSONDecodeError:
                        item['task_params'] = task_params
                else:
                    item['task_params'] = task_params if task_params else None
            
            # 获取可选的任务类型
            types = orm.list_task_types()
            # 获取任务总览（按类型+状态聚合）
            summary = orm.get_summary()
            
            # 计算分页信息
            has_prev = page > 1
            has_next = page < total_pages
            
            return Response({
                'success': True,
                'items': items,
                'total': total,
                'page': page,
                'page_size': page_size,
                'total_pages': total_pages,
                'has_prev': has_prev,
                'has_next': has_next,
                'options': {
                    'types': types
                },
                'summary': summary
            })
            
        except Exception as e:
            logger.error(f"获取任务列表失败: {str(e)}")
            logger.error(traceback.format_exc())
            return Response({
                'success': False,
                'error': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def post(self, request):
        try:
            # 实例化QtasksOrm
            orm = QtasksOrm()
            
            # 首先检查URL中是否包含delete_all参数，或者请求体中是否有action=delete_all
            # 这样可以处理多种请求格式
            is_delete_all = False
            
            # 检查request.query_params
            if request.query_params.get('action') == 'delete_all':
                is_delete_all = True
            
            # 检查request.data
            elif hasattr(request, 'data'):
                data = request.data
                if data:
                    if isinstance(data, dict) and data.get('action') == 'delete_all':
                        is_delete_all = True
                    elif isinstance(data, list):
                        # 旧格式：直接传递status列表
                        pass
            
            # 检查是否是删除所有任务请求
            if is_delete_all:
                # 直接全量删除，不受 list_tasks 默认 limit=10000 限制
                before_count = orm.count_tasks()
                deleted_count = orm.delete_all_tasks()
                after_count = orm.count_tasks()
                
                return Response({
                    'success': True,
                    'count': deleted_count,
                    'deleted_count': deleted_count,
                    'before_count': before_count,
                    'after_count': after_count
                })
            
            # 否则执行原有任务重试逻辑
            # 获取请求体中的状态列表
            status_list = []
            
            # 处理不同格式的请求体
            data = request.data
            if isinstance(data, dict):
                status_list = data.get('status', [])
            elif isinstance(data, list):
                # 直接传递status列表的旧格式
                status_list = data
            else:
                # 尝试从URL参数获取status
                status_param = request.GET.get('status', '')
                if status_param:
                    status_list = [status_param]
                else:
                    return Response({
                        'success': False,
                        'error': '请求体格式错误，需要包含status字段'
                    }, status=status.HTTP_400_BAD_REQUEST)
            
            if not isinstance(status_list, list):
                status_list = [status_list]
            
            # 获取所有符合条件的任务
            all_tasks = []
            for status in status_list:
                status_value = str(status).strip()
                if not status_value:
                    continue
                task_count = orm.count_tasks(status=status_value)
                if task_count <= 0:
                    continue
                tasks = orm.list_tasks(status=status_value, limit=task_count)
                all_tasks.extend(tasks)
            
            # 去重，确保每个任务只处理一次
            unique_tasks = {task['task_id']: task for task in all_tasks}.values()
            count = len(unique_tasks)
            
            # 更新每个任务的状态
            if count > 0:
                for task in unique_tasks:
                    try:
                        # 更新任务状态为"待处理"
                        orm.update_task_status(task['task_id'], '待处理')
                    except Exception as e:
                        logger.error(f"更新任务 {task['task_id']} 状态失败: {str(e)}")
                        continue
            
            return Response({
                'success': True,
                'count': count
            })
            
        except Exception as e:
            logger.error(f"任务操作失败: {str(e)}")
            logger.error(traceback.format_exc())
            return Response({
                'success': False,
                'error': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
