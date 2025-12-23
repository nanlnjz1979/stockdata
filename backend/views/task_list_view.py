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
            page = int(request.GET.get('page', 1))
            page_size = int(request.GET.get('page_size', 50))
            
            # 计算偏移量
            offset = (page - 1) * page_size
            
            # 实例化QtasksOrm
            orm = QtasksOrm()
            
            # 获取所有任务（这里先获取所有，然后在Python层面处理过滤和分页）
            all_tasks = orm.list_tasks(status=status_filter if status_filter else None, 
                                      task_type=task_type if task_type else None)
            
            # 处理param_contains过滤
            filtered_tasks = all_tasks
            if param_contains:
                filtered_tasks = []
                for task in all_tasks:
                    try:
                        # 检查task_params是否包含指定字符串
                        if isinstance(task.get('task_params'), str):
                            if param_contains in task['task_params']:
                                filtered_tasks.append(task)
                        elif isinstance(task.get('task_params'), dict):
                            # 如果已经是字典，转换为字符串再检查
                            if param_contains in json.dumps(task['task_params']):
                                filtered_tasks.append(task)
                    except Exception:
                        continue
            
            # 计算总数
            total = len(filtered_tasks)
            
            # 排序和分页
            # 按created_at降序排序
            filtered_tasks.sort(key=lambda x: x.get('created_at') or 0, reverse=True)
            
            # 分页处理
            items = filtered_tasks[offset:offset + page_size]
            
            # 转换task_params为JSON对象
            for item in items:
                if isinstance(item['task_params'], str) and item['task_params'].strip():
                    try:
                        item['task_params'] = json.loads(item['task_params'])
                    except json.JSONDecodeError:
                        item['task_params'] = item.get('task_params')
                else:
                    item['task_params'] = None
            
            # 获取可选的任务类型
            # 从所有任务中提取唯一的task_type
            all_types = set()
            for task in all_tasks:
                if task.get('task_type') and task['task_type'].strip():
                    all_types.add(task['task_type'])
            types = sorted(list(all_types))
            
            # 计算分页信息
            total_pages = max(1, (total + page_size - 1) // page_size)
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
                }
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
                # 删除所有任务
                # 首先获取所有任务
                all_tasks = orm.list_tasks()
                count = len(all_tasks)
                
                # 删除每个任务
                if count > 0:
                    for task in all_tasks:
                        try:
                            orm.delete_task(task['task_id'])
                        except Exception as e:
                            logger.error(f"删除任务 {task['task_id']} 失败: {str(e)}")
                            continue
                
                return Response({
                    'success': True,
                    'count': count
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
                tasks = orm.list_tasks(status=status.strip())
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