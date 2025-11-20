from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
import logging
import traceback
from db.db_pool import get_conn, put_conn
import json
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
            
            # 构建SQL查询
            where_conditions = []
            params = []
            
            if task_type:
                where_conditions.append("task_type = %s")
                params.append(task_type)
            
            if status_filter:
                where_conditions.append("status = %s")
                params.append(status_filter)
            
            if param_contains:
                where_conditions.append("params LIKE %s")
                params.append(f"%{param_contains}%")
            
            # 构建完整的WHERE子句
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            # 计算偏移量
            offset = (page - 1) * page_size
            
            # 执行查询获取总数
            conn = None
            cursor = None
            try:
                # 从连接池获取连接
                conn = get_conn()
                cursor = conn.cursor()
                
                # 获取总数
                cursor.execute(f"SELECT COUNT(*) FROM tasks {where_clause}", params)
                total = cursor.fetchone()[0]
                
                # 获取任务列表
                cursor.execute(
                    f"SELECT task_id, task_type, task_desc, task_params, status, priority, "
                    f"created_at, started_at, ended_at "
                    f"FROM tasks "
                    f"{where_clause} "
                    f"ORDER BY created_at DESC "
                    f"LIMIT %s , %s",
                    params + [page_size, offset]
                )
                
                # 获取列名
                columns = [col[0] for col in cursor.description]
                
                # 转换为字典列表
                items = []
                for row in cursor.fetchall():
                    item = dict(zip(columns, row))
                    # 转换params为对象（如果是JSON字符串）
                    if isinstance(item['task_params'], str) and item['task_params'].strip():
                        try:
                            
                            item['task_params'] = json.loads(item['task_params'])
                        except json.JSONDecodeError:
                            item['task_params'] = item['params']
                    else:
                        item['task_params'] = None
                    items.append(item)
                
                # 获取可选的任务类型
                cursor.execute("SELECT DISTINCT task_type FROM tasks WHERE task_type != '' ORDER BY task_type")
                types = [row[0] for row in cursor.fetchall()]
            finally:
                # 关闭游标
                if cursor:
                    cursor.close()
                # 归还连接到连接池
                if conn:
                    put_conn(conn)
            
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
            # 获取请求体中的状态列表
            data = request.data
            if not isinstance(data, dict) or 'status' not in data:
                return Response({
                    'success': False,
                    'error': '请求体格式错误，需要包含status字段'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            status_list = data['status']
            if not isinstance(status_list, list):
                return Response({
                    'success': False,
                    'error': 'status字段必须是数组格式'
                }, status=status.HTTP_400_BAD_REQUEST)
            
            # 构建SQL查询语句和更新语句
            placeholders = ','.join(['%s'] * len(status_list))
            select_sql = f"SELECT COUNT(*) FROM tasks WHERE status IN ({placeholders})"
            update_sql = f"""UPDATE tasks 
                    SET status = '待处理', 
                        started_at = NULL, 
                        ended_at = NULL 
                    WHERE status IN ({placeholders})"""
            
            conn = None
            cursor = None
            count = 0
            try:
                # 从连接池获取连接
                conn = get_conn()
                cursor = conn.cursor()
                
                # 先执行SELECT查询获取受影响的记录条数
                cursor.execute(select_sql, status_list)
                count = cursor.fetchone()[0] if cursor.rowcount > 0 else 0
                
                # 再执行UPDATE操作
                if count > 0:
                    cursor.execute(update_sql, status_list)
                    conn.commit()
            finally:
                # 关闭游标
                if cursor:
                    cursor.close()
                # 归还连接到连接池
                if conn:
                    put_conn(conn)
            
            return Response({
                'success': True,
                'count': count
            })
            
        except Exception as e:
            logger.error(f"任务重试失败: {str(e)}")
            logger.error(traceback.format_exc())
            return Response({
                'success': False,
                'error': str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)