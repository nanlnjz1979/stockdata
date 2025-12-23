from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from pathlib import Path
from django.conf import settings

class ScheduleConfigView(APIView):
    """参数配置：读取与保存 MongoDB 中的 schedule_configs。"""
    def get(self, request):
        try:
            import sys
            project_root = Path(settings.BASE_DIR).parent
            if str(project_root) not in sys.path:
                sys.path.append(str(project_root))
            # 导入MongoDB连接函数
            from db import get_mongo_conn, put_mongo_conn
        except Exception as e:
            return Response({'error': f'导入连接模块失败: {str(e)}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        try:
            # 获取MongoDB连接
            conn = get_mongo_conn()
            # 获取数据库和集合
            db = conn['stockdata']
            schedule_configs_col = db['schedule_configs']
            # 从MongoDB读取所有配置
            rows = list(schedule_configs_col.find({}, {'_id': 1, 'name': 1, 'task_desc': 1, 'params': 1, 'schedule_time': 1, 'enabled': 1}))
            # 转换为前端需要的格式
            items = []
            for row in rows:
                item = {
                    'id': row.get('_id'),
                    'name': row.get('name'),
                    'task_desc': row.get('task_desc'),
                    'params': row.get('params'),
                    'schedule_time': row.get('schedule_time'),
                    'enabled': row.get('enabled')
                }
                items.append(item)
            return Response({'items': items})
        except Exception as e:
            return Response({'error': f'查询失败: {str(e)}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            if 'conn' in locals():
                try:
                    put_mongo_conn(conn)
                except Exception:
                    try:
                        conn.close()
                    except Exception:
                        pass

    def put(self, request):
        """覆盖保存所有配置项。支持传入 {items: [...]} 或 {configs: {id: {...}}}。"""
        payload = request.data or {}
        items = payload.get('items')
        configs_dict = payload.get('configs')
        try:
            import sys
            project_root = Path(settings.BASE_DIR).parent
            if str(project_root) not in sys.path:
                sys.path.append(str(project_root))
            # 导入MongoDB连接函数
            from db import get_mongo_conn, put_mongo_conn
            from global_config.config import GlobalConfig
        except Exception as e:
            return Response({'error': f'导入模块失败: {str(e)}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        # 统一为列表结构
        if items is None and isinstance(configs_dict, dict):
            items = []
            for _id, cfg in configs_dict.items():
                one = {'id': _id}
                one.update(cfg or {})
                items.append(one)
        if not isinstance(items, list):
            return Response({'error': '请求体需包含 items 列表或 configs 字典'}, status=status.HTTP_400_BAD_REQUEST)

        # 归一化并保存到MongoDB
        conn = None
        try:
            # 获取MongoDB连接
            conn = get_mongo_conn()
            # 获取数据库和集合
            db = conn['stockdata']
            schedule_configs_col = db['schedule_configs']
            # 先清空原集合
            schedule_configs_col.delete_many({})
            # 准备插入的数据
            insert_data = []
            for it in items:
                _id = str(it.get('id') or '').strip()
                if not _id:
                    continue
                name = it.get('name') or ''
                task_desc = it.get('task_desc') or ''
                params = it.get('params') or ''
                # 使用GlobalConfig的归一化方法处理时间和启用状态
                schedule_time = it.get('schedule_time')
                enabled = it.get('enabled')
                
                # 构建插入文档
                doc = {
                    '_id': _id,
                    'name': name,
                    'task_desc': task_desc,
                    'params': params,
                    'schedule_time': schedule_time,
                    'enabled': enabled
                }
                insert_data.append(doc)
            
            # 批量插入到MongoDB
            if insert_data:
                schedule_configs_col.insert_many(insert_data)
            
            return Response({'saved': True, 'count': len(items)})
        except Exception as e:
            return Response({'error': f'保存失败: {str(e)}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            if conn:
                try:
                    put_mongo_conn(conn)
                except Exception:
                    try:
                        conn.close()
                    except Exception:
                        pass


class ScheduleApplyView(APIView):
    """立即生效：触发 build_schedules_from_global_config 更新定时任务配置。"""
    def post(self, request):
        try:
            import sys
            project_root = Path(settings.BASE_DIR).parent
            if str(project_root) not in sys.path:
                sys.path.append(str(project_root))
            
            # 动态导入 scheduler 模块
            from stocks.tasks.scheduler import build_schedules_from_global_config
            
            # 执行配置更新
            count = build_schedules_from_global_config()
            
            return Response({
                'success': True,
                'message': f'定时任务配置已更新',
                'count': count
            })
        except Exception as e:
            return Response({
                'success': False,
                'error': f'更新配置失败: {str(e)}'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)