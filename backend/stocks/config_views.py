from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from pathlib import Path
from django.conf import settings

class ScheduleConfigView(APIView):
    """参数配置：读取与保存 QuestDB 中的 schedule_configs。"""
    def get(self, request):
        try:
            import sys
            project_root = Path(settings.BASE_DIR).parent
            if str(project_root) not in sys.path:
                sys.path.append(str(project_root))
            from data_pipeline.collector import qdb_connect
        except Exception as e:
            return Response({'error': f'导入连接模块失败: {str(e)}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        try:
            conn = qdb_connect()
            cur = conn.cursor()
            cur.execute("SELECT id, name, task_desc, params, schedule_time, enabled FROM schedule_configs ORDER BY id")
            rows = cur.fetchall() or []
            cols = [d[0] for d in cur.description] if cur.description else ['id','name','task_desc','params','schedule_time','enabled']
            items = [{cols[i]: r[i] for i in range(len(cols))} for r in rows]
            try:
                conn.close()
            except Exception:
                pass
            return Response({'items': items})
        except Exception as e:
            try:
                conn.close()
            except Exception:
                pass
            return Response({'error': f'查询失败: {str(e)}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

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
            from data_pipeline.collector import qdb_connect
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

        # 归一化并保存
        try:
            conn = qdb_connect()
            cur = conn.cursor()
            cur.execute("TRUNCATE TABLE schedule_configs")
            for it in items:
                _id = str(it.get('id') or '').strip()
                if not _id:
                    continue
                name = it.get('name') or ''
                task_desc = it.get('task_desc') or ''
                params = it.get('params') or ''
                schedule_time = GlobalConfig._normalize_schedule_time_for_db(it.get('schedule_time'))
                enabled = GlobalConfig._normalize_enabled_for_db(it.get('enabled'))
                sql = (
                    """
                    INSERT INTO schedule_configs
                        (id, name, task_desc, params, schedule_time, enabled)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """
                )
                cur.execute(sql, (_id, name, task_desc, params, schedule_time, enabled))
            conn.commit()
            try:
                conn.close()
            except Exception:
                pass
            return Response({'saved': True, 'count': len(items)})
        except Exception as e:
            try:
                conn.close()
            except Exception:
                pass
            return Response({'error': f'保存失败: {str(e)}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)