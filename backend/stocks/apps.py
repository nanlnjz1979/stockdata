from django.apps import AppConfig
# 移除异步线程，改为同步执行


class StocksConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'stocks'

    _qdb_bootstrap_done = False

    def ready(self):
        # 避免在开发模式下重复执行（Django autoreload 会多次调用 ready）
        if StocksConfig._qdb_bootstrap_done:
            return
        StocksConfig._qdb_bootstrap_done = True

        # 同步执行：服务启动时直接检查并创建 QuestDB 表
        try:
            import sys
            from pathlib import Path
            from django.conf import settings
            project_root = Path(settings.BASE_DIR).parent
            if str(project_root) not in sys.path:
                sys.path.append(str(project_root))

            from data_pipeline.collector import qdb_connect, qdb_ensure_tables
            conn = qdb_connect()
            if conn:
                try:
                    qdb_ensure_tables(conn)
                finally:
                    try:
                        conn.close()
                    except Exception:
                        pass
        except Exception:
            # 启动期不阻塞服务；如果失败可以在运行时兜底或手动脚本创建
            pass
         # 同步执行：服务启动时直接加载所有计划任务配置
        try:
            from global_config import GlobalConfig
            conn = qdb_connect()
            if conn:
                
                try:
                    _ = GlobalConfig(conn)  # 读取并缓存 schedule_configs
                finally:
                    try:
                        conn.close()
                    except Exception:
                        pass
        except Exception:
            # 启动期不阻塞服务；如果失败可以在运行时兜底或手动脚本创建
            pass