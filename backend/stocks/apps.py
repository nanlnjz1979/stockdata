from django.apps import AppConfig
# 移除异步线程，改为同步执行


class StocksConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'stocks'

    _bootstrap_done = False

    def ready(self):
        # 避免在开发模式下重复执行（Django autoreload 会多次调用 ready）
        if StocksConfig._bootstrap_done:
            return
        StocksConfig._bootstrap_done = True

        # 同步执行：服务启动时直接加载所有计划任务配置并注册django-q定时任务
        try:
            import sys
            from pathlib import Path
            from django.conf import settings
            project_root = Path(settings.BASE_DIR).parent
            if str(project_root) not in sys.path:
                sys.path.append(str(project_root))

            from db.db_pool import get_conn, put_conn
            from db import get_mongo_conn, put_mongo_conn
            from backend.global_config import GlobalConfig
            from stocks.tasks.scheduler import build_schedules_from_global_config
            # 使用MongoDB连接初始化GlobalConfig
            mongo_conn = get_mongo_conn()
            if mongo_conn:
                try:
                    _ = GlobalConfig(mongo_conn)  # 读取并缓存 schedule_configs（同时写回默认值）
                finally:
                    try:
                        put_mongo_conn(mongo_conn)
                    except Exception:
                        pass
                # 构建/更新django-q计划
                try:
                    n = build_schedules_from_global_config()
                    print(f"[StocksConfig.ready] django-q schedules built/updated: {n}")
                except Exception as e:
                    print(f"[StocksConfig.ready] build schedules error: {e}")
        except Exception:
            # 启动期不阻塞服务；如果失败可以在运行时兜底或手动脚本创建
            pass

        
        
