from rest_framework import viewsets, status
from rest_framework.views import APIView
from rest_framework.decorators import action
from rest_framework.response import Response
from django.shortcuts import get_object_or_404
from django.conf import settings
from django.utils import timezone
from stocks.tasks import get_all_tasks, get_all_schedules, get_task_statistics, get_recent_tasks
from rest_framework import status
import os
from pathlib import Path

from stocks.models import StockBasic, StockFinance
from stocks.serializers import StockBasicSerializer, StockFinanceSerializer


class StockBasicViewSet(viewsets.ModelViewSet):
    queryset = StockBasic.objects.all()
    serializer_class = StockBasicSerializer

    @action(detail=False, methods=['get'])
    def search(self, request):
        q = request.query_params.get('q', '')
        qs = StockBasic.objects.filter(stock_name__icontains=q) | StockBasic.objects.filter(stock_code__icontains=q)
        return Response(StockBasicSerializer(qs[:50], many=True).data)


class StockFinanceViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = StockFinance.objects.all()
    serializer_class = StockFinanceSerializer

    @action(detail=False, methods=['get'])
    def by_code(self, request):
        code = request.query_params.get('code')
        if not code:
            return Response({'detail': 'code required'}, status=status.HTTP_400_BAD_REQUEST)
        qs = StockFinance.objects.filter(stock__stock_code=code).order_by('-report_date')
        return Response(StockFinanceSerializer(qs, many=True).data)


# 占位：按时间范围查询历史K线（后续接入TimescaleDB）
class TaskListView(APIView):
    def get(self, request):
        import os
        try:
            import psycopg2
            from psycopg2 import OperationalError
            # 导入连接池
            from db.db_pool import get_conn, put_conn
        except Exception as e:
            return Response({'error': f'缺少依赖: {str(e)}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        host = os.getenv('QDB_HOST', 'localhost')
        port = int(os.getenv('QDB_PORT', '8812'))
        user = os.getenv('QDB_USER', 'admin')
        password = os.getenv('QDB_PASS', 'quest')
        dbname = os.getenv('QDB_DB', 'qdb')

        type_filter = request.query_params.get('task_type') or request.query_params.get('type')
        status_filter = request.query_params.get('status')
        param_contains = request.query_params.get('param_contains') or request.query_params.get('q')

        # 分页参数，默认第1页、每页50条；允许使用limit作为page_size别名
        try:
            page = int(request.query_params.get('page') or 1)
        except Exception:
            page = 1
        try:
            page_size = int(request.query_params.get('page_size') or request.query_params.get('limit') or 50)
        except Exception:
            page_size = 50
        if page < 1:
            page = 1
        if page_size < 1:
            page_size = 50
        page_size = min(page_size, 200)

        # 尝试使用连接池获取连接
        try:
            conn = get_conn()
        except OperationalError as e:
            return Response({'error': f'QuestDB连接失败: {str(e)}'}, status=status.HTTP_503_SERVICE_UNAVAILABLE)
        except Exception as e:
            return Response({'error': f'连接异常: {str(e)}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        # 构造查询条件
        where = []
        params = []
        if status_filter:
            where.append("status=%s")
            params.append(status_filter)
        if type_filter:
            where.append("task_type=%s")
            params.append(type_filter)
        if param_contains:
            where.append("lower(task_params) LIKE %s")
            params.append('%' + param_contains.lower() + '%')
        where_sql = (" WHERE " + " AND ".join(where)) if where else ""

        cur = conn.cursor()
        # 统计总数
        total = 0
        try:
            cur.execute(f"SELECT count(*) FROM tasks{where_sql}", params)
            total = int((cur.fetchone() or [0])[0] or 0)
        except Exception:
            total = 0

        # QuestDB不支持OFFSET，这里通过拉取前 page*page_size 条再在Python层切片实现分页
        fetch_limit = min(page * page_size, max(page_size, 1000))
        if total > 0:
            fetch_limit = min(fetch_limit, total)

        rows = []
        cols = ['task_id','task_type','task_desc','task_params','priority','status','created_at','started_at','ended_at']
        try:
            cur.execute(
                f"SELECT task_id, task_type, task_desc, task_params, priority, status, created_at, started_at, ended_at FROM tasks{where_sql} ORDER BY priority DESC LIMIT %s",
                params + [fetch_limit]
            )
            rows = cur.fetchall() or []
            if cur.description:
                cols = [d[0] for d in cur.description]
        except Exception:
            rows = []

        items_all = [{cols[i]: r[i] for i in range(len(cols))} for r in rows]
        start = (page - 1) * page_size
        end = start + page_size
        items = items_all[start:end]

        # 选项派生
        types = sorted(list({(it.get('task_type') or '') for it in items_all if it.get('task_type')}))
        # 固定状态列表，确保前端始终可选
        statuses = ['待处理','处理中','成功','失败','重试中','已取消']

        try:
            # 使用连接池归还连接
            put_conn(conn)
        except Exception:
            try:
                # 备选方案：直接关闭连接
                conn.close()
            except Exception:
                pass

        total_pages = (total + page_size - 1) // page_size if page_size else 1
        return Response({
            'items': items,
            'total': total,
            'page': page,
            'page_size': page_size,
            'total_pages': total_pages,
            'has_prev': page > 1,
            'has_next': page < total_pages,
            'options': {'types': types, 'statuses': statuses}
        })

import threading
import time

# 导入队列更新相关功能
from stocks.tasks.QueueUpdateTask import get_queue_ctrl

_update_ctrl = {
    'thread': None,
    'stop_event': threading.Event(),
    'state': {
        'running': False,
        'paused': False,
        'stopped': False,
        'updated_count': 0,
        'total_codes': 0,
        'current_code': None,
        'started_at': None,
        'ended_at': None,
    }
}

def _start_full_update_thread():
    if _update_ctrl['thread'] and _update_ctrl['state']['running']:
        return False
    _update_ctrl['stop_event'].clear()
    _update_ctrl['state'].update({
        'running': True,
        'paused': False,
        'stopped': False,
        'updated_count': 0,
        'total_codes': 0,
        'current_code': None,
        'started_at': timezone.now(),
        'ended_at': None,
    })
    from stocks.tasks import DownloadDailyTask, QtasksOrm
    def worker():
        try:
            import sys
            project_root = Path(settings.BASE_DIR).parent
            if str(project_root) not in sys.path:
                sys.path.append(str(project_root))
            from db.db_pool import get_conn, put_conn
            from data_pipeline.collector import populate_stock_basic_if_empty, qdb_get_all_basic, sync_basic_to_django
            conn = get_conn()
            
            if not conn:
                _update_ctrl['state']['error'] = 'QuestDB连接失败'
                raise RuntimeError('QuestDB连接失败')

            
            populate_stock_basic_if_empty(conn=conn)
            sync_basic_to_django(conn=conn)
            basics = qdb_get_all_basic(conn=conn)   #基础股票代码库
            _update_ctrl['state']['total_codes'] = len(basics)
            
            orm = QtasksOrm(conn)
            task = DownloadDailyTask(orm)
            for item in basics:     #基础库中的一条记录
                code = item.get('code')     #股票代码
                if not code:
                    continue
                market = item.get('market') #市场上海，深圳，北京
                listing_date = item.get('listing_date')#上市时间
                if not listing_date:
                    start_date = '19841118'             #如果上市时间没有，则默认是19841118，中国最早股票上市日期
                else:
                    try:
                        if hasattr(listing_date, 'strftime'):
                            start_date = listing_date.strftime("%Y%m%d")
                        else:
                            s = str(listing_date)
                            start_date = s.replace('-', '')
                    except Exception:
                        start_date = '19841118'
                
                task.generate("download_daily", f"Download daily data for {code}", {"code": code, "start_date": start_date, "end_date": timezone.now().strftime("%Y%m%d"), "market": market ,"adjust": "all"}, priority=0)
            dl_daily = orm.list_tasks(status="待处理", task_type="download_daily", limit=100000)
            for item in dl_daily:
                if _update_ctrl['stop_event'].is_set():
                    break
                while _update_ctrl['state']['paused']:
                    if _update_ctrl['stop_event'].is_set():
                        break
                    time.sleep(0.2)
                if _update_ctrl['stop_event'].is_set():
                    break
                try:
                    import json
                    params = json.loads(item.get('task_params') or '{}')
                except Exception:
                    params = {}
                code = params.get('code')
                _update_ctrl['state']['current_code'] = code
                t = DownloadDailyTask(orm)
                t.task_id = item.get('task_id')
                t.task_type = item.get('task_type')
                t.task_desc = item.get('task_desc')
                t.params_str = item.get('task_params') or '{}'
                t.priority = item.get('priority') or 0
                try:
                    orm.update_task_status(t.task_id, "处理中")
                except Exception:
                    pass
                try:
                    ok = t.run(conn=conn)
                except Exception:
                    ok = False
                try:
                    orm.update_task_status(t.task_id, "成功" if ok else "失败")
                except Exception:
                    pass
                _update_ctrl['state']['updated_count'] += 1
                time.sleep(0.01)
            try:
                if conn:
                    put_conn(conn)
            except Exception:
                pass
        finally:
            _update_ctrl['state']['running'] = False
            _update_ctrl['state']['stopped'] = _update_ctrl['stop_event'].is_set()
            _update_ctrl['state']['ended_at'] = timezone.now()
            _update_ctrl['thread'] = None
    t = threading.Thread(target=worker, daemon=True)
    _update_ctrl['thread'] = t
    t.start()
    return True

# 队列更新：从任务列表中取任务并执行



class UpdateRunView(APIView):

    
    def post(self, request):
        """触发一次数据更新（占位）：在后台线程执行采集脚本的run_once。"""
        import threading
        import time
        import sys
        from datetime import datetime

        def task():
            try:
                project_root = Path(settings.BASE_DIR).parent
                if str(project_root) not in sys.path:
                    sys.path.append(str(project_root))
                from data_pipeline.collector import run_once
            except Exception:
                pass

        threading.Thread(target=task, daemon=True).start()
        return Response({
            'started': True,
            'started_at': timezone.now(),
            'note': '后台执行 data_pipeline.collector.run_once(TEST)（占位）'
        })


# 队列更新 API：从任务列表取任务执行
# 从QueueUpdateTask导入队列更新视图
from stocks.tasks.QueueUpdateTask import QueueUpdateStartView

class UpdateStatusView(APIView):
    # 静态数据库连接
    _static_conn = None
    
    def get(self, request):
        """返回数据更新相关状态（QuestDB），不依赖SQLite。"""
        import os
        import psycopg2
        from psycopg2 import OperationalError
        from datetime import datetime

        # QuestDB 连接参数
        host = os.getenv('QDB_HOST', 'localhost')
        port = int(os.getenv('QDB_PORT', '8812'))
        user = os.getenv('QDB_USER', 'admin')
        password = os.getenv('QDB_PASS', 'quest')
        dbname = os.getenv('QDB_DB', 'qdb')

        qdb_ok = False
        qdb_error = None
        stock_basic_count = 0
        finance_count = 0
        latest_finance_date = None
        total_codes = 0
        updated_count = 0
        recent_updates = []
        conn = None

        try:
            # 初始化或获取静态连接
            if UpdateStatusView._static_conn is None or UpdateStatusView._static_conn.closed:
                UpdateStatusView._static_conn = psycopg2.connect(
                    host=host, 
                    port=port, 
                    user=user, 
                    password=password, 
                    dbname=dbname, 
                    connect_timeout=2
                )
                UpdateStatusView._static_conn.autocommit = True
            
            conn = UpdateStatusView._static_conn
            cur = conn.cursor()
            qdb_ok = True
            # 统计基础股票数量
            try:
                cur.execute('select count(*) from stock_basic')
                r = cur.fetchone()
                stock_basic_count = int((r and r[0]) or 0)
                total_codes = stock_basic_count
            except Exception:
                pass
            # 已更新股票数量（按日线存在的distinct代码数）
            try:
                cur.execute('select count(distinct code) from stock_daily')
                r = cur.fetchone()
                updated_count = int((r and r[0]) or 0)
            except Exception:
                pass
            # 最近更新（按最大交易日倒序，取前8）
            try:
                cur.execute("""
                    select d.code, b.name, max(d.trade_date) as last_date
                    from stock_daily d
                    left join stock_basic b on d.code=b.code
                    group by d.code, b.name
                    order by last_date desc
                    limit 8
                """)
                rows = cur.fetchall() or []
                recent_updates = [
                    {
                        'code': r[0],
                        'name': r[1],
                        'last_updated_date': r[2],
                        'last_run_time': None,
                    } for r in rows
                ]
            except Exception:
                pass
        except OperationalError as e:
            qdb_error = str(e)
        except Exception as e:
            qdb_error = str(e)

        # 控制器状态（正在运行时优先使用内存中的计数和进度）
        ctrl = _update_ctrl['state'].copy()
        if ctrl.get('running'):
            total_codes = ctrl.get('total_codes') or total_codes
            updated_count = ctrl.get('updated_count') or updated_count

        # 队列控制器状态
        queue_ctrl = get_queue_ctrl()['state'].copy()

        return Response({
            'stock_basic_count': stock_basic_count,
            'finance_count': finance_count,
            'latest_finance_date': latest_finance_date,
            'total_codes': total_codes,
            'updated_count': updated_count,
            'recent_updates': recent_updates,
            'controller': ctrl,
            'queue_controller': queue_ctrl,
            'questdb': {
                'host': host,
                'port': port,
                'user': user,
                'dbname': dbname,
                'connected': qdb_ok,
                'error': qdb_error,
            }
        })

class UpdateFullView(APIView):
    def post(self, request):
        """触发全量更新：可暂停/继续/停止。若QuestDB连接失败，返回错误。"""
        # 在启动线程前快速检查QuestDB连接，失败则直接返回错误
        

        started = _start_full_update_thread()
        return Response({
            'started': started,
            'started_at': _update_ctrl['state']['started_at'],
            'total_codes': _update_ctrl['state']['total_codes'],
            'note': '后台执行 akshare 全量更新（日线，可暂停/继续/停止）'
        })

class UpdatePauseView(APIView):
    def post(self, request):
        """暂停当前全量更新。"""
        if _update_ctrl['state']['running'] and not _update_ctrl['state']['paused']:
            _update_ctrl['state']['paused'] = True
        return Response({'running': _update_ctrl['state']['running'], 'paused': _update_ctrl['state']['paused']})

class UpdateResumeView(APIView):
    def post(self, request):
        """继续当前全量更新。"""
        if _update_ctrl['state']['running'] and _update_ctrl['state']['paused']:
            _update_ctrl['state']['paused'] = False
        return Response({'running': _update_ctrl['state']['running'], 'paused': _update_ctrl['state']['paused']})

class UpdateStopView(APIView):
    def post(self, request):
        """停止全量更新（设置停止标记，线程将尽快退出）。"""
        if _update_ctrl['state']['running']:
            _update_ctrl['stop_event'].set()
        return Response({'running': _update_ctrl['state']['running'], 'stopped': _update_ctrl['state']['stopped']})

class QueueUpdatePauseView(APIView):
    def post(self, request):
        queue_ctrl = get_queue_ctrl()
        if queue_ctrl['state']['running'] and not queue_ctrl['state']['paused']:
            queue_ctrl['state']['paused'] = True
        return Response({'running': queue_ctrl['state']['running'], 'paused': queue_ctrl['state']['paused']})

class QueueUpdateResumeView(APIView):
    def post(self, request):
        queue_ctrl = get_queue_ctrl()
        if queue_ctrl['state']['running'] and queue_ctrl['state']['paused']:
            queue_ctrl['state']['paused'] = False
        return Response({'running': queue_ctrl['state']['running'], 'paused': queue_ctrl['state']['paused']})

class QueueUpdateStopView(APIView):
    def post(self, request):
        queue_ctrl = get_queue_ctrl()
        if queue_ctrl['state']['running']:
            queue_ctrl['stop_event'].set()
        return Response({'running': queue_ctrl['state']['running'], 'stopped': queue_ctrl['state']['stopped']})


# QuotePlaceholderView已移除，实时行情功能已取消

class DataStatusView(APIView):
    def get(self, request):
        # 读取筛选参数
        market = request.query_params.get('market')

        import os
        import psycopg2
        # 导入连接池
        try:
            from db.db_pool import get_conn, put_conn
        except Exception:
            get_conn, put_conn = None, None

        host = os.getenv('QDB_HOST', 'localhost')
        port = int(os.getenv('QDB_PORT', '8812'))
        user = os.getenv('QDB_USER', 'admin')
        password = os.getenv('QDB_PASS', 'quest')
        dbname = os.getenv('QDB_DB', 'qdb')

        trends = []
        conn = None

        # 本地函数：根据market构造代码前缀过滤
        def market_prefixes(m):
            m = (m or '').upper()
            if m == 'SH':
                return ['600', '601', '603', '605', '688']
            if m == 'SZ':
                return ['000', '001', '002', '300', '301']
            if m == 'BJ':
                return ['430', '83', '87']
            return []

        # 本地函数：根据代码推断市场
        def infer_market(code):
            s = str(code or '').strip()
            if s.startswith(('600', '601', '603', '605', '688')):
                return 'SH'
            if s.startswith(('000', '001', '002', '300', '301')):
                return 'SZ'
            if s.startswith(('430', '83', '87')):
                return 'BJ'
            return 'SZ'

        try:
            # 尝试使用连接池获取连接
            if get_conn:
                try:
                    conn = get_conn()
                except Exception:
                    # 连接池失败，尝试直接连接
                    conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname, connect_timeout=2)
            else:
                # 如果连接池不可用，直接创建连接
                conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname, connect_timeout=2)
            
            conn.autocommit = True
            cur = conn.cursor()
            # 基础股票筛选
            where = []
            params = []
            # market过滤转化为code前缀过滤
            if market:
                prefs = market_prefixes(market)
                if prefs:
                    ors = []
                    for p in prefs:
                        params.append(p + '%')
                        ors.append(f"code like ${len(params)}")
                    where.append('(' + ' or '.join(ors) + ')')
            where_sql = (' where ' + ' and '.join(where)) if where else ''

            # 总览和市场分布功能已移除

            # 上市日期范围功能已移除

        except Exception:
            pass
        finally:
            # 归还连接到连接池或关闭连接
            if conn:
                try:
                    if put_conn:
                        put_conn(conn)
                    else:
                        conn.close()
                except Exception:
                    pass

        return Response({
            'backup': {
                'db_path': 'QuestDB',
                'size_bytes': None,
                'last_modified': None,
                'health_score': 100,
            },
            'trends': trends,
        })
