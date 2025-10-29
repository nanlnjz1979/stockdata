from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
import os
import re
from datetime import datetime

class DailyDataView(APIView):
    def get(self, request):
        """按代码与日期范围返回QuestDB中的股票日线数据（JSON）。
        参数：
          - symbol: 如 sh603843（必填）
          - start_date: YYYYMMDD（默认 19900101）
          - end_date: YYYYMMDD（默认 21000118）
          - adjust: ""/"hfq"/"qfq"；为空字符串表示查询 adjust_type 为 NULL；未传则不筛选复权类型
        """
        try:
            import psycopg2
            from psycopg2 import OperationalError
        except Exception:
            return Response({'error': '缺少 psycopg2 依赖，无法连接 QuestDB'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        symbol = request.query_params.get('symbol') or request.query_params.get('code')
        start_s = request.query_params.get('start_date') or '19900101'
        end_s = request.query_params.get('end_date') or '21000118'
        adjust = request.query_params.get('adjust')

        if not symbol:
            return Response({'error': 'symbol 参数必填，如 sh603843'}, status=status.HTTP_400_BAD_REQUEST)
        # 解析出纯数字代码
        code = re.sub(r'[^0-9]', '', str(symbol))
        if not code:
            return Response({'error': 'symbol 无法解析出股票代码'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            start_date = datetime.strptime(start_s, '%Y%m%d').date()
            end_date = datetime.strptime(end_s, '%Y%m%d').date()
        except Exception:
            return Response({'error': 'start_date 或 end_date 格式错误，应为 YYYYMMDD'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            from .db_pool import get_conn, put_conn
            conn = get_conn()
        except RuntimeError as e:
            return Response({'error': f'QuestDB连接失败: {str(e)}'}, status=status.HTTP_503_SERVICE_UNAVAILABLE)
        except Exception as e:
            return Response({'error': f'连接异常: {str(e)}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        cur = conn.cursor()
        where = ["code=%s", "trade_date >= %s", "trade_date <= %s"]
        params = [code, start_date, end_date]
        if adjust is not None:
            adj = str(adjust).strip()
            if adj == '':
                where.append("adjust_type IS NULL")
            else:
                where.append("adjust_type=%s")
                params.append(adj)
        where_sql = ' WHERE ' + ' AND '.join(where)

        items = []
        try:
            cur.execute(
                f"""
                SELECT  trade_date,  open, close, high, low, volume, amount, turnover, outstanding_share
                FROM stock_daily
                {where_sql}
                ORDER BY trade_date ASC
                """,
                params
            )
            rows = cur.fetchall() or []
            for r in rows:
                td = r[0]
                td_s = None
                try:
                    td_s = td.strftime('%Y-%m-%d')
                except Exception:
                    s = str(td)
                    # 取日期部分（去掉时间），保留破折号格式
                    td_s = s.split('T')[0].split(' ')[0]
                items.append([
                     td_s,
                     r[1],
                     r[3],
                     r[4],
                     r[2],
                     r[5],
                     r[6],
                     r[8],
                     r[7],
                ])
        except Exception as e:
            try:
                put_conn(conn)
            except Exception:
                pass
            return Response({'error': f'查询失败: {str(e)}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        try:
            put_conn(conn)
        except Exception:
            pass

        return Response(items)