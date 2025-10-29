from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from .db_pool import get_conn, put_conn


def _list_by_market(market: str):
    conn = None
    try:
        conn = get_conn()
    except Exception as e:
        return {'error': f'连接池错误: {str(e)}'}
    items = []
    try:
        cur = conn.cursor()
        cur.execute(
            """
            select code, name, company_name, listing_date
            from stock_basic
            where market = %s
            order by code asc
            """,
            (market,)
        )
        rows = cur.fetchall() or []
        # 映射字段名，listing_date 映射为 listing_data（按需求）
        items = [
            [
                r[0], r[1], r[2], r[3]
            ] for r in rows
        ]
    except Exception as e:
        return {'error': f'查询失败: {str(e)}'}
    finally:
        if conn is not None:
            try:
                put_conn(conn)
            except Exception:
                pass
    return items


class StocksSHView(APIView):
    def get(self, request):
        """上证（SH）股票基础数据，JSON数组返回。"""
        result = _list_by_market('SH')
        if isinstance(result, dict) and 'error' in result:
            return Response(result, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        return Response(result)


class StocksSZView(APIView):
    def get(self, request):
        """深证（SZ）股票基础数据，JSON数组返回。"""
        result = _list_by_market('SZ')
        if isinstance(result, dict) and 'error' in result:
            return Response(result, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        return Response(result)


class StocksBJView(APIView):
    def get(self, request):
        """北京（BJ）股票基础数据，JSON数组返回。"""
        result = _list_by_market('BJ')
        if isinstance(result, dict) and 'error' in result:
            return Response(result, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        return Response(result)