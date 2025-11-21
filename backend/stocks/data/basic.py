from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from .db_pool import get_conn, put_conn


from backend.global_config.stock_info import StockInfo
class StocksSHView(APIView):
    def get(self, request):
        """上证（SH）股票基础数据，JSON数组返回。"""
        
        result = StockInfo.get_stocks_by_market('SH')
        if isinstance(result, dict) and 'error' in result:
            return Response(result, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        return Response(result)


class StocksSZView(APIView):
    def get(self, request):
        """深证（SZ）股票基础数据，JSON数组返回。"""
        result = StockInfo.get_stocks_by_market('SZ')
        if isinstance(result, dict) and 'error' in result:
            return Response(result, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        return Response(result)


class StocksBJView(APIView):
    def get(self, request):
        """北京（BJ）股票基础数据，JSON数组返回。"""
        result = StockInfo.get_stocks_by_market('BJ')
        if isinstance(result, dict) and 'error' in result:
            return Response(result, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        return Response(result)