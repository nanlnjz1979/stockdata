from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from .db_pool import get_conn, put_conn
from datetime import datetime


def _query_dragon_tiger(ingest_date: str = None, query_type: str = None):
    # 如果没有提供日期，默认使用当天日期
    if not ingest_date:
        ingest_date = datetime.now().strftime('%Y-%m-%d')
    
    conn = None
    try:
        conn = get_conn()
    except Exception as e:
        return {'error': f'连接池错误: {str(e)}'}
    
    try:
        cur = conn.cursor()
        # 构建SQL查询，明确指定列名以匹配数据库表结构
        sql = """
        select  code, name, buy_amount, buy_times, 
               sell_amount, sell_times, net_amount, query_type 
        from inst_trading_tracker 
        where ingest_date = %s
        """
        params = [ingest_date]
        
        if query_type:
            sql += " and query_type = %s"
            params.append(query_type)
        
        cur.execute(sql, params)
        
        # 直接返回数值列表，不包含字段名
        rows = cur.fetchall() or []
        items = []
        for row in rows:
            # 将元组转换为列表，只返回数值
            items.append(list(row))
        
        return items
    except Exception as e:
        return {'error': f'查询失败: {str(e)}'}
    finally:
        if conn is not None:
            try:
                put_conn(conn)
            except Exception:
                pass


class DragonTigerView(APIView):
    def get(self, request):
        """龙虎榜列表数据，支持按ingest_date和query_type筛选。如果不提供ingest_date，默认查询当天数据。"""
        # 获取查询参数
        ingest_date = request.query_params.get('date')
        query_type = request.query_params.get('symbol')
        
        # 统一日期格式：兼容 YYYYMMDD 与 YYYY-MM-DD，最终转为 YYYY-MM-DD
        if ingest_date and ingest_date != 'None':
            if len(ingest_date) == 8 and '-' not in ingest_date:
                ingest_date = f"{ingest_date[:4]}-{ingest_date[4:6]}-{ingest_date[6:]}"
            # 其余情况保持原值，交由后续逻辑处理
        # 如果未提供ingest_date或值为空，则使用当天日期
        if ingest_date=='' or  ingest_date == 'None' or ingest_date == None:
            ingest_date = datetime.now().strftime('%Y-%m-%d')
        # 执行查询
        result = _query_dragon_tiger(ingest_date, query_type)
        
        # 处理错误情况
        if isinstance(result, dict) and 'error' in result:
            return Response(result, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
        return Response(result)


# DragonTigerDetailView类已移除