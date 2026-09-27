from rest_framework.views import APIView
from rest_framework.response import Response
from datetime import datetime
import logging
from db.db_pool import get_conn, put_conn

class HeatmapDataView(APIView):
    """
    热力图数据API视图
    返回股票最近更新的热力图数据
    """
    
    def get(self, request, *args, **kwargs):
        """
        获取热力图数据
        参数:
            period: 时间范围 (7/30/90天)
            type: 复权类型 (before/after/none)
            limit: 返回数量限制
        """
        # 获取请求参数
        period = request.GET.get('period', '30')  # 默认30天
        data_type = request.GET.get('type', 'none')  # 默认不复权
        limit = request.GET.get('limit', '100')  # 默认返回100支股票
        
        try:
            period = int(period)
            limit = int(limit)
        except ValueError:
            return Response({'error': 'Invalid parameters'}, status=400)
        
        # 从数据库获取真实数据
        heatmap_data = self._get_data_from_database(period, data_type, limit)
        
        return Response({
            'data': heatmap_data,
            'period': period,
            'type': data_type,
            'total': len(heatmap_data)
        })

    def _get_data_from_database(self, period, data_type, limit):
        """
        从数据库获取热力图数据
        使用用户提供的SQL查询：SELECT code, date FROM stock_daily LATEST BY code
        """
        logger = logging.getLogger(__name__)
        conn = None
        result = []
        
        try:
            # 从StockInfo获取股票代码到名称的映射
            from global_config.stock_info import StockInfo
            all_stocks = StockInfo.get_all_stocks()
            code_to_name = {stock.get('code'): stock.get('name', '') for stock in all_stocks}
            
            # 获取数据库连接
            conn = get_conn()
            
            # 直接查基表 stock_daily，只取 code 和 max(date)
            # 加 WHERE date >= today() - 365 只扫最近12个月的分区（428个分区中只需3-4个）
            # 用 last_date 别名避免 ClickHouse 把 max(date) AS date 与 WHERE 中的 date 混淆
            sql = f"""
            SELECT code, max(date) AS last_date
            FROM stock_daily
            WHERE date >= today() - 365
            GROUP BY code
            ORDER BY code
            LIMIT {limit}
            SETTINGS max_execution_time = 30
            """
            
            # 执行查询 - ClickHouse客户端直接支持execute方法，不需要cursor
            rows = conn.execute(sql)
            
            # 处理查询结果
            for row in rows:
                code, last_update_date = row
                
                # 计算更新状态和天数差
                if isinstance(last_update_date, str):
                    last_update_date = datetime.strptime(last_update_date, '%Y-%m-%d')
                
                today = datetime.now().date()
                days_diff = (today - last_update_date.date()).days
                update_status = 1 - (days_diff / period) if days_diff <= period else 0
                
                # 使用StockInfo中的真实股票名称
                stock_name = code_to_name.get(code, code)
                type_name = '不复权'
                
                result.append({
                    'code': code,
                    'name': stock_name,
                    'last_update': last_update_date.strftime('%Y-%m-%d'),
                    'update_status': round(update_status, 2),
                    'type': type_name,
                    'days_since_update': days_diff
                })
            
            # 按更新状态排序
            result.sort(key=lambda x: x['update_status'], reverse=True)
            
        except Exception as e:
            logger.error(f"查询热力图数据失败: {str(e)}")
            # 发生错误时记录日志但仍返回空列表，由上层处理
            result = []
        finally:
            # 归还数据库连接
            if conn:
                put_conn(conn)
        
        return result
