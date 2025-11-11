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
        使用用户提供的SQL查询：SELECT code, trade_date FROM stock_daily LATEST BY code
        """
        logger = logging.getLogger(__name__)
        conn = None
        result = []
        
        try:
            # 获取数据库连接
            conn = get_conn()
            
            # 使用用户提供的SQL查询获取每个股票的最后更新时间
            sql = "SELECT code, trade_date FROM stock_daily LATEST BY code LIMIT %s"
            
            # 执行查询
            with conn.cursor() as cursor:
                cursor.execute(sql, (limit,))
                rows = cursor.fetchall()
                
                # 处理查询结果
                for row in rows:
                    code, trade_date = row
                    
                    # 计算更新状态和天数差
                    if isinstance(trade_date, str):
                        last_update_date = datetime.strptime(trade_date, '%Y-%m-%d')
                    else:
                        last_update_date = trade_date
                    
                    today = datetime.now().date()
                    days_diff = (today - last_update_date.date()).days
                    update_status = 1 - (days_diff / period) if days_diff <= period else 0
                    
                    # 根据复权类型设置类型名称
                    if data_type == 'before':
                        type_name = '前复权'
                    elif data_type == 'after':
                        type_name = '后复权'
                    else:
                        type_name = '不复权'
                    
                    # 判断市场类型
                    market = '上海' if code.startswith('6') else '深圳'
                    
                    result.append({
                        'code': code,
                        'name': f"{market}股票{code[-4:]}",  # 简单生成股票名称
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