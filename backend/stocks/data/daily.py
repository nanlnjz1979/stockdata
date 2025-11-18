from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
import os
import re
from datetime import datetime
import pandas as pd
# 导入文件配置类
from global_config.file_config import FileConfig
# 导入股票数据读取类
from db.stock_data import StockData

class DailyDataView(APIView):
    def _get_data_from_database(self, symbol, code, start_date, end_date, adjust):
        """从数据库读取股票数据
        
        Args:
            symbol: 完整股票代码（可能带前缀）
            code: 纯数字股票代码
            start_date: 开始日期，格式为'YYYY-MM-DD'
            end_date: 结束日期，格式为'YYYY-MM-DD'
            adjust: 复权类型
            
        Returns:
            Response对象，包含查询结果或错误信息
        """
        try:
            import psycopg2
            from psycopg2 import OperationalError
        except Exception:
            return Response({'error': '缺少 psycopg2 依赖，无法连接 QuestDB'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
        # 添加项目路径以便导入db模块
        import sys
        import os
        project_root = os.path.join(os.path.dirname(__file__), '..', '..', '..')
        if project_root not in sys.path:
            sys.path.append(project_root)
        
        # 导入连接池
        try:
            from backend.db.db_pool import get_conn, put_conn
        except Exception:
            # 如果导入失败，设置为None
            get_conn, put_conn = None, None
        
        try:
            # 尝试使用连接池获取连接
            conn = get_conn()
            conn.autocommit = True
        except OperationalError as e:
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
            # 确保关闭连接
            try:
                if put_conn:
                    put_conn(conn)
                else:
                    conn.close()
            except Exception:
                pass
            return Response({'error': f'查询失败: {str(e)}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
        # 归还连接到连接池或关闭连接
        try:
            if put_conn:
                put_conn(conn)
            else:
                conn.close()
        except Exception:
            pass
        
        return Response(items)
    
    def _get_data_from_file(self, symbol, code, start_date, end_date, adjust):
        """从文件读取股票数据
        
        Args:
            symbol: 完整股票代码（可能带前缀）
            code: 纯数字股票代码
            start_date: 开始日期，格式为'YYYY-MM-DD'
            end_date: 结束日期，格式为'YYYY-MM-DD'
            adjust: 复权类型
            
        Returns:
            Response对象，包含查询结果或错误信息
        """
        try:
            # 从配置中获取CSV文件目录
            csv_dir = FileConfig.get('csv_data_dir', os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'daily'))
            # 设置StockData的数据目录
            StockData.set_data_dir(csv_dir)
            
            # 使用StockData.GetData获取数据
            # 将YYYY-MM-DD格式转换为YYYYMMDD格式
            start_date_str = start_date.replace('-', '')
            end_date_str = end_date.replace('-', '')
            
            df = StockData.GetData(code=symbol, s_time=start_date, e_time=end_date, adjust=adjust)
            
            # 转换为所需的输出格式
            items = []
            if not df.empty:
                for _, row in df.iterrows():
                    # 确保日期格式正确
                    trade_date = row['trade_date']
                    if isinstance(trade_date, datetime):
                        td_s = trade_date.strftime('%Y-%m-%d')
                    else:
                        try:
                            # 尝试直接处理字符串
                            td_s = str(trade_date)
                            # 处理可能的时间部分
                            if 'T' in td_s or ' ' in td_s:
                                td_s = td_s.split('T')[0].split(' ')[0]
                        except:
                            td_s = str(trade_date)
                    
                    items.append([
                        td_s,
                        row.get('open'),
                        row.get('high'),
                        row.get('low'),
                        row.get('close'),
                        row.get('volume'),
                        row.get('amount'),
                        row.get('outstanding_share'),
                        row.get('turnover'),
                    ])
            
            return Response(items)
        except Exception as e:
            return Response({'error': f'文件读取失败: {str(e)}'}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def get(self, request):
        """按代码与日期范围返回股票日线数据（JSON）。
        根据配置决定从数据库或文件读取数据。
        参数：
          - symbol: 如 sh603843（必填）
          - start_date: YYYYMMDD（默认 19900101）
          - end_date: YYYYMMDD（默认 21000118）
          - adjust: ""/"hfq"/"qfq"；为空字符串表示查询 adjust_type 为 NULL；未传则不筛选复权类型
        """
        # 从配置中获取数据来源设置，默认为'database'
        data_source = FileConfig.get('data_source', 'database')

        symbol = request.query_params.get('symbol') or request.query_params.get('code')
        start_s = request.query_params.get('start_date') or '19900101'
        end_s = request.query_params.get('end_date') or '21000118'
        adjust = request.query_params.get('adjust')

        if not symbol:
            return Response({'error': 'symbol 参数必填，如 sh603843'}, status=status.HTTP_400_BAD_REQUEST)
        
        # 解析出纯数字代码
        try:
            # 优化：直接处理数字或字符串，避免不必要的正则处理
            code = str(symbol)
            if code.isdigit():
                # 如果已经是纯数字，直接使用
                pass
            else:
                # 否则使用正则提取数字
                code = re.sub(r'[^0-9]', '', code)
            
            if not code:
                return Response({'error': 'symbol 无法解析出股票代码'}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({'error': f'处理symbol参数失败: {str(e)}'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            # 先解析为datetime对象进行验证
            start_datetime = datetime.strptime(start_s, '%Y%m%d')
            end_datetime = datetime.strptime(end_s, '%Y%m%d')
            # 转换为适合SQL查询的字符串格式 'YYYY-MM-DD'
            start_date = start_datetime.strftime('%Y-%m-%d')
            end_date = end_datetime.strftime('%Y-%m-%d')
            
        except Exception:
            return Response({'error': 'start_date 或 end_date 格式错误，应为 YYYYMMDD'}, status=status.HTTP_400_BAD_REQUEST)
        
        # 根据配置选择数据源
        if data_source.lower() == 'csv':
            # 从文件读取数据
            return self._get_data_from_file(symbol, code, start_date, end_date, adjust)
        else:
            # 从数据库读取数据
            return self._get_data_from_database(symbol, code, start_date, end_date, adjust)