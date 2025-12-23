from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
import logging
import pandas as pd
from datetime import datetime
import os
import psycopg2
from psycopg2 import OperationalError
from db.db_pool import get_conn, put_conn
from global_config.data_fetch import get_sw_industry_first_info, get_sw_industry_second_info, get_sw_industry_third_info, DataFetchError

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SWIndustryDataAPI(APIView):
    """
    申万行业数据抓取和存储API
    调用方式: POST /api/stocks/sw/generate
    """
    
    def post(self, request):
        try:
            # 获取数据库连接
            conn = get_conn()
            
            try:
                # 抓取申万一级行业数据
                sw_first_df = get_sw_industry_first_info()
                
                # 抓取申万二级行业数据
                sw_second_df = get_sw_industry_second_info()
                
                # 抓取申万三级行业数据
                sw_third_df = get_sw_industry_third_info()
                
                # 表已经提前创建，跳过表操作
                logger.info("表已存在，跳过表创建操作")
                
                # 存储数据
                current_timestamp = datetime.now()
                
                # 存储一级行业数据
                self._save_industry_data(conn, sw_first_df, "一级行业", current_timestamp)
                
                # 存储二级行业数据，需要关联一级行业
                self._save_industry_data(conn, sw_second_df, "二级行业", current_timestamp, sw_first_df)
                
                # 存储三级行业数据，需要关联二级行业
                self._save_industry_data(conn, sw_third_df, "三级行业", current_timestamp, sw_second_df)
                
                return Response({
                    "success": True,
                    "message": "申万行业数据抓取和存储成功",
                    "data": {
                        "first_level_count": len(sw_first_df),
                        "second_level_count": len(sw_second_df),
                        "third_level_count": len(sw_third_df),
                        "total_count": len(sw_first_df) + len(sw_second_df) + len(sw_third_df)
                    }
                }, status=status.HTTP_200_OK)
                
            finally:
                # 归还数据库连接
                put_conn(conn)
                
        except ImportError:
            logger.error("缺少akshare依赖")
            return Response({
                "success": False,
                "message": "缺少akshare依赖，请安装: pip install akshare"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        except DataFetchError as e:
            logger.error(f"数据抓取失败: {str(e)}")
            return Response({
                "success": False,
                "message": f"数据抓取失败: {str(e)}"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        except OperationalError as e:
            logger.error(f"QuestDB连接失败: {str(e)}")
            return Response({
                "success": False,
                "message": f"数据库连接失败: {str(e)}"
            }, status=status.HTTP_503_SERVICE_UNAVAILABLE)
        except Exception as e:
            logger.error(f"抓取申万行业数据时发生错误: {str(e)}")
            import traceback
            traceback.print_exc()
            return Response({
                "success": False,
                "message": f"数据抓取失败: {str(e)}"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    

    
    def _save_industry_data(self, conn, df, level_name, timestamp, parent_df=None):
        """
        保存行业数据到数据库
        """
        try:
            # 处理DataFrame，添加timestamp列并调整列名
            insert_df = df.copy()
            
            # 确定上级行业
            insert_df['parent_industry'] = ''  # 一级行业默认空字符串
            if '上级行业' in insert_df.columns:
                insert_df['parent_industry'] = insert_df['上级行业'].fillna('')
            
            # 提取和转换字段
            insert_df['component_count'] = insert_df.get('成份个数', 0).fillna(0)
            insert_df['static_pe'] = insert_df.get('静态市盈率', 0).fillna(0)
            insert_df['ttm_pe'] = insert_df.get('TTM(滚动)市盈率', insert_df.get('TTM市盈率', 0)).fillna(0)
            insert_df['pb_ratio'] = insert_df.get('市净率', 0).fillna(0)
            insert_df['static_dividend_yield'] = insert_df.get('静态股息率', 0).fillna(0)
            
            # 重命名列以匹配数据库表结构
            insert_df = insert_df.rename(columns={
                '行业代码': 'industry_code',
                '行业名称': 'industry_name'
            })
            
            # 准备插入数据
            insert_data = []
            for _, row in insert_df.iterrows():
                # 将每一行转换为Python原生类型的元组，确保LowCardinality(String)字段使用空字符串而非None
                data_row = (
                    str(row['industry_code']) if row['industry_code'] is not None else '',
                    str(row['industry_name']) if row['industry_name'] is not None else '',
                    str(row['parent_industry']) if row['parent_industry'] is not None else '',
                    int(row['component_count']),
                    float(row['static_pe']),
                    float(row['ttm_pe']),
                    float(row['pb_ratio']),
                    float(row['static_dividend_yield']),
                    timestamp  # 已经是Python datetime类型
                )
                insert_data.append(data_row)
            
            # 使用execute方法批量插入数据
            if insert_data:
                # ClickHouse驱动直接接受数据列表，不需要手动构建SQL
                insert_sql = """
                INSERT INTO sw_industry_data (
                    industry_code, industry_name, parent_industry, component_count, 
                    static_pe, ttm_pe, pb_ratio, static_dividend_yield, timestamp
                ) VALUES
                """
                
                # 直接将数据列表传递给execute方法
                conn.execute(insert_sql, insert_data)
            
            # ClickHouse自动提交，不需要显式commit
            logger.info(f"成功存储{level_name}数据，共{len(insert_data)}条")
            
        except Exception as e:
            # ClickHouse不需要显式rollback
            logger.error(f"存储{level_name}数据时发生错误: {str(e)}")
            import traceback
            traceback.print_exc()
            raise


class SWIndustryClassificationAPI(APIView):
    """
    申万行业分类数据查询API
    调用方式: GET /api/stocks/sw/classification
    """
    
    def get(self, request):
        try:
            # 获取数据库连接
            conn = get_conn()
            
            # 查询申万行业分类数据
            query_sql = """
            SELECT 
                industry_code,
                industry_name,
                parent_industry,
                component_count,
                static_pe,
                ttm_pe,
                pb_ratio,
                static_dividend_yield
            FROM 
                sw_industry_data_v
            ORDER BY 
                CASE 
                    WHEN parent_industry = '' THEN 0
                    ELSE 1
                END,
                industry_code
            """
            
            # 使用ClickHouse Client的execute方法查询数据
            rows = conn.execute(query_sql)
            
            # 定义列名
            columns = ['industry_code', 'industry_name', 'parent_industry', 'component_count', 
                     'static_pe', 'ttm_pe', 'pb_ratio', 'static_dividend_yield']
            
            # 创建DataFrame
            df = pd.DataFrame(rows, columns=columns)
            
            # 转换为字典列表
            result = df.to_dict('records')
            
            return Response({
                "success": True,
                "message": "查询成功",
                "data": result
            }, status=status.HTTP_200_OK)
            
        except OperationalError as e:
            logger.error(f"数据库连接失败: {str(e)}")
            return Response({
                "success": False,
                "message": f"数据库连接失败: {str(e)}"
            }, status=status.HTTP_503_SERVICE_UNAVAILABLE)
        except Exception as e:
            logger.error(f"查询申万行业分类数据时发生错误: {str(e)}")
            import traceback
            traceback.print_exc()
            return Response({
                "success": False,
                "message": f"查询失败: {str(e)}"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            # 归还数据库连接
            if 'conn' in locals():
                put_conn(conn)


class SWThirdLevelIndustryCodesAPI(APIView):
    """
    申万三级行业成分股API
    调用方式: GET /api/stocks/sw/third_level_industry_codes?code=行业代码
    功能: 获取并保存三级行业的成分股数据
    """
    

    
    def _save_industry_stocks(self, conn, df, industry_code):
        """
        保存行业成分股数据到数据库
        使用批量插入方式提高性能
        """
        try:
            # 准备插入数据
            insert_data = []
            current_time = datetime.now()
            
            for _, row in df.iterrows():
                # 处理日期
                include_date_str = row.get('纳入时间', '')
                include_date = None
                if include_date_str and include_date_str != '—':
                    try:
                        include_date = datetime.strptime(include_date_str, '%Y-%m-%d').date()
                    except ValueError:
                        include_date = None
                
                # 处理数值字段，将NaN和无效值转换为0.0，因为ClickHouse表不允许NULL值
                def safe_float(value):
                    if pd.isna(value) or value == '—' or value == '':
                        return 0.0
                    try:
                        return float(value)
                    except (ValueError, TypeError):
                        return 0.0
                
                # 处理分类字段，将'—'转换为空字符串，因为String类型不能接受None值
                def safe_str(value):
                    if pd.isna(value) or value == '—' or value is None or value == '':
                        return ''
                    return str(value)
                
                # 将每一行转换为Python原生类型的元组
                data_row = (
                    str(row.get('股票代码', '')),
                    str(row.get('股票简称', '')),
                    industry_code,
                    include_date,
                    safe_str(row.get('申万1级', '')),
                    safe_str(row.get('申万2级', '')),
                    safe_str(row.get('申万3级', '')),
                    safe_float(row.get('价格')),
                    safe_float(row.get('市盈率')),
                    safe_float(row.get('市盈率ttm')),
                    safe_float(row.get('市净率')),
                    safe_float(row.get('股息率')),
                    safe_float(row.get('市值')),
                    safe_float(row.get('归母净利润同比增长(09-30)')),
                    safe_float(row.get('归母净利润同比增长(06-30)')),
                    safe_float(row.get('营业收入同比增长(09-30)')),
                    safe_float(row.get('营业收入同比增长(06-30)')),
                    current_time
                )
                insert_data.append(data_row)
            
            # 使用execute方法批量插入数据
            if insert_data:
                # ClickHouse驱动的execute方法直接接受数据列表，不需要在SQL中写占位符
                insert_sql = """
                INSERT INTO sw_industry_stocks (
                    stock_code, stock_name, industry_code, include_date, 
                    sw_first_level, sw_second_level, sw_third_level, 
                    price, pe, pe_ttm, pb, dividend_yield, market_cap,
                    net_profit_growth_0930, net_profit_growth_0630,
                    revenue_growth_0930, revenue_growth_0630,
                    update_time
                ) VALUES
                """
                
                # 直接将数据列表传递给execute方法
                conn.execute(insert_sql, insert_data)
            
            # ClickHouse自动提交，不需要显式commit
            logger.info(f"成功保存行业{industry_code}的成分股数据，共{len(insert_data)}条")
            
        except Exception as e:
            # ClickHouse不需要显式rollback
            logger.error(f"保存行业{industry_code}的成分股数据时发生错误: {str(e)}")
            import traceback
            traceback.print_exc()
            raise
    
    def get(self, request):
        conn = None
        try:
            # 获取请求中的code参数
            code = request.GET.get('code', None)
            
            # 如果提供了code参数，获取该行业对应的股票代码
            if code:
                # 调用我们封装的函数获取行业成分股
                from global_config.data_fetch import get_sw_index_third_cons
                df = get_sw_index_third_cons(symbol=code)
                
                # 获取数据库连接并保存数据
                conn = get_conn()
                

                # 表已经提前创建，跳过表检查
                
                # 保存数据到数据库
                self._save_industry_stocks(conn, df, code)
                
                # 转换为字典列表返回
                result = df.to_dict('records')
                
                return Response({
                    "success": True,
                    "message": f"成功获取并保存行业{code}的成分股数据",
                    "data": {
                        #"stocks": result,
                        "count": len(result),
                        "saved_to_db": True
                    }
                }, status=status.HTTP_200_OK)
            else:
                # 如果没有提供code参数，返回错误信息
                return Response({
                    "success": False,
                    "message": "请提供行业代码参数code",
                    "data": None
                }, status=status.HTTP_400_BAD_REQUEST)
           
        except OperationalError as e:
            logger.error(f"数据库连接失败: {str(e)}")
            return Response({
                "success": False,
                "message": f"数据库连接失败: {str(e)}"
            }, status=status.HTTP_503_SERVICE_UNAVAILABLE)
        except Exception as e:
            logger.error(f"查询或保存行业数据时发生错误: {str(e)}")
            import traceback
            traceback.print_exc()
            return Response({
                "success": False,
                "message": f"操作失败: {str(e)}"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            # 归还数据库连接
            if conn:
                put_conn(conn)