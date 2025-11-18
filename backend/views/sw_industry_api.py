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
from backend.global_config.data_fetch import get_sw_industry_first_info, get_sw_industry_second_info, get_sw_industry_third_info, DataFetchError

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
                
                # 先删除表，然后重建
                logger.info("开始删除并重建sw_industry_data表")
                cursor = conn.cursor()
                try:
                    # 删除表（如果存在）
                    cursor.execute("DROP TABLE IF EXISTS sw_industry_data")
                    conn.commit()
                    logger.info("成功删除sw_industry_data表")
                finally:
                    cursor.close()
                
                # 重建表
                self._ensure_table_exists(conn)
                logger.info("表重建完成")
                
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
    
    def _ensure_table_exists(self, conn):
        """
        确保sw_industry_data表存在
        """
        create_table_sql = """
        create table if not exists sw_industry_data (
            industry_code SYMBOL INDEX,        -- 行业代码，使用SYMBOL类型并创建索引
            industry_name SYMBOL ,        -- 行业名称，使用SYMBOL类型并创建索引
            parent_industry SYMBOL ,      -- 上级行业，使用SYMBOL类型并创建索引
            component_count INT,               -- 成份个数
            static_pe DOUBLE,                  -- 静态市盈率
            ttm_pe DOUBLE,                    -- TTM(滚动)市盈率
            pb_ratio DOUBLE,                  -- 市净率
            static_dividend_yield DOUBLE,     -- 静态股息率
            timestamp TIMESTAMP               -- 时间戳，QuestDB推荐使用
        ) TIMESTAMP(timestamp) PARTITION BY MONTH;
        """
        
        try:
            cursor = conn.cursor()
            cursor.execute(create_table_sql)
            cursor.close()
            logger.info("确保sw_industry_data表存在")
        except Exception as e:
            logger.error(f"创建表时发生错误: {str(e)}")
            raise
    
    def _save_industry_data(self, conn, df, level_name, timestamp, parent_df=None):
        """
        保存行业数据到数据库
        """
        insert_sql = """
        INSERT INTO sw_industry_data (
            industry_code, industry_name, parent_industry, 
            component_count, static_pe, ttm_pe, pb_ratio, static_dividend_yield, timestamp
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor = conn.cursor()
        try:
            for _, row in df.iterrows():
                # 提取行业代码和名称
                industry_code = row['行业代码']
                industry_name = row['行业名称']
                
                # 确定上级行业
                parent_industry = ""  # 一级行业的上级行业为空字符串
                # 对于二级和三级行业，直接使用已有的上级行业字段
                if '上级行业' in row and pd.notna(row['上级行业']):
                    parent_industry = row['上级行业']
                
                # 提取财务指标
                component_count = int(row.get('成份个数', 0))
                static_pe = float(row.get('静态市盈率', 0))
                ttm_pe = float(row.get('TTM(滚动)市盈率', row.get('TTM市盈率', 0)))
                pb_ratio = float(row.get('市净率', 0))
                static_dividend_yield = float(row.get('静态股息率', 0))
                
                # 插入数据
                params = (
                    industry_code,
                    industry_name,
                    parent_industry,
                    component_count,
                    static_pe,
                    ttm_pe,
                    pb_ratio,
                    static_dividend_yield,
                    timestamp
                )
                
                cursor.execute(insert_sql, params)
            
            conn.commit()
            logger.info(f"成功存储{level_name}数据，共{len(df)}条")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"存储{level_name}数据时发生错误: {str(e)}")
            raise
        finally:
            cursor.close()


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
                sw_industry_data
            ORDER BY 
                CASE 
                    WHEN parent_industry = '' THEN 0
                    ELSE 1
                END,
                industry_code
            """
            
            df = pd.read_sql(query_sql, conn)
            
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
    
    def _ensure_stock_table_exists(self, conn):
        """
        确保sw_industry_stocks表存在，以股票代码为主键
        """
        create_table_sql = """
        create table if not exists sw_industry_stocks (
            stock_code SYMBOL INDEX,        -- 股票代码，使用SYMBOL类型并作为主键
            stock_name SYMBOL,                    -- 股票简称
            industry_code SYMBOL ,           -- 行业代码
            include_date DATE,                    -- 纳入时间
            sw_first_level SYMBOL,                -- 申万1级
            sw_second_level SYMBOL,               -- 申万2级
            sw_third_level SYMBOL,                -- 申万3级
            price DOUBLE,                         -- 价格
            pe DOUBLE,                            -- 市盈率
            pe_ttm DOUBLE,                        -- 市盈率ttm
            pb DOUBLE,                            -- 市净率
            dividend_yield DOUBLE,                -- 股息率
            market_cap DOUBLE,                    -- 市值
            net_profit_growth_0930 DOUBLE,        -- 归母净利润同比增长(09-30)
            net_profit_growth_0630 DOUBLE,        -- 归母净利润同比增长(06-30)
            revenue_growth_0930 DOUBLE,           -- 营业收入同比增长(09-30)
            revenue_growth_0630 DOUBLE,           -- 营业收入同比增长(06-30)
            update_time TIMESTAMP                 -- 更新时间
        ) TIMESTAMP(update_time) PARTITION BY MONTH;
        """
        
        try:
            cursor = conn.cursor()
            cursor.execute(create_table_sql)
            cursor.close()
            logger.info("确保sw_industry_stocks表存在")
        except Exception as e:
            logger.error(f"创建行业成分股表时发生错误: {str(e)}")
            raise
    
    def _save_industry_stocks(self, conn, df, industry_code):
        """
        保存行业成分股数据到数据库
        使用INSERT OR UPDATE语句确保以股票代码为主键的数据更新
        """
        # 使用QuestDB的upsert语法
        insert_sql = """
        INSERT INTO sw_industry_stocks (
            stock_code, stock_name, industry_code, include_date, 
            sw_first_level, sw_second_level, sw_third_level, 
            price, pe, pe_ttm, pb, dividend_yield, market_cap,
            net_profit_growth_0930, net_profit_growth_0630,
            revenue_growth_0930, revenue_growth_0630,
            update_time
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor = conn.cursor()
        current_timestamp = datetime.now()
        
        try:
            for _, row in df.iterrows():
                # 处理数据，将NaN转换为None
                def safe_convert(value, dtype=float):
                    if pd.isna(value):
                        return None
                    try:
                        return dtype(value)
                    except (ValueError, TypeError):
                        return None
                
                # 提取数据，根据实际列名调整
                stock_code = row.get('股票代码', '')
                stock_name = row.get('股票简称', '')
                include_date_str = row.get('纳入时间', '')
                
                # 处理日期
                include_date = None
                if include_date_str and include_date_str != '—':
                    try:
                        include_date = datetime.strptime(include_date_str, '%Y-%m-%d').date()
                    except ValueError:
                        include_date = None
                
                # 构建参数
                params = (
                    stock_code,
                    stock_name,
                    industry_code,
                    include_date,
                    row.get('申万1级', '') if row.get('申万1级', '') != '—' else None,
                    row.get('申万2级', '') if row.get('申万2级', '') != '—' else None,
                    row.get('申万3级', '') if row.get('申万3级', '') != '—' else None,
                    safe_convert(row.get('价格')),
                    safe_convert(row.get('市盈率')),
                    safe_convert(row.get('市盈率ttm')),
                    safe_convert(row.get('市净率')),
                    safe_convert(row.get('股息率')),
                    safe_convert(row.get('市值')),
                    safe_convert(row.get('归母净利润同比增长(09-30)')),
                    safe_convert(row.get('归母净利润同比增长(06-30)')),
                    safe_convert(row.get('营业收入同比增长(09-30)')),
                    safe_convert(row.get('营业收入同比增长(06-30)')),
                    current_timestamp
                )
                
                cursor.execute(insert_sql, params)
            
            conn.commit()
            logger.info(f"成功保存行业{industry_code}的成分股数据，共{len(df)}条")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"保存行业{industry_code}的成分股数据时发生错误: {str(e)}")
            raise
        finally:
            cursor.close()
    
    def get(self, request):
        conn = None
        try:
            # 获取请求中的code参数
            code = request.GET.get('code', None)
            
            # 如果提供了code参数，获取该行业对应的股票代码
            if code:
                # 调用我们封装的函数获取行业成分股
                from backend.global_config.data_fetch import get_sw_index_third_cons, get_sw_index_data
                df = get_sw_index_third_cons(symbol=code)
                
                # 获取数据库连接并保存数据
                conn = get_conn()
                

                # 确保表存在
                self._ensure_stock_table_exists(conn)
                
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