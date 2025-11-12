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
            industry_name SYMBOL INDEX,        -- 行业名称，使用SYMBOL类型并创建索引
            parent_industry SYMBOL INDEX,      -- 上级行业，使用SYMBOL类型并创建索引
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