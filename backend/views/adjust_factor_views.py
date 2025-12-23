from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
import logging
import traceback
from datetime import datetime, timedelta
import pandas as pd
import os
from django.conf import settings
from db import get_conn, put_conn
from global_config.data_fetch import AkshareFetcher, DataFetchError
from global_config.file_config import FileConfig

logger = logging.getLogger(__name__)

class AdjustFactorUpdateView(APIView):
    """
    复权因子更新API视图
    调用方式: POST /api/stocks/update/adjust_factor/start
    功能: 更新股票的复权因子数据到ClickHouse数据库
    """
    
    def post(self, request):
        try:
            # 记录开始时间
            start_time = datetime.now()
            logger.info(f"开始更新复权因子数据，开始时间: {start_time}")
            
            from global_config.stock_info import StockInfo
            import concurrent.futures

            basics = StockInfo.get_all_stocks()    #基础股票代码库
            
            # 从股票基础信息中提取股票代码列表
            stock_codes = [stock.get('code', '') for stock in basics if stock.get('code')]
            
            # 创建保存复权因子数据的目录
            adjust_factor_dir = os.path.join(settings.BASE_DIR, 'data', 'adjust_factors')
            os.makedirs(adjust_factor_dir, exist_ok=True)
            
            # 定义线程池结果
            total_files_saved = 0
            total_records = 0
            
            def process_stock(code):
                """
                处理单个股票的复权因子数据
                返回处理结果：(成功标志, 记录数, 错误信息)
                """
                nonlocal total_files_saved, total_records
                
                try:
                    logger.info(f"线程开始处理股票{code}的复权因子数据")
                    
                    # 每个线程创建自己的AkshareFetcher实例
                    fetcher = AkshareFetcher()
                    
                    # 获取前复权因子数据
                    qfq_df = fetcher.fetch_stock_adjust_factor(code, adjust="qfq-factor")
                    
                    # 获取后复权因子数据
                    hfq_df = fetcher.fetch_stock_adjust_factor(code, adjust="hfq-factor")
                    
                    if not qfq_df.empty and not hfq_df.empty:
                        # 确保列名正确并保留所需列
                        # 处理前复权因子数据
                        if '日期' in qfq_df.columns:
                            qfq_df = qfq_df[['日期', '复权因子']].rename(columns={'日期': 'date', '复权因子': 'qfq_factor'})
                        
                        # 处理后复权因子数据
                        if '日期' in hfq_df.columns:
                            hfq_df = hfq_df[['日期', '复权因子']].rename(columns={'日期': 'date', '复权因子': 'hfq_factor'})
                        
                        # 合并前复权和后复权数据
                        merged_df = qfq_df.merge(hfq_df, on='date', how='inner')
                        
                        if not merged_df.empty:
                            # 添加股票代码列
                            merged_df['code'] = code
                            
                            # 重命名列以匹配数据库表结构
                            merged_df = merged_df.rename(columns={
                                'hfq_factor': 'hfq',
                                'qfq_factor': 'qfq'
                            })
                            
                            # 调整列顺序：code, date, hfq, qfq
                            merged_df = merged_df[['code', 'date', 'hfq', 'qfq']]
                            
                            # 保存到CSV文件
                            csv_file_path = os.path.join(adjust_factor_dir, f"{code}.csv")
                            merged_df.to_csv(csv_file_path, index=False, encoding='utf-8')
                            
                            logger.info(f"线程成功保存股票{code}的复权因子数据到文件: {csv_file_path}，共{len(merged_df)}条记录")
                            
                            return (True, len(merged_df), None)
                    return (False, 0, "没有获取到有效数据")
                except Exception as e:
                    error_msg = f"处理股票{code}的复权因子数据失败: {str(e)}"
                    logger.error(error_msg)
                    traceback.print_exc()
                    return (False, 0, error_msg)
            
            # 使用线程池并行处理股票
            thread_count = min(5, len(stock_codes))  # 限制线程数量，避免API限制
            logger.info(f"使用{thread_count}个线程并行处理{len(stock_codes)}只股票")
            
            # 统计结果
            successful_stocks = 0
            total_processed_records = 0
            errors = []
            
            # 创建线程池并处理股票
            with concurrent.futures.ThreadPoolExecutor(max_workers=thread_count) as executor:
                # 提交所有任务
                future_to_code = {executor.submit(process_stock, code): code for code in stock_codes}
                
                # 获取结果
                for future in concurrent.futures.as_completed(future_to_code):
                    code = future_to_code[future]
                    try:
                        success, record_count, error = future.result()
                        if success:
                            successful_stocks += 1
                            total_processed_records += record_count
                        elif error:
                            errors.append(f"股票{code}: {error}")
                    except Exception as e:
                        error_msg = f"获取股票{code}的处理结果时发生错误: {str(e)}"
                        logger.error(error_msg)
                        errors.append(error_msg)
            
            # 计算耗时
            end_time = datetime.now()
            time_cost = (end_time - start_time).total_seconds()
            
            logger.info(f"复权因子数据更新完成，共处理{len(stock_codes)}只股票，成功{successful_stocks}只，保存{total_processed_records}条记录，耗时{time_cost:.2f}秒")
            
            # 构建响应
            response_data = {
                "success": True,
                "message": f"复权因子数据更新完成，共处理{len(stock_codes)}只股票，成功{successful_stocks}只，保存{total_processed_records}条记录",
                "total_stocks": len(stock_codes),
                "successful_stocks": successful_stocks,
                "total_records": total_processed_records,
                "time_cost": time_cost
            }
            
            if errors:
                response_data["errors"] = errors
            
            return Response(response_data)
            
        except Exception as e:
            logger.error(f"更新复权因子数据失败: {str(e)}")
            logger.error(traceback.format_exc())
            return Response({
                "success": False,
                "message": f"更新复权因子数据失败: {str(e)}",
                "error": str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class AdjustFactorStatusView(APIView):
    """
    获取复权因子更新状态API视图
    调用方式: GET /api/stocks/update/adjust_factor/status
    功能: 获取复权因子更新的状态信息
    """
    
    def get(self, request):
        conn = None
        try:
            # 获取数据库连接
            conn = get_conn()
            
            # 查询最近的复权因子更新记录
            # 实际项目中应该根据具体情况实现
            # 例如：从日志表或状态表中查询
            
            # 这里模拟返回一些状态信息
            status_data = {
                "last_update_time": datetime.now().isoformat(),
                "total_count": 10000,
                "updated_count": 8500,
                "status": "completed",
                "message": "复权因子更新已完成"
            }
            
            return Response({
                "success": True,
                "data": status_data
            })
            
        except Exception as e:
            logger.error(f"获取复权因子更新状态失败: {str(e)}")
            logger.error(traceback.format_exc())
            return Response({
                "success": False,
                "message": f"获取复权因子更新状态失败: {str(e)}",
                "error": str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            # 归还数据库连接
            if conn:
                put_conn(conn)
