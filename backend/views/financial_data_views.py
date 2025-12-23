from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
import logging
import traceback
from datetime import datetime, timedelta
import pandas as pd
import os
from django.conf import settings
from global_config.data_fetch import AkshareFetcher, DataFetchError

logger = logging.getLogger(__name__)

# 定义全局状态变量，用于跟踪金融数据更新状态
financial_update_status = {
    "running": False,
    "paused": False,
    "total_count": 0,
    "updated_count": 0,
    "status": "idle",
    "last_update_time": datetime.now().isoformat(),
    "current_code": "-"
}


class FinancialDataUpdateView(APIView):
    """
    金融数据更新API视图
    调用方式: POST /api/stocks/update/financial_data/start
    功能: 更新股票的金融数据到CSV文件
    """
    
    def post(self, request):
        global financial_update_status
        try:
            # 记录开始时间
            start_time = datetime.now()
            logger.info(f"开始更新金融数据，开始时间: {start_time}")
            
            from global_config.stock_info import StockInfo
            import concurrent.futures

            basics = StockInfo.get_all_stocks()    #基础股票代码库
            
            # 从股票基础信息中提取股票代码列表
            stock_codes = [stock.get('code', '') for stock in basics if stock.get('code')]
            
            # 创建保存金融数据的目录
            financial_data_dir = os.path.join(settings.BASE_DIR, 'data', 'financial_data')
            os.makedirs(financial_data_dir, exist_ok=True)
            
            # 检查已下载的文件，只保留未下载的股票代码
            existing_files = os.listdir(financial_data_dir)
            downloaded_codes = set()
            for file_name in existing_files:
                if file_name.endswith('.csv'):
                    # 提取股票代码（去掉.csv扩展名）
                    code = os.path.splitext(file_name)[0]
                    downloaded_codes.add(code)
            
            # 过滤掉已下载的股票代码
            stock_codes = [code for code in stock_codes if code not in downloaded_codes]
            logger.info(f"已下载{len(downloaded_codes)}只股票的金融数据，本次需要下载{len(stock_codes)}只股票")
            
            # 如果没有需要下载的股票，直接返回
            if not stock_codes:
                return Response({
                    "success": True,
                    "message": "所有股票的金融数据已下载完成，无需更新",
                    "total_stocks": 0,
                    "successful_stocks": 0,
                    "total_records": 0,
                    "time_cost": 0
                })
            
            # 更新全局状态
            financial_update_status = {
                "running": True,
                "paused": False,
                "total_count": len(stock_codes),
                "updated_count": 0,
                "status": "running",
                "last_update_time": start_time.isoformat(),
                "current_code": "-"
            }
            
            def process_stock(code):
                """
                处理单个股票的金融数据
                返回处理结果：(成功标志, 记录数, 错误信息)
                """
                global financial_update_status
                try:
                    # 更新当前处理的股票代码
                    financial_update_status["current_code"] = code
                    
                    logger.info(f"线程开始处理股票{code}的金融数据")
                    
                    # 每个线程创建自己的AkshareFetcher实例
                    fetcher = AkshareFetcher()
                    
                    # 调用fetch_stock_financial_data方法获取并处理金融数据
                    df = fetcher.fetch_stock_financial_data(code)
                    
                    if not df.empty:
                        # 保存处理后的数据到文件，文件名就是股票代码
                        csv_file_path = os.path.join(financial_data_dir, f"{code}.csv")
                        df.to_csv(csv_file_path, index=False, encoding='utf-8')
                        logger.info(f"线程成功处理并保存股票{code}的金融数据到{csv_file_path}，共{len(df)}条记录")
                        # 更新已处理股票数量
                        financial_update_status["updated_count"] += 1
                        return (True, len(df), None)
                    # 即使没有获取到数据，也更新已处理股票数量
                    financial_update_status["updated_count"] += 1
                    return (False, 0, "没有获取到有效数据")
                except Exception as e:
                    error_msg = f"处理股票{code}的金融数据失败: {str(e)}"
                    logger.error(error_msg)
                    traceback.print_exc()
                    # 即使处理失败，也更新已处理股票数量
                    financial_update_status["updated_count"] += 1
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
            
            # 更新全局状态为已完成
            financial_update_status = {
                "running": False,
                "paused": False,
                "total_count": len(stock_codes),
                "updated_count": len(stock_codes),
                "status": "completed",
                "last_update_time": end_time.isoformat(),
                "current_code": "-"
            }
            
            logger.info(f"金融数据更新完成，共处理{len(stock_codes)}只股票，成功{successful_stocks}只，保存{total_processed_records}条记录，耗时{time_cost:.2f}秒")
            
            # 构建响应
            response_data = {
                "success": True,
                "message": f"金融数据更新完成，共处理{len(stock_codes)}只股票，成功{successful_stocks}只，保存{total_processed_records}条记录",
                "total_stocks": len(stock_codes),
                "successful_stocks": successful_stocks,
                "total_records": total_processed_records,
                "time_cost": time_cost
            }
            
            if errors:
                response_data["errors"] = errors
            
            return Response(response_data)
            
        except Exception as e:
            # 更新全局状态为失败
            financial_update_status = {
                "running": False,
                "paused": False,
                "total_count": 0,
                "updated_count": 0,
                "status": "failed",
                "last_update_time": datetime.now().isoformat(),
                "current_code": "-"
            }
            
            logger.error(f"更新金融数据失败: {str(e)}")
            logger.error(traceback.format_exc())
            return Response({
                "success": False,
                "message": f"更新金融数据失败: {str(e)}",
                "error": str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class FinancialDataStatusView(APIView):
    """
    获取金融数据更新状态API视图
    调用方式: GET /api/stocks/update/financial_data/status
    功能: 获取金融数据更新的状态信息
    """
    
    def get(self, request):
        global financial_update_status
        try:
            # 返回真实的全局状态
            return Response({
                "success": True,
                "data": {
                    "running": financial_update_status["running"],
                    "paused": financial_update_status["paused"],
                    "total_count": financial_update_status["total_count"],
                    "updated_count": financial_update_status["updated_count"],
                    "status": financial_update_status["status"],
                    "last_update_time": financial_update_status["last_update_time"],
                    "current_code": financial_update_status["current_code"]
                }
            })
            
        except Exception as e:
            logger.error(f"获取金融数据更新状态失败: {str(e)}")
            logger.error(traceback.format_exc())
            return Response({
                "success": False,
                "message": f"获取金融数据更新状态失败: {str(e)}",
                "error": str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class FinancialDataPauseView(APIView):
    """
    暂停金融数据更新API视图
    调用方式: POST /api/stocks/update/financial_data/pause
    功能: 暂停金融数据更新任务
    """
    
    def post(self, request):
        try:
            # 这里模拟暂停逻辑
            logger.info("暂停金融数据更新")
            return Response({
                "success": True,
                "message": "金融数据更新已暂停",
                "paused": True
            })
        except Exception as e:
            logger.error(f"暂停金融数据更新失败: {str(e)}")
            logger.error(traceback.format_exc())
            return Response({
                "success": False,
                "message": f"暂停金融数据更新失败: {str(e)}",
                "error": str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class FinancialDataResumeView(APIView):
    """
    恢复金融数据更新API视图
    调用方式: POST /api/stocks/update/financial_data/resume
    功能: 恢复金融数据更新任务
    """
    
    def post(self, request):
        try:
            # 这里模拟恢复逻辑
            logger.info("恢复金融数据更新")
            return Response({
                "success": True,
                "message": "金融数据更新已恢复",
                "paused": False
            })
        except Exception as e:
            logger.error(f"恢复金融数据更新失败: {str(e)}")
            logger.error(traceback.format_exc())
            return Response({
                "success": False,
                "message": f"恢复金融数据更新失败: {str(e)}",
                "error": str(e)
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
