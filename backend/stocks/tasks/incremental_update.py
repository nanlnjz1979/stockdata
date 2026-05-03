import os
import json
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta

from .base import BaseTask
# 复用现有的数据处理和写入函数
from .download_daily import _insert_daily
from backend.global_config.utils import is_all_holiday, save_to_csv
from backend.global_config.file_config import FileConfig
# 导入全局路径工具
from stocks.utils import normalize_path, join_path, safe_join
# 依赖：Akshare 数据源
try:
    from backend.global_config.data_fetch import AkshareFetcher
except Exception:
    AkshareFetcher = None

logger = logging.getLogger(__name__)

#得到一个股票的最后更新日期
def _get_last_update_date(code: str, conn=None) -> Optional[datetime]:
    """
    从stock_daily表获取指定股票代码的最后更新日期。
    如果没有找到数据，则返回None。
    """
    if not conn:
        return None
    try:
        # 使用ClickHouse客户端直接执行，不需要cursor
        result = conn.execute(
            f"SELECT max(date) as last_date FROM stock_daily WHERE code = '{code}'"
        )
        row = result[0]
        if row and row[0]:
            # 确保返回datetime对象
            last_date = row[0]
            if isinstance(last_date, str):
                try:
                    last_date = datetime.strptime(last_date, '%Y-%m-%d')
                except Exception:
                    return None
            return last_date
    except Exception as e:
        logger.warning("获取最后更新日期失败: code=%s, error=%s", code, e)
    return None


def _save_to_database(code: str, df, conn=None):
    """
    封装数据库保存逻辑
    
    Args:
        code: 股票代码
        df: 日线数据DataFrame
        conn: 数据库连接对象
        
    Returns:
        bool: 是否保存成功
    """
    try:
        # 调用现有的插入函数
        _insert_daily(code, df, conn=conn)
        logger.info("成功将股票 %s 的日线数据插入数据库", code)
        return True
    except Exception as e:
        logger.exception("插入日线数据时发生异常: %s, 错误: %s", code, e)
        return False


def _get_all_stocks_last_date(conn=None) -> Dict[str, datetime]:
    """
    获取所有股票的最后交易日日期。
    使用ClickHouse的GROUP BY和MAX语法高效获取每个股票代码的最新交易日期。
    
    Args:
        conn: 数据库连接对象
        
    Returns:
        Dict[str, datetime]: 股票代码到最后交易日日期的映射字典
    """
    result = {}
    if not conn:
        return result
    
    try:
        # 使用ClickHouse客户端直接执行，不需要cursor
        # 使用GROUP BY和MAX获取每个股票的最新交易日期
        result_set = conn.execute(
            """
            SELECT code, MAX(date) as last_date 
            FROM stock_daily_v
            GROUP BY code
            """
        )
        
        # 处理查询结果
        for row in result_set:
            if row and len(row) >= 2 and row[0]:
                code = row[0]
                last_date = row[1]
                
                # 确保last_date是datetime对象
                if isinstance(last_date, str):
                    try:
                        last_date = datetime.strptime(last_date, '%Y-%m-%d')
                    except Exception:
                        # 解析失败则跳过该记录
                        continue
                
                result[code] = last_date
        
        logger.info(f"成功获取{len(result)}只股票的最后交易日日期")
    except Exception as e:
        logger.error(f"获取所有股票最后交易日失败: {e}")
    
    return result


def _get_all_stocks_last_date_cvs() -> Dict[str, datetime]:
    """
    从CSV文件中获取所有股票的最后交易日日期。
    扫描data/daily目录下的CSV文件，获取每个股票的最新交易日期。
    
    Returns:
        Dict[str, datetime]: 股票代码到最后交易日日期的映射字典
    """
    import time
    # 记录函数开始执行时间
    start_time = time.time()
    
    result = {}
    import pandas as pd
    import glob
    
    # 构建CSV文件目录路径
    csv_dir = join_path(os.path.dirname(__file__), '../../data/daily')
    
    try:
        # 检查目录是否存在
        if not os.path.exists(csv_dir):
            logger.warning(f"CSV文件目录不存在: {csv_dir}")
            return result
        
        # 查找所有CSV文件
        csv_files = glob.glob(os.path.join(csv_dir, '*.csv'))
        
        if not csv_files:
            logger.info(f"没有找到CSV文件: {csv_dir}")
            return result
        
        logger.info(f"找到{len(csv_files)}个CSV文件")
        
        # 遍历每个CSV文件
        for csv_file in csv_files:
            try:
                # 从文件名提取股票代码（假设文件名格式为：股票代码_复权类型.csv）
                filename = os.path.basename(csv_file)
                # 提取股票代码，忽略复权类型部分
                code_parts = filename.split('_')
                if not code_parts:
                    continue
                code = code_parts[0].replace('.csv', '')
                
                # 如果已经处理过该股票，跳过
                if code in result:
                    continue
                
                # 读取CSV文件，只读取date列以提高效率
                df = pd.read_csv(csv_file, usecols=['date'])
                
                if df.empty:
                    continue
                logger.info(f"处理CSV文件: {filename}, 股票代码: {code}, 记录数: {len(df)}")
                
                # 获取最大的交易日期
                max_date_str = df['date'].max()
                
                if pd.notna(max_date_str):
                    try:
                        # 将字符串转换为datetime对象
                        # 尝试不同的日期格式
                        date = None
                        date_formats = ['%Y-%m-%d', '%Y%m%d', '%Y-%m-%dT%H:%M:%S.%fZ']
                        for fmt in date_formats:
                            try:
                                date = datetime.strptime(str(max_date_str), fmt)
                                break
                            except ValueError:
                                continue
                        
                        # 如果标准格式解析失败，尝试处理ISO格式（去掉Z后缀）
                        if date is None:
                            try:
                                if str(max_date_str).endswith('Z'):
                                    # 处理ISO格式的日期字符串，去掉Z并使用fromisoformat
                                    iso_date_str = str(max_date_str)[:-1]
                                    date = datetime.fromisoformat(iso_date_str)
                            except Exception:
                                pass
                        
                        if date:
                            result[code] = date
                    except Exception as e:
                        logger.warning(f"解析日期失败: {max_date_str}, 文件: {filename}, 错误: {e}")
            
            except Exception as e:
                logger.warning(f"处理CSV文件失败: {csv_file}, 错误: {e}")
        
        logger.info(f"从CSV文件成功获取{len(result)}只股票的最后交易日日期")
    except Exception as e:
        logger.error(f"从CSV文件获取所有股票最后交易日失败: {e}")
    
    # 记录函数执行完成时间
    end_time = time.time()
    execution_time = end_time - start_time
    logger.info(f"_get_all_stocks_last_date_cvs函数执行完成，耗时: {execution_time:.2f}秒")
    print(f"_get_all_stocks_last_date_cvs函数执行完成，耗时: {execution_time:.2f}秒")
    
    return result


class IncrementalUpdateTask(BaseTask):
    """
    增量更新任务：根据stock_daily表中已有的数据，只更新最后日期到今天的数据。
    
    params 支持：
      - code: 单个股票代码（字符串，必填）
      - codes: 多个股票代码（列表，可选，与code二选一）
      - market: 市场标识（可选，'SH'/'SZ'/'BJ'，默认会尝试从数据库获取）
      - adjust: 复权类型（可选，''/空表示不复权，'qfq'前复权，'hfq'后复权，'all'全部）
    
    执行逻辑：
    1. 检查指定股票代码在stock_daily表中的最后更新日期
    2. 如果有最后更新日期，则从该日期的次日开始拉取数据
    3. 如果没有历史数据，则从较早日期开始拉取（可配置默认起始日期）
    4. 使用ak.stock_zh_a_hist获取数据并写入QuestDB
    """

    def __init__(self, orm = None):
        # 仅要求传入orm，其余字段在generate()时设置
        super().__init__(orm, task_type="", task_desc="", params=None, priority=0)

    def generate(self, task_type: str, task_desc: str = "", params: Optional[Dict[str, Any]] = None, priority: int = 1,conn=None) -> str:
        # 在生成前配置必要字段
        self.task_type = task_type
        self.task_desc = task_desc
        self.params_str = self._ensure_json_str(params)
        self.priority = priority

        # 获取所有股票的最后交易日日期
        from backend.global_config.stock_info import StockInfo
        if FileConfig.get('data_source') == 'csv' :
            last_dates = _get_all_stocks_last_date_cvs()

            listing_date = StockInfo.get_all_stocks()

            # 获取所有股票的最后交易日日期
            # 将listing_date中存在但last_dates中不存在的股票补充进去
            for code,x,xx, list_date in listing_date.items():
                if code not in last_dates:
                    last_dates[code] = list_date
        else:
            last_dates = _get_all_stocks_last_date(conn=conn)
        
        # 循环生成每个股票的增量更新任务
        task_ids = []
        for code, last_date in last_dates.items():
            
            # 计算起始日期（最后交易日的次日）
            if last_date:
                start_date = (last_date + timedelta(days=1)).strftime('%Y%m%d')
            else:
                # 如果没有历史数据，使用默认起始日期
                start_date = '20200101'

            # 今天的日期作为结束日期
            end_date = datetime.now().strftime('%Y%m%d')

            # 如果起始日期已经是今天或未来，跳过
            if start_date > end_date:
                logger.info("股票 %s 已是最新数据，跳过生成任务", code)
                continue
            
            # 判断起止日期是否均为非交易日，若是则跳过
            if is_all_holiday(start_date, end_date):
                logger.info("股票 %s 的起止日期(%s~%s)均为非交易日，跳过生成任务", code, start_date, end_date)
                continue

            # 构造任务参数
            task_params = {
                'code': code,
                'start_date': start_date,
                'end_date': end_date,
                'adjust': 'all'   # 默认不复权
            }

           
        
            self.task_type = task_type
            self.task_desc = f"增量更新股票 {code} 从 {start_date} 到 {end_date}",
            self.priority = priority
            self.params_str = self._ensure_json_str(task_params)
            self.priority=priority
            # 生成任务ID并记录
            
            task_id = super().generate()
            task_ids.append(task_id)
            

        logger.info("成功生成 %d 个增量更新任务", len(task_ids))
        return ""


    def _parse_params(self) -> Dict[str, Any]:
        try:
            return json.loads(self.params_str or '{}')
        except Exception:
            return {}

    @classmethod
    def taskID(cls) -> str:
        return f"STOCK_Update"
    def run(self, conn=None, params_str: str = None) -> bool:
        # 检查参数
        if params_str:
            self.params_str = params_str
        params = self._parse_params()
        
        # 读取是否保存到文件的配置
        to_csv = FileConfig.get("to_csv", False)
        logger.info(f"任务配置：保存到CSV文件 = {to_csv}")
        
        code = params.get('code')
        start_date = params.get('start_date')
        end_date = params.get('end_date')
        adjust = params.get('adjust', 'all')

        if not code or not start_date or not end_date:
            logger.error("增量更新任务缺少必要参数: %s", params)
            return False

        # 如果不保存到CSV文件，则检查数据库连接
        if not to_csv:
            conn_local = conn 
            if not conn_local:
                logger.error("数据库连接失败")
                return False
        else:
            conn_local = None

        try:
            # 用于存储结果
            result_dict = {
                'total_saved': 0,
                'success_adj_types': [],
                'failed_adj_types': [],
                'total_rows': 0
            }
            
            # 使用 akshare 获取日线数据
            adjust_all = ['', 'qfq', 'hfq'] if adjust == 'all' else [adjust]

            # 初始化AkshareFetcher
            fetcher = AkshareFetcher()
            
            # 收集所有复权类型的数据
            collected_data = []
            
            # 第一阶段：收集所有数据
            for adj in adjust_all:
                try:
                    # 使用AkshareFetcher获取日线数据
                    df = fetcher.fetch_stock_daily(
                        code=code,
                        start_date=start_date,
                        end_date=end_date,
                        adjust=adj
                    )
                    if df is None or df.empty:
                        logger.info("股票 %s 在 %s~%s 无数据，跳过", code, start_date, end_date)
                        continue
                    
                    # 将数据添加到收集列表中
                    collected_data.append((code, df, adj))

                    logger.info("已收集数据: %s [%s ~ %s] adjust=%s, 行数: %d", code, start_date, end_date, adj, len(df))
                    result_dict['total_rows'] += len(df)
                except Exception as e:
                    logger.exception("数据获取失败: code=%s, adj=%s, error=%s", code, adj, e)
                    result_dict['failed_adj_types'].append((adj, str(e)))
            
            # 如果没有收集到任何数据，返回成功
            if not collected_data:
                logger.info("未收集到任何数据: %s [%s ~ %s]", code, start_date, end_date)
                return True
            
            # 根据配置决定保存方式：互斥保存
            if to_csv:
                # 只保存到文件
                file_name = join_path(os.path.dirname(__file__), '..', '..', 'data', 'daily_append')
                logger.info("开始保存数据到文件...")
                all_data = []
                for code_data, df_data, adj_data in collected_data:
                    temp_df = df_data.copy()
                    # 移除adjust_type列处理，因为数据库已移除该字段
                    all_data.append(temp_df)
                try:
                    import pandas as pd
                    combined_df = pd.concat(all_data, ignore_index=True)
                    saved_count = save_to_csv(code_data, combined_df, None, file_name=file_name)
                    result_dict['total_saved'] += saved_count
                    logger.info("已保存数据到文件: %s, 行数: %d", code_data, saved_count)
                except Exception as e:
                    logger.exception("保存到文件失败: code=%s, error=%s", code_data, e)
            else:
                # 只保存到数据库
                logger.info("开始保存数据到数据库...")
                
                for code_data, df_data, adj_data in collected_data:
                    try:
                        db_success = _save_to_database(code_data, df_data, conn=conn_local)
                        if db_success:
                            result_dict['total_saved'] += len(df_data)
                            logger.info("成功保存到数据库: %s, 行数: %d", code_data, len(df_data))
                        else:
                            logger.error("保存数据库失败: %s", code_data)
                    except Exception as e:
                        logger.exception("保存到数据库异常: code=%s, error=%s", code_data, e)
            
            # 记录结果
            total_saved = result_dict['total_saved']
            
            save_type = "CSV文件" if to_csv else "数据库"
            logger.info("任务完成：code=%s, 保存到=%s, total_saved=%s", 
                      code, save_type, total_saved)
            
            return total_saved > 0
                
        except Exception as e:
            logger.exception("增量更新过程中发生异常: %s [%s ~ %s], error: %s", code, start_date, end_date, e)
            return False
        finally:
            # 只有在保存到数据库且连接是内部创建时才需要关闭连接
            if not to_csv and conn is None and conn_local:
                try:
                    conn_local.close()
                except Exception:
                    pass
            
        return False
          