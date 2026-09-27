import os
import json
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta

from .base import BaseTask
# 复用现有的数据处理和写入函数
from .download_daily import _insert_daily
from stocks.price_scale import frame_first_close, is_amplified, select_unadjusted_frame
from backend.global_config.utils import is_all_holiday, norm_date, save_to_csv
from backend.global_config.file_config import FileConfig
# 导入全局路径工具
from stocks.utils import normalize_path, join_path, safe_join
# 依赖：Akshare 数据源
try:
    from backend.global_config.data_fetch import AkshareFetcher, format_fetch_error
except Exception:
    AkshareFetcher = None

    def format_fetch_error(exc, max_length: int = 240):
        text = str(exc or "")
        return text[:max_length - 3] + "..." if len(text) > max_length else text

logger = logging.getLogger(__name__)


def _previous_close(code, start_date, conn):
    """Return the stored close of the trading day before this batch."""
    if conn is None or not code or not str(code).isalnum():
        return None
    normalized = norm_date(start_date)
    if not normalized or len(normalized) != 8 or not normalized.isdigit():
        return None
    day = f"{normalized[:4]}-{normalized[4:6]}-{normalized[6:8]}"
    try:
        rows = conn.execute(
            "SELECT close FROM stock_daily "
            f"WHERE code = '{code}' AND date < toDate('{day}') "
            "ORDER BY date DESC LIMIT 1"
        )
        if rows and rows[0] and rows[0][0] is not None:
            return float(rows[0][0])
    except Exception as exc:
        logger.warning(
            "读取上一交易日收盘价失败 code=%s before=%s error=%s",
            code,
            day,
            format_fetch_error(exc),
        )
    return None


def _reject_amplified_daily(fetcher, code, start_date, end_date, primary, conn):
    """Refuse a batch whose first close is on a different scale from stored history.

    The other AkShare source is requested only after the first frame is amplified.
    A matching alternate is stored; two amplified frames, or no alternate, are dropped.
    """
    previous = _previous_close(code, start_date, conn)
    if not is_amplified(frame_first_close(primary), previous):
        return primary
    used = getattr(fetcher, "last_daily_source", None)
    other = "stock_zh_a_hist" if used == "stock_zh_a_daily" else "stock_zh_a_daily"
    alternate = None
    try:
        alternate = fetcher.fetch_stock_daily(
            code=code,
            start_date=start_date,
            end_date=end_date,
            adjust="",
            source=other,
        )
    except Exception as exc:
        logger.warning(
            "[增量任务执行] 备用数据源失败 code=%s source=%s error=%s",
            code,
            other,
            format_fetch_error(exc),
        )
    chosen = select_unadjusted_frame(primary, alternate, previous)
    if chosen is None:
        logger.error(
            "[增量任务执行] 价格尺度异常，拒绝入库 code=%s range=%s~%s previous=%s primary=%s alternate=%s",
            code,
            start_date,
            end_date,
            previous,
            frame_first_close(primary),
            frame_first_close(alternate),
        )
        return None
    logger.warning(
        "[增量任务执行] 主数据源价格尺度异常，改用 %s code=%s previous=%s",
        other,
        code,
        previous,
    )
    return chosen


def _task_params_key(params: Any) -> str:
    """Build a stable comparison key for task params regardless of JSON field order."""
    if isinstance(params, str):
        try:
            params = json.loads(params)
        except Exception:
            return params
    if isinstance(params, dict):
        # 增量任务当前只保存不复权行情；将历史 all/qfq/hfq 参数归一，
        # 防止旧任务与新任务被当成不同参数重复入队。
        params = dict(params)
        params['adjust'] = ''
    try:
        return json.dumps(params or {}, ensure_ascii=False, sort_keys=True)
    except Exception:
        return str(params)

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
        logger.debug("成功将股票 %s 的日线数据插入数据库", code)
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
        # 直接查基表 stock_daily，避免对全表做不必要的视图聚合
        # WHERE date >= today() - 3650 限制只扫最近10年的分区，覆盖全部活跃股票
        result_set = conn.execute(
            """
            SELECT code, MAX(date) as last_date 
            FROM stock_daily
            WHERE date >= today() - 3650
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
        
        logger.info("[增量任务生成] 已读取最新交易日 code_count=%d", len(result))
    except Exception as e:
        logger.error(f"获取所有股票最后交易日失败: {e}")
    
    return result


def _get_all_stocks_last_date_cvs() -> Dict[str, datetime]:
    """
    从CSV文件中获取所有股票的最后交易日日期。
    同时扫描data/daily和data/daily_append目录，取每个股票的最新交易日期。
    
    Returns:
        Dict[str, datetime]: 股票代码到最后交易日日期的映射字典
    """
    import time
    # 记录函数开始执行时间
    start_time = time.time()
    
    result = {}
    import pandas as pd
    import glob

    csv_dirs = [
        join_path(os.path.dirname(__file__), '../../data/daily'),
        join_path(os.path.dirname(__file__), '../../data/daily_append'),
    ]

    def parse_csv_date(value) -> Optional[datetime]:
        if pd.isna(value):
            return None
        value_str = str(value)
        date_formats = ['%Y-%m-%d', '%Y%m%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S.%fZ']
        for fmt in date_formats:
            try:
                return datetime.strptime(value_str, fmt)
            except ValueError:
                continue
        try:
            if value_str.endswith('Z'):
                return datetime.fromisoformat(value_str[:-1])
            return datetime.fromisoformat(value_str)
        except Exception:
            return None
    
    try:
        total_files = 0
        for csv_dir in csv_dirs:
            if not os.path.exists(csv_dir):
                logger.warning("CSV文件目录不存在: %s", csv_dir)
                continue

            csv_files = glob.glob(os.path.join(csv_dir, '*.csv'))
            total_files += len(csv_files)
            logger.debug("[增量任务生成] 扫描CSV目录 dir=%s file_count=%d", csv_dir, len(csv_files))

            for csv_file in csv_files:
                try:
                    filename = os.path.basename(csv_file)
                    code = filename.replace('.csv', '')

                    df = pd.read_csv(csv_file, usecols=['date'])

                    if df.empty:
                        continue
                    logger.debug("处理CSV文件: %s, 股票代码: %s, 记录数: %d", filename, code, len(df))

                    max_date = parse_csv_date(df['date'].max())
                    if not max_date:
                        logger.warning("解析日期失败: %s, 文件: %s", df['date'].max(), filename)
                        continue

                    if code not in result or max_date > result[code]:
                        result[code] = max_date

                except Exception as e:
                    logger.warning(f"处理CSV文件失败: {csv_file}, 错误: {e}")

        if total_files == 0:
            logger.info("[增量任务生成] 未找到CSV文件 dirs=%s", csv_dirs)

        logger.info("[增量任务生成] 已从CSV读取最新交易日 code_count=%d file_count=%d", len(result), total_files)
    except Exception as e:
        logger.error(f"从CSV文件获取所有股票最后交易日失败: {e}")
    
    # 记录函数执行完成时间
    end_time = time.time()
    execution_time = end_time - start_time
    logger.debug("_get_all_stocks_last_date_cvs 执行完成 elapsed=%.2fs", execution_time)
    
    return result


class IncrementalUpdateTask(BaseTask):
    """
    增量更新任务：根据stock_daily表中已有的数据，只更新最后日期到今天的数据。
    
    params 支持：
      - code: 单个股票代码（字符串，必填）
      - codes: 多个股票代码（列表，可选，与code二选一）
      - market: 市场标识（可选，'SH'/'SZ'/'BJ'，默认会尝试从数据库获取）
      - adjust: 兼容历史参数；当前增量任务始终只下载不复权行情
    
    执行逻辑：
    1. 检查指定股票代码在stock_daily表中的最后更新日期
    2. 如果有最后更新日期，则从该日期的次日开始拉取数据
    3. 如果没有历史数据，则从较早日期开始拉取（可配置默认起始日期）
    4. 使用ak.stock_zh_a_hist获取数据并写入QuestDB
    """

    def __init__(self, orm = None):
        # 仅要求传入orm，其余字段在generate()时设置
        super().__init__(orm, task_type="", task_desc="", params=None, priority=0)

    def generate(self, task_type: str, task_desc: str = "", params: Optional[Dict[str, Any]] = None, priority: int = 1,conn=None) -> Dict[str, int]:
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
            for stock in listing_date:
                code = stock.get('code')
                list_date = stock.get('listing_date')
                if code and code not in last_dates and list_date:
                    # 确保list_date是datetime对象
                    if isinstance(list_date, str):
                        try:
                            list_date = datetime.strptime(list_date.split(' ')[0], '%Y-%m-%d')
                        except:
                            try:
                                list_date = datetime.strptime(list_date, '%Y%m%d')
                            except:
                                list_date = datetime(2020, 1, 1)
                    last_dates[code] = list_date
        else:
            last_dates = _get_all_stocks_last_date(conn=conn)

        active_task_param_keys = set()
        if hasattr(self.orm, 'list_tasks'):
            for task_status in ('待处理', '处理中'):
                try:
                    for task in self.orm.list_tasks(status=task_status, task_type=task_type, limit=100000):
                        active_task_param_keys.add(_task_params_key(task.get('task_params')))
                except Exception as e:
                    logger.warning("查询已有增量更新任务失败: status=%s, error=%s", task_status, e)
        
        logger.info("[增量任务生成] 开始生成 task_type=%s code_count=%d", task_type, len(last_dates))

        # 循环生成每个股票的增量更新任务
        task_ids = []
        skip_no_range = 0
        skip_non_trading = 0
        skip_duplicate = 0
        for code, last_date in last_dates.items():
            
            # 计算起始日期（最后交易日的次日）
            if last_date:
                start_date = (last_date + timedelta(days=1)).strftime('%Y%m%d')
            else:
                # 如果没有历史数据，使用默认起始日期
                start_date = '20200101'

            # 今天的日期作为结束日期
            end_date = datetime.now().strftime('%Y%m%d')

            # 起止日期相同表示没有可生成的增量区间，避免创建空跑任务。
            if start_date >= end_date:
                skip_no_range += 1
                logger.debug("股票 %s 无新增日期区间(%s~%s)，跳过生成任务", code, start_date, end_date)
                continue
            
            # 判断起止日期是否均为非交易日，若是则跳过
            if is_all_holiday(start_date, end_date):
                skip_non_trading += 1
                logger.debug("股票 %s 的起止日期(%s~%s)均为非交易日，跳过生成任务", code, start_date, end_date)
                continue

            # 构造任务参数
            task_params = {
                'code': code,
                'start_date': start_date,
                'end_date': end_date,
                'adjust': ''       # stock_daily 只保存不复权行情
            }

            params_str = self._ensure_json_str(task_params)
            params_key = _task_params_key(params_str)
            if params_key in active_task_param_keys:
                skip_duplicate += 1
                logger.debug("股票 %s 已存在相同增量更新任务(%s~%s)，跳过生成任务", code, start_date, end_date)
                continue

           
        
            self.task_type = task_type
            self.task_desc = f"增量更新股票 {code} 从 {start_date} 到 {end_date}"
            self.priority = priority
            self.params_str = params_str
            self.priority = priority
            # 生成任务ID并记录
            
            task_id = super().generate()
            task_ids.append(task_id)
            active_task_param_keys.add(params_key)
            logger.debug("[增量任务生成] 入队 code=%s task_id=%s range=%s~%s", code, task_id, start_date, end_date)
            
        logger.info(
            "[增量任务生成] 完成 generated=%d skipped_no_range=%d skipped_non_trading=%d skipped_duplicate=%d total=%d",
            len(task_ids),
            skip_no_range,
            skip_non_trading,
            skip_duplicate,
            len(last_dates),
        )
        return {
            "generated": len(task_ids),
            "skipped_no_range": skip_no_range,
            "skipped_non_trading": skip_non_trading,
            "skipped_duplicate": skip_duplicate,
            "total": len(last_dates),
        }


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
        code = params.get('code')
        start_date = params.get('start_date')
        end_date = params.get('end_date')
        # stock_daily 没有复权类型维度，增量任务统一只抓取不复权行情。
        # 兼容已经入队的旧任务，避免 adjust=all 再展开为三次请求。
        adjust = ''

        save_type = "CSV文件" if to_csv else "数据库"
        logger.debug("[增量任务执行] 开始 code=%s range=%s~%s adjust=%s save_to=%s", code, start_date, end_date, adjust, save_type)

        if not code or not start_date or not end_date:
            logger.error("增量更新任务缺少必要参数: %s", params)
            return False

        normalized_start_date = norm_date(start_date)
        normalized_end_date = norm_date(end_date)
        if normalized_start_date and normalized_end_date and normalized_start_date == normalized_end_date:
            logger.debug(
                "[增量任务执行] 无新增日期区间，直接标记成功 code=%s range=%s~%s",
                code,
                start_date,
                end_date,
            )
            return True

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
            
            # 使用 akshare 获取不复权日线数据；复权行情由 fq_factor 视图计算。
            adjust_all = ['']

            # 初始化AkshareFetcher
            fetcher = AkshareFetcher()
            
            # 收集不复权行情数据
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
                        logger.debug("股票 %s 在 %s~%s 无数据，跳过 adjust=%s", code, start_date, end_date, adj)
                        continue

                    df = _reject_amplified_daily(
                        fetcher,
                        code,
                        start_date,
                        end_date,
                        df,
                        conn_local,
                    )
                    if df is None or df.empty:
                        result_dict['failed_adj_types'].append((adj, "price scale"))
                        continue

                    # 将数据添加到收集列表中
                    collected_data.append((code, df, adj))

                    logger.debug("已收集数据: %s [%s ~ %s] adjust=%s, 行数: %d", code, start_date, end_date, adj, len(df))
                    result_dict['total_rows'] += len(df)
                except Exception as e:
                    error_summary = format_fetch_error(e)
                    logger.warning(
                        "[增量任务执行] 数据获取失败 code=%s range=%s~%s adjust=%s error=%s",
                        code,
                        start_date,
                        end_date,
                        adj or '-',
                        error_summary,
                    )
                    result_dict['failed_adj_types'].append((adj, error_summary))

            # 如果没有收集到任何数据，返回成功
            if not collected_data:
                logger.debug("[增量任务执行] 无数据 code=%s range=%s~%s", code, start_date, end_date)
                return True

            # 根据配置决定保存方式：互斥保存
            if to_csv:
                # 只保存到文件
                csv_dir = join_path(os.path.dirname(__file__), '..', '..', 'data', 'daily_append')
                logger.debug("开始保存数据到文件: %s", csv_dir)

                # 分别保存每个股票的数据
                for code_data, df_data, adj_data in collected_data:
                    try:
                        saved_count = save_to_csv(code_data, df_data, file_name=csv_dir)
                        result_dict['total_saved'] += saved_count
                        logger.debug("已保存数据到文件: %s, 行数: %d", code_data, saved_count)
                    except Exception as e:
                        logger.error("保存到文件失败: code=%s, error=%s", code_data, format_fetch_error(e))
            else:
                # 只保存到数据库
                logger.debug("开始保存数据到数据库")

                for code_data, df_data, adj_data in collected_data:
                    try:
                        db_success = _save_to_database(code_data, df_data, conn=conn_local)
                        if db_success:
                            result_dict['total_saved'] += len(df_data)
                            logger.debug("成功保存到数据库: %s, 行数: %d", code_data, len(df_data))
                        else:
                            logger.error("保存数据库失败: %s", code_data)
                    except Exception as e:
                        logger.error("保存到数据库异常: code=%s, error=%s", code_data, format_fetch_error(e))

            # 记录结果
            total_saved = result_dict['total_saved']

            logger.debug(
                "[增量任务执行] 完成 code=%s range=%s~%s fetched_rows=%d saved_rows=%s failed_adjust=%d save_to=%s",
                code,
                start_date,
                end_date,
                result_dict['total_rows'],
                total_saved,
                len(result_dict['failed_adj_types']),
                save_type,
            )

            return total_saved > 0

        except Exception as e:
            logger.error("增量更新过程中发生异常: code=%s range=%s~%s error=%s", code, start_date, end_date, format_fetch_error(e))
            return False
        finally:
            # 只有在保存到数据库且连接是内部创建时才需要关闭连接
            if not to_csv and conn is None and conn_local:
                try:
                    conn_local.close()
                except Exception:
                    pass

        return False
