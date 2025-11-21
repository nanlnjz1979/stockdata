import pandas as pd
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.utils import timezone
from backend.db import get_conn, put_conn
from stocks.models import StockBasic, StockDaily
from django.db import connection
from datetime import datetime, timedelta
from backend.global_config.data_fetch import is_trading_day, DataFetchError


from backend.global_config.stock_info import StockInfo
class DataIntegrityCheckView(APIView):
    def get(self, request):
        """获取数据完整性的统计信息 - 从QuestDB数据库中获取数据，使用数据库连接池"""
        conn = None
        try:
            # 从数据库连接池获取连接
            conn = get_conn()
            
            if not conn:
                return Response({
                    'error': '无法获取数据库连接'
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
            with conn.cursor() as cursor:
                # 1. 查询股票代码总数（只计算个数，不获取具体代码）
                total_stocks = StockInfo.get_stock_count()  # 股票代码个数保存在total_stocks
                
                # 2. 查询有日数据的股票代码列表（从数据库中获取实际有数据的股票）
                cursor.execute("SELECT DISTINCT code FROM stock_daily")
                stocks_with_data = [row[0] for row in cursor.fetchall()]  # 从数据库中获取有数据的股票代码
                stocks_with_data_count = len(stocks_with_data)  # 有数据的股票数量
                
                # 3. 计算缺失的股票数量和完整性百分比
                missing_stocks = total_stocks - stocks_with_data_count
                integrity_percentage = round((stocks_with_data_count / total_stocks * 100) if total_stocks > 0 else 0, 2)
                
                # 只返回指定的四个字段，注意stocks_with_data字段返回的是数量而不是列表
                return Response({
                    'total': total_stocks,
                    'missing': missing_stocks,
                    'complete': f"{integrity_percentage}%",
                    'stocks_with_data': stocks_with_data
                })
        
        except Exception as e:
            return Response({
                'error': f'完整性检查失败: {str(e)}'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            # 确保连接被正确归还到连接池
            if conn:
                put_conn(conn)
    from backend.global_config.stock_info import StockInfo
    def post(self, request):
        """对单个股票进行完整性检查 - 从QuestDB数据库获取数据，使用数据库连接池"""
        # 1. 参数验证
        stock_code = self._validate_stock_code(request)
        if stock_code is None:
            return Response({'error': 'stock_code is required'}, status=status.HTTP_400_BAD_REQUEST)
        
        conn = None
        try:
            # 2. 获取数据库连接
            conn = get_conn()
            if not conn:
                return Response({
                    'error': '无法获取数据库连接'
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
            with conn.cursor() as cursor:
                # 3. 查询股票基本信息
                
                stock_info = StockInfo.get_company_name_by_code(stock_code)
                if not stock_info:
                    return Response({'error': f'Stock {stock_code} not found'}, status=status.HTTP_404_NOT_FOUND)
                
                stock_name = stock_info
                
                # 4. 获取记录总数
                record_count = self._get_record_count(cursor, stock_code)
                
                # 5. 分析完整性问题
                issues, details = self._analyze_integrity_issues(cursor, stock_code, record_count)
                
                # 6. 确定状态
                status_value = self._determine_status(issues, record_count)
                
                # 7. 返回结果
                return self._build_response(stock_code, stock_name, record_count, issues, status_value, details)
        
        except Exception as e:
            return Response({
                'error': f'完整性检查失败: {str(e)}'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            # 确保连接被正确归还到连接池
            if conn:
                put_conn(conn)
    
    def _validate_stock_code(self, request):
        """验证请求中的股票代码参数"""
        return request.data.get('stock_code')
    

    
    def _get_record_count(self, cursor, stock_code):
        """获取股票的日数据记录总数"""
        cursor.execute("SELECT COUNT(*) FROM stock_daily WHERE code = %s", [stock_code])
        return cursor.fetchone()[0]
    
    def _analyze_integrity_issues(self, cursor, stock_code, record_count):
        """分析数据完整性问题，返回问题列表和详细问题记录"""
        issues = []
        details = {
            'date_gaps': [],
            'missing_adjustment': [],
            'old_data_dates': []
        }
        
        # 检查是否有数据缺失
        if record_count == 0:
            issues.append('没有找到日数据记录')
        else:
            # 检查复权数据
            adjustment_issues = self._check_adjustment_data(cursor, stock_code)
            issues.extend(adjustment_issues)
            details['missing_adjustment'] = adjustment_issues
            
            # 检查数据更新时间
            update_issues = self._check_data_update_time(cursor, stock_code)
            issues.extend(update_issues)
            details['old_data_dates'] = update_issues
            
            # 检查日期连续性 - 检测是否缺少交易日数据
            """ #这里不检查数据的完整性了，在下一个阶段处理
            missing_dates = self._check_date_continuity(cursor, stock_code)
            details['date_gaps'] = missing_dates
            if missing_dates:
                issues.append(f'缺失交易日数据: {missing_dates[:5]}...' if len(missing_dates) > 5 else f'缺失交易日数据: {missing_dates}')
            """
        return issues, details
    
    def _check_adjustment_data(self, cursor, stock_code):
        """检查各种复权数据的完整性"""
        issues = []
        
        # 检查前复权数据 (qfq)
        cursor.execute("SELECT COUNT(*) FROM stock_daily WHERE code = %s AND adjust_type = 'qfq'", [stock_code])
        if cursor.fetchone()[0] == 0:
            issues.append('缺少前复权数据')
        
        # 检查后复权数据 (hfq)
        cursor.execute("SELECT COUNT(*) FROM stock_daily WHERE code = %s AND adjust_type = 'hfq'", [stock_code])
        if cursor.fetchone()[0] == 0:
            issues.append('缺少后复权数据')
        
        # 检查不复权数据 (null)
        cursor.execute("SELECT COUNT(*) FROM stock_daily WHERE code = %s AND adjust_type IS NULL", [stock_code])
        if cursor.fetchone()[0] == 0:
            issues.append('缺少不复权数据')
        
        return issues
    
    def _check_data_update_time(self, cursor, stock_code):
        """检查数据更新时间是否及时"""
        issues = []
        
        # 获取最新交易日期
        cursor.execute("SELECT MAX(trade_date) FROM stock_daily WHERE code = %s", [stock_code])
        latest_date = cursor.fetchone()[0]
        
        if latest_date:
            # 计算与当前日期的天数差
            from datetime import datetime, date
            try:
                # 尝试解析日期，处理可能的格式变化
                latest_date_obj = datetime.strptime(str(latest_date), '%Y-%m-%d %H:%M:%S').date()
                days_since_latest = (date.today() - latest_date_obj).days
                
                if days_since_latest > 10:  # 如果超过10天没有更新
                    issues.append(f'数据更新不及时，最近数据日期为{latest_date}')
            except ValueError as e:
                # 如果日期格式不正确，记录为问题
                print(f"异常值: {latest_date}")
                print(f"异常信息: {str(e)}")
                issues.append(f'日期格式错误: {latest_date}')
        
        return issues
    
    def _check_date_continuity(self, cursor, stock_code):
        """检查股票日数据的日期连续性，返回缺失的日期列表"""
        missing_dates = []
        
        # 导入交易日检查函数
        from backend.global_config.data_fetch import is_trading_day, DataFetchError
        
        # 获取该股票的所有交易日期，按日期排序
        cursor.execute("SELECT DISTINCT trade_date FROM stock_daily WHERE code = %s ORDER BY trade_date", [stock_code])
        dates = [row[0] for row in cursor.fetchall()]
        
        if len(dates) <= 1:
            return missing_dates  # 如果只有0或1条记录，无法检测连续性
        
        # 检查相邻日期之间是否有缺失
        from datetime import timedelta, datetime
        for i in range(len(dates) - 1):
            # 确保日期格式一致
            current_date = dates[i]
            next_date = dates[i + 1]
            
            # 处理可能的字符串日期格式
            if isinstance(current_date, str):
                current_date = datetime.strptime(current_date, '%Y-%m-%d %H:%M:%S').date()
            else:
                try:
                    current_date = current_date.date()
                except:
                    continue
            
            if isinstance(next_date, str):
                next_date = datetime.strptime(next_date, '%Y-%m-%d %H:%M:%S').date()
            else:
                try:
                    next_date = next_date.date()
                except:
                    continue
            
            # 计算两个日期之间的天数差
            days_diff = (next_date - current_date).days
            
            # 如果差值大于1天，说明可能有缺失
            # 简化处理：如果差值超过3天（考虑周末和可能的节假日），则认为有缺失
            if days_diff > 2:
                # 计算缺失的日期范围
                missing_start = current_date + timedelta(days=1)
                missing_end = next_date - timedelta(days=1)
                
                # 生成缺失的日期字符串列表
                current_missing = missing_start
                while current_missing <= missing_end:
                    # 首先检查是否为周末
                    if current_missing.weekday() < 5:  # 0=周一, 4=周五, 5=周六, 6=周日
                        try:
                            # 如果不是周末，使用data_fetch.py中的is_trading_day函数判断是否为交易日
                            if is_trading_day(current_missing):
                                missing_dates.append(current_missing.strftime('%Y-%m-%d'))
                        except DataFetchError as e:
                            # 如果调用is_trading_day失败，记录错误并假设非周末即为交易日
                            print(f"检查交易日失败: {str(e)}")
                            missing_dates.append(current_missing.strftime('%Y-%m-%d'))
                    # 周末日期直接跳过，不添加到缺失列表中
                    current_missing += timedelta(days=1)
        
        return missing_dates
    
    def _determine_status(self, issues, record_count):
        """根据问题列表和记录数量确定股票状态"""
        if len(issues) == 0:
            return 'complete'
        elif record_count > 0:
            return 'partial'
        else:
            return 'missing'
    
    def _build_response(self, stock_code, stock_name, record_count, issues, status_value, details=None):
        """构建API响应数据"""
        response_data = {
            'stock_code': stock_code,
            'stock_name': stock_name,
            'total_records': record_count,
            'issues': issues,
            'status': status_value,
            'timestamp': timezone.now().isoformat()
        }
        
        # 如果有详细信息，添加到响应中
        if details and status_value == 'partial':
            response_data['details'] = details
        
        return Response(response_data)


class FullIntegrityCheckView(APIView):
    """
    股票数据完整性检查视图
    每次调用处理单个股票代码
    使用QuestDB数据库连接池进行数据查询
    """
    from backend.global_config.stock_info import StockInfo
    def post(self, request):
        # 1. 参数验证
        stock_code = self._validate_stock_code(request)
        if not stock_code:
            return self._error_response('股票代码不能为空', status.HTTP_400_BAD_REQUEST)
        
        conn = None
        try:
            # 2. 获取数据库连接
            conn = get_conn()
            if conn is None:
                return self._error_response('无法获取数据库连接', status.HTTP_503_SERVICE_UNAVAILABLE)
            
            with conn.cursor() as cursor:
                # 3. 查询股票基本信息
                
                stock_name = StockInfo.get_company_name_by_code(stock_code)
                if not stock_name:
                    return self._error_response('未找到股票信息', status.HTTP_404_NOT_FOUND)
                
                stock_code, stock_name = stock_info
                
                # 4. 获取日数据记录数
                daily_count = self._get_daily_count(cursor, stock_code)
                
                # 5. 检查复权数据完整性
                adjustment_issues = self._check_adjustment_integrity(cursor, stock_code, daily_count)
                
                # 6. 检查日期连续性
                missing_dates = self._check_date_continuity(cursor, stock_code)
                
                # 7. 检查数据更新时间
                update_issues, last_update = self._get_last_update_date(cursor, stock_code, daily_count)
                
                # 8. 合并所有问题
                issues = []
                details = {
                    'date_gaps': missing_dates,
                    'missing_adjustment': adjustment_issues,
                    'old_data_dates': update_issues
                }
                
                issues.extend(adjustment_issues)
                
                if missing_dates:
                    issues.append(f'缺失交易日数据: {missing_dates[:5]}...' if len(missing_dates) > 5 else f'缺失交易日数据: {missing_dates}')
                
                issues.extend(update_issues)
                
                # 9. 确定状态
                status_value = self._determine_status(issues, daily_count)
                
                # 10. 构建响应
                return self._build_response(stock_code, stock_name, daily_count, issues, status_value, last_update, details)
                
        except Exception as e:
            print(f"完整性检查异常: {str(e)}")
            return self._error_response(f"服务器内部错误: {str(e)}", status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            if conn:
                put_conn(conn)
    
    def _validate_stock_code(self, request):
        """验证请求中的股票代码参数"""
        return request.data.get('stock_code')
    
    def _error_response(self, message, status_code):
        """构建统一的错误响应"""
        return Response({'error': message}, status=status_code)
    
    def _get_daily_count(self, cursor, stock_code):
        """获取股票的日数据记录总数"""
        cursor.execute("SELECT COUNT(*) FROM stock_daily WHERE code = %s", [stock_code])
        return cursor.fetchone()[0]
    
    def _check_adjustment_integrity(self, cursor, stock_code, daily_count):
        """检查复权数据的完整性，返回问题列表"""
        issues = []
        
        # 检查前复权数据
        cursor.execute("SELECT COUNT(*) FROM stock_daily WHERE code = %s AND adjust_type = 'qfq'", [stock_code])
        qfq_count = cursor.fetchone()[0]
        if qfq_count < daily_count * 0.9:  # 如果前复权数据不足90%
            issues.append('前复权数据不完整')
        
        # 检查后复权数据
        cursor.execute("SELECT COUNT(*) FROM stock_daily WHERE code = %s AND adjust_type = 'hfq'", [stock_code])
        hfq_count = cursor.fetchone()[0]
        if hfq_count < daily_count * 0.9:  # 如果后复权数据不足90%
            issues.append('后复权数据不完整')
        
        # 检查不复权数据
        cursor.execute("SELECT COUNT(*) FROM stock_daily WHERE code = %s AND adjust_type IS NULL", [stock_code])
        original_count = cursor.fetchone()[0]
        if original_count < daily_count * 0.9:  # 如果不复权数据不足90%
            issues.append('不复权数据不完整')
        
        return issues
    
    def _check_date_continuity(self, cursor, stock_code):
        """检查股票日数据的日期连续性，返回缺失的日期列表"""
        missing_dates = []
        
        # 导入交易日检查函数
        from backend.global_config.data_fetch import is_trading_day, DataFetchError
        
        # 获取该股票的所有交易日期，按日期排序
        cursor.execute("SELECT DISTINCT date FROM stock_daily WHERE code = %s ORDER BY date", [stock_code])
        dates = [row[0] for row in cursor.fetchall()]
        
        if len(dates) <= 1:
            return missing_dates  # 如果只有0或1条记录，无法检测连续性
        
        # 检查相邻日期之间是否有缺失
        from datetime import timedelta, datetime
        for i in range(len(dates) - 1):
            current_date = dates[i]
            next_date = dates[i + 1]
            
            # 处理可能的字符串日期格式
            if isinstance(current_date, str):
                current_date = datetime.strptime(current_date, '%Y-%m-%d %H:%M:%S').date()
            else:
                try:
                    current_date = current_date.date()
                except:
                    continue
            
            if isinstance(next_date, str):
                next_date = datetime.strptime(next_date, '%Y-%m-%d %H:%M:%S').date()
            else:
                try:
                    next_date = next_date.date()
                except:
                    continue
            
            # 计算两个日期之间的天数差
            days_diff = (next_date - current_date).days
            
            # 如果差值大于1天，说明可能有缺失
            # 注意：这里需要考虑周末和节假日
            # 简化处理：如果差值超过3天（考虑周末和可能的节假日），则认为有缺失
            if days_diff > 3:
                # 计算缺失的日期范围
                missing_start = current_date + timedelta(days=1)
                missing_end = next_date - timedelta(days=1)
                
                # 生成缺失的日期字符串列表
                current_missing = missing_start
                while current_missing <= missing_end:
                    # 首先检查是否为周末
                    if current_missing.weekday() < 5:  # 0=周一, 4=周五, 5=周六, 6=周日
                        try:
                            # 如果不是周末，使用data_fetch.py中的is_trading_day函数判断是否为交易日
                            if is_trading_day(current_missing):
                                missing_dates.append(current_missing.strftime('%Y-%m-%d'))
                        except DataFetchError as e:
                            # 如果调用is_trading_day失败，记录错误并假设非周末即为交易日
                            print(f"检查交易日失败: {str(e)}")
                            missing_dates.append(current_missing.strftime('%Y-%m-%d'))
                    # 周末日期直接跳过，不添加到缺失列表中
                    current_missing += timedelta(days=1)
        
        return missing_dates
    
    def _get_last_update_date(self, cursor, stock_code, daily_count):
        """获取最后更新日期并检查更新时间"""
        issues = []
        
        # 获取最新交易日期
        cursor.execute("SELECT MAX(date) FROM stock_daily WHERE code = %s", [stock_code])
        latest_date = cursor.fetchone()[0]
        
        if latest_date:
            # 计算与当前日期的天数差
            from datetime import datetime, date
            try:
                # 尝试解析日期，处理可能的格式变化
                latest_date_obj = datetime.strptime(str(latest_date), '%Y-%m-%d %H:%M:%S').date()
                days_since_latest = (date.today() - latest_date_obj).days
                
                if days_since_latest > 10:  # 如果超过10天没有更新
                    issues.append(f'数据更新不及时，最近数据日期为{latest_date}')
            except ValueError as e:
                # 如果日期格式不正确，记录为问题
                print(f"异常值: {latest_date}")
                print(f"异常信息: {str(e)}")
                issues.append(f'日期格式错误: {latest_date}')
        else:
            issues.append('没有找到交易日期数据')
        
        return issues, latest_date
    
    def _determine_status(self, issues, daily_count):
        """根据问题列表和记录数量确定股票状态"""
        if len(issues) == 0:
            return 'complete'
        elif daily_count > 0:
            return 'partial'
        else:
            return 'missing'
    
    def _build_response(self, stock_code, stock_name, daily_count, issues, status_value, last_update, details=None):
        """构建响应数据"""
        response_data = {
            'stock_code': stock_code,
            'stock_name': stock_name,
            'total_records': daily_count,
            'issues': issues,
            'status': status_value,
            'last_update': last_update,
            'timestamp': timezone.now().isoformat()
        }
        
        # 如果有详细信息，添加到响应中
        if details and status_value == 'partial':
            response_data['details'] = details
        
        return response_data


class CSVStdCheck(APIView):
    """
    股票数据格式标准化检查视图
    实现两个步骤的检测流程：
    1. 第一步：通过GET方法获取所有基础股票数据的结合
    2. 第二步：通过POST方法检测指定股票代码的CSV文件数据完整性和正确性
    使用QuestDB数据库直接查询实际数据
    """
    
    def get(self, request):
        """获取所有基础股票数据的结合，用于CSV异常值检测的第一步"""

        try:
            # 否则获取所有股票的基本信息，使用StockInfo.get_all_stocks()函数
            stocks = StockInfo.get_all_stocks()
            
            # 构建响应数据
            pending_stocks = [stock.get('code', '') for stock in stocks]  # get_all_stocks()返回字典列表格式
            
            
            return Response({
                # 添加新字段以支持前端需求
                'pending_stocks': pending_stocks,
                'total': len(pending_stocks),
                'message': '获取待检测股票列表成功'
            })
        
        except Exception as e:
            return Response({
                'error': f'获取待检测股票列表失败: {str(e)}',
                'pending_stocks': [],
                'total': 0
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def _enhanced_stock_data_checks(self, df):
        """
        增强型股票数据校验
        基于CSV文件格式的股票数据进行全面校验
        
        根据用户提供的CSV数据格式：
        "code","trade_date","adjust_type","open","close","high","low","volume", "amount", "turnover","outstanding_share"
        
        添加的校验规则：
        1. 交易日期连续性检查
        2. 成交额和成交量一致性检查
        3. 换手率合理性检查
        4. 流通股本稳定性检查
        5. 价格数据连续性检查（无异常跳变）
        6. 数据重复检查
        7. 涨跌幅限制检查（A股±10%）
        8. 价格逻辑关系检查（high >= open/close >= low）
        
        重要说明：所有数据对比检查都将按照adjust_type分组进行，确保在相同类型内进行比较
        """
        issues = []
        details = {}
        
        try:
            # 检查是否有adjust_type列
            has_adjust_type = 'adjust_type' in df.columns
            
            # 如果有adjust_type列，则按adjust_type分组处理；否则整个DataFrame作为一组
            if has_adjust_type:
                # 处理数据重复检查（全局检查，不分组）
                if 'trade_date' in df.columns:
                    # 按adjust_type分组检查重复日期
                    for adjust_type, group in df.groupby('adjust_type'):
                        duplicate_dates = group[group.duplicated('trade_date', keep=False)]
                        if not duplicate_dates.empty:
                            duplicate_count = len(duplicate_dates)
                            issues.append(f'adjust_type={adjust_type}：检测到{duplicate_count}条重复的交易日记录')
                            details[f'duplicate_trade_dates_count_{adjust_type}'] = duplicate_count
                
                # 按adjust_type分组进行其他检查
                for adjust_type, group in df.groupby('adjust_type'):
                    self._process_adjust_type_group(group, adjust_type, issues, details)
            else:
                # 无adjust_type列时的处理
                # 1. 数据重复检查
                if 'trade_date' in df.columns:
                    duplicate_dates = df[df.duplicated('trade_date', keep=False)]
                    if not duplicate_dates.empty:
                        duplicate_count = len(duplicate_dates)
                        issues.append(f'检测到{duplicate_count}条重复的交易日记录')
                        details['duplicate_trade_dates_count'] = duplicate_count
                
                # 处理其他检查
                self._process_adjust_type_group(df, 'default', issues, details)
                
        except Exception as e:
            issues.append(f'增强型校验失败: {str(e)}')
        
        return {
            "status": "pass" if len(issues) == 0 else "fail",
            "issues": issues,
            "details": details
        }
        
    def _process_adjust_type_group(self, df, adjust_type, issues, details):
        """
        处理特定adjust_type分组的数据校验
        
        参数：
        - df: 当前adjust_type分组的数据
        - adjust_type: 当前处理的adjust_type值
        - issues: 全局issues列表
        - details: 全局details字典
        """
        try:
            # 1. 交易日期连续性检查（除停牌日外）
            if 'trade_date' in df.columns and len(df) > 1:
                df_copy = df.copy()
                df_copy['trade_date'] = pd.to_datetime(df_copy['trade_date'])
                df_copy = df_copy.sort_values('trade_date')
                
                # 计算日期差值
                date_diffs = df_copy['trade_date'].diff().dt.days
                large_gaps = date_diffs[date_diffs > 5]  # 超过5天的间隔视为异常
                
                if not large_gaps.empty:
                    gap_count = len(large_gaps)
                    issues.append(f'adjust_type={adjust_type}：检测到{gap_count}个交易日期异常间隔（超过5天）')
                    details[f'large_date_gaps_count_{adjust_type}'] = gap_count
            
            # 2. 成交额和成交量一致性检查
            if all(col in df.columns for col in ['volume', 'amount', 'close']):
                # 尝试转换为数值类型
                try:
                    df_numeric = df.copy()
                    df_numeric['volume'] = pd.to_numeric(df_numeric['volume'])
                    df_numeric['amount'] = pd.to_numeric(df_numeric['amount'])
                    df_numeric['close'] = pd.to_numeric(df_numeric['close'])
                    
                    # 计算理论成交额（成交量 * 收盘价）
                    df_numeric['theoretical_amount'] = df_numeric['volume'] * df_numeric['close']
                    
                    # 过滤掉成交量为0的行
                    valid_data = df_numeric[df_numeric['volume'] > 0]
                    
                    if len(valid_data) > 0:
                        # 计算偏差率
                        valid_data['deviation_rate'] = abs(valid_data['amount'] - valid_data['theoretical_amount']) / valid_data['amount']
                        
                        # 检查偏差是否过大（超过20%）
                        large_deviation = valid_data[valid_data['deviation_rate'] > 0.2]
                        if not large_deviation.empty:
                            deviation_count = len(large_deviation)
                            issues.append(f'adjust_type={adjust_type}：检测到{deviation_count}条成交额与成交量不一致的异常记录')
                            details[f'成交额成交量不一致_count_{adjust_type}'] = deviation_count
                except Exception as e:
                    issues.append(f'adjust_type={adjust_type}：成交额一致性检查失败: {str(e)}')
            
            # 3. 换手率合理性检查
            if 'turnover' in df.columns:
                try:
                    df_numeric = df.copy()
                    df_numeric['turnover'] = pd.to_numeric(df_numeric['turnover'])
                    
                    # 检查换手率是否在合理范围内（0-100%）
                    invalid_turnover = df_numeric[(df_numeric['turnover'] < 0) | (df_numeric['turnover'] > 1)]
                    invalid_count = len(invalid_turnover)
                    
                    if invalid_count > 0:
                        issues.append(f'adjust_type={adjust_type}：检测到{invalid_count}条换手率异常记录（应在0-100%范围内）')
                        details[f'invalid_turnover_count_{adjust_type}'] = invalid_count
                    
                    # 检查异常高的换手率
                    high_turnover = df_numeric[df_numeric['turnover'] > 0.5]  # 超过50%视为异常高
                    high_count = len(high_turnover)
                    if high_count > 0:
                        # 获取异常高换手率的具体日期
                        # 检查trade_date列是否为字符串类型，如果是则转换为datetime
                        if pd.api.types.is_string_dtype(high_turnover['trade_date']):
                            # 处理ISO格式的日期字符串 (如 "1991-04-03T00:00:00.000000Z")
                            high_turnover_dates = []
                            for date_str in high_turnover['trade_date']:
                                # 提取日期部分（T之前的部分）
                                if isinstance(date_str, str) and 'T' in date_str:
                                    date_part = date_str.split('T')[0]
                                    high_turnover_dates.append(date_part)
                                else:
                                    # 尝试转换为datetime然后格式化
                                    try:
                                        date_obj = pd.to_datetime(date_str)
                                        high_turnover_dates.append(date_obj.strftime('%Y-%m-%d'))
                                    except:
                                        high_turnover_dates.append(str(date_str))
                        else:
                            # 如果已经是datetime类型，直接格式化
                            high_turnover_dates = high_turnover['trade_date'].dt.strftime('%Y-%m-%d').tolist()
                        # 限制显示的日期数量，避免消息过长
                        display_dates = high_turnover_dates[:5]  # 只显示前5个日期
                        dates_str = ', '.join(display_dates)
                        if len(high_turnover_dates) > 5:
                            dates_str += f'... 等{high_count}天'
                        else:
                            dates_str += f'，共{high_count}天'
                        
                        issues.append(f'adjust_type={adjust_type}：检测到{high_count}条换手率异常高的记录（>50%），日期：{dates_str}')
                        details[f'high_turnover_count_{adjust_type}'] = high_count
                        details[f'high_turnover_dates_{adjust_type}'] = high_turnover_dates  # 保存所有日期到details
                except Exception as e:
                    issues.append(f'adjust_type={adjust_type}：换手率检查失败: {str(e)}')
            
            # 4. 流通股本稳定性检查
            if 'outstanding_share' in df.columns:
                try:
                    df_numeric = df.copy()
                    df_numeric['outstanding_share'] = pd.to_numeric(df_numeric['outstanding_share'])
                    
                    # 检查流通股本不应为负值
                    neg_outstanding = df_numeric['outstanding_share'] < 0
                    neg_outstanding_count = neg_outstanding.sum()
                    if neg_outstanding_count > 0:
                        issues.append(f'adjust_type={adjust_type}：检测到{neg_outstanding_count}条流通股本为负值的异常记录')
                        details[f'negative_outstanding_count_{adjust_type}'] = neg_outstanding_count
                    
                    # 检查流通股本异常波动
                    if len(df_numeric) > 1:
                        outstanding_std = df_numeric['outstanding_share'].std()
                        outstanding_mean = df_numeric['outstanding_share'].mean()
                        
                        if outstanding_mean > 0:
                            cv = outstanding_std / outstanding_mean  # 变异系数
                            if cv > 0.5:  # 变异系数超过50%视为异常波动
                                issues.append(f'adjust_type={adjust_type}：流通股本存在异常波动')
                                details[f'流通股本变异系数_{adjust_type}'] = round(cv, 4)
                except Exception as e:
                    issues.append(f'adjust_type={adjust_type}：流通股本检查失败: {str(e)}')
            
            # 5. 涨跌幅限制检查（A股±10%）
            if 'close' in df.columns and 'trade_date' in df.columns:
                try:
                    df_numeric = df.copy()
                    df_numeric['close'] = pd.to_numeric(df_numeric['close'])
                    df_numeric = df_numeric.sort_values('trade_date')
                    
                    # 计算涨跌幅
                    df_numeric['pct_change'] = df_numeric['close'].pct_change() * 100
                    
                    # 检查是否超过A股限制（±10%，允许1%的误差范围）
                    abnormal_changes = df_numeric[abs(df_numeric['pct_change']) > 11]  # 允许1%误差
                    if not abnormal_changes.empty:
                        abnormal_count = len(abnormal_changes)
                        
                        # 获取异常涨跌幅的具体日期
                        # 检查trade_date列是否为字符串类型，如果是则转换处理
                        if pd.api.types.is_string_dtype(abnormal_changes['trade_date']):
                            # 处理ISO格式的日期字符串 (如 "1991-04-03T00:00:00.000000Z")
                            abnormal_dates = []
                            for date_str in abnormal_changes['trade_date']:
                                # 提取日期部分（T之前的部分）
                                if isinstance(date_str, str) and 'T' in date_str:
                                    date_part = date_str.split('T')[0]
                                    abnormal_dates.append(date_part)
                                else:
                                    # 尝试转换为datetime然后格式化
                                    try:
                                        date_obj = pd.to_datetime(date_str)
                                        abnormal_dates.append(date_obj.strftime('%Y-%m-%d'))
                                    except:
                                        abnormal_dates.append(str(date_str))
                        else:
                            # 如果已经是datetime类型，直接格式化
                            abnormal_dates = abnormal_changes['trade_date'].dt.strftime('%Y-%m-%d').tolist()
                        
                        # 限制显示的日期数量，避免消息过长
                        display_dates = abnormal_dates[:5]  # 只显示前5个日期
                        dates_str = ', '.join(display_dates)
                        if len(abnormal_dates) > 5:
                            dates_str += f'... 等{abnormal_count}天'
                        else:
                            dates_str += f'，共{abnormal_count}天'
                        
                        issues.append(f'adjust_type={adjust_type}：检测到{abnormal_count}条涨跌幅异常记录（超过±11%），日期：{dates_str}')
                        details[f'abnormal_pct_change_count_{adjust_type}'] = abnormal_count
                        details[f'abnormal_pct_change_dates_{adjust_type}'] = abnormal_dates  # 保存所有异常日期
                except Exception as e:
                    issues.append(f'adjust_type={adjust_type}：涨跌幅检查失败: {str(e)}')
            
            # 7. 价格逻辑关系检查
            price_columns = ['open', 'close', 'high', 'low']
            if all(col in df.columns for col in price_columns):
                try:
                    df_numeric = df.copy()
                    for col in price_columns:
                        df_numeric[col] = pd.to_numeric(df_numeric[col], errors='coerce')
                    
                    # 检查high >= open/close >= low
                    high_violations = ((df_numeric['high'] < df_numeric['open']) | 
                                      (df_numeric['high'] < df_numeric['close']))
                    low_violations = ((df_numeric['low'] > df_numeric['open']) | 
                                     (df_numeric['low'] > df_numeric['close']))
                    
                    violations = high_violations | low_violations
                    violation_count = violations.sum()
                    
                    if violation_count > 0:
                        issues.append(f'检测到{violation_count}条价格逻辑关系异常记录（high应>=open/close>=low）')
                        details['price_logic_violations_count'] = violation_count
                except Exception as e:
                    issues.append(f'价格逻辑检查失败: {str(e)}')
            
            # 8. 价格数据连续性检查（无异常跳变）
            if 'close' in df.columns and len(df) > 1:
                try:
                    df_numeric = df.copy()
                    df_numeric['close'] = pd.to_numeric(df_numeric['close'])
                    df_numeric = df_numeric.sort_values('trade_date')
                    
                    # 计算价格变化率（绝对值）
                    df_numeric['price_change_pct'] = df_numeric['close'].pct_change().abs()
                    
                    # 检测异常跳变（超过30%且不是第一天）
                    abnormal_jumps = df_numeric[(df_numeric['price_change_pct'] > 0.3) & 
                                              (df_numeric['price_change_pct'].notna())]
                    
                    if not abnormal_jumps.empty:
                        jump_count = len(abnormal_jumps)
                        issues.append(f'检测到{jump_count}个价格异常跳变点（超过30%的变化）')
                        details['price_abnormal_jumps_count'] = jump_count
                except Exception as e:
                    issues.append(f'价格连续性检查失败: {str(e)}')
            
        except Exception as e:
            issues.append(f'增强型校验失败: {str(e)}')
        
        return {
            "status": "pass" if len(issues) == 0 else "fail",
            "issues": issues,
            "details": details
        }
        
    def post(self, request):
        """检测指定股票代码的CSV文件数据完整性和正确性，用于CSV异常值检测的第二步"""
        # 获取股票代码参数
        stock_code = request.data.get('stock_code')
        # 验证参数
        if not stock_code:
            return Response({
                'error': '股票代码不能为空',
                'pending_stocks': []
            }, status=status.HTTP_400_BAD_REQUEST)
        
        try:
            # 2. 获取股票基本信息
            stock_info = StockInfo.get_company_name_by_code(stock_code)
            if not stock_info:
                return Response({'error': f'Stock {stock_code} not found'}, status=status.HTTP_404_NOT_FOUND)
            
            stock_name = stock_info
            
            # 3. 构建CSV文件路径
            import os
            import pandas as pd
            csv_file_path = os.path.join('data', 'daily', f'{stock_code}.csv')
            
            # 检查CSV文件是否存在
            if not os.path.exists(csv_file_path):
                # 尝试使用绝对路径
                base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                csv_file_path = os.path.join(base_dir, 'data', 'daily', f'{stock_code}.csv')
                
                if not os.path.exists(csv_file_path):
                    return Response({
                        'error': f'CSV file for stock {stock_code} not found in data/daily directory'
                    }, status=status.HTTP_404_NOT_FOUND)
            
            # 4. 读取CSV文件到DataFrame
            df = pd.read_csv(csv_file_path, 
                             keep_default_na=False,  # 禁用默认的 NaN 转换
                             na_values=[],           # 不将任何额外值视为 NaN
                            )
            
            # 5. 获取记录总数
            record_count = len(df)
            
            # 6. 执行四个方面的检测并返回结果（严格按照要求的格式）
            # 7. 执行增强型校验

            return Response({
                'stock_code': stock_code,
                'stock_name': stock_name,
                'total_records': record_count,
                'file_path': csv_file_path,
                'accuracy_check': self._check_data_accuracy_df(df),
                'logical_check': self._check_data_logical_df(df),
                'format_check': self._check_data_format_df(df),
                'suspension_check': self._check_suspension_days_df(df),
                'enhanced_check': self._enhanced_stock_data_checks(df),  # 添加增强型校验结果
                'timestamp': timezone.now().isoformat()
            })
            
        except Exception as e:
            return Response({
                'error': f'Error during CSV file check: {str(e)}'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    
    def _check_data_accuracy_df(self, df):
        """
        数据准确性检查（基于DataFrame）
        检查股票数据的准确性，包括价格范围合理性等
        """
        issues = []
        details = {}
        
        try:
            # 检查价格是否在合理范围内（例如0-10000元）
            price_columns = ['open', 'close', 'high', 'low']
            for col in price_columns:
                if col in df.columns:
                    # 检查价格是否为负数
                    neg_count = (df[col] < 0).sum()
                    if neg_count > 0:
                        issues.append(f'{col}列存在{neg_count}条负数记录')
                        details[f'{col}_negative_count'] = neg_count
                    
                    # 检查价格是否过高（超过10000元）
                    high_count = (df[col] > 10000).sum()
                    if high_count > 0:
                        issues.append(f'{col}列存在{high_count}条异常高价记录')
                        details[f'{col}_high_count'] = high_count
            
            # 检查成交量是否为负数
            if 'volume' in df.columns:
                neg_volume_count = (df['volume'] < 0).sum()
                if neg_volume_count > 0:
                    issues.append(f'存在{neg_volume_count}条成交量为负的记录')
                    details['negative_volume_count'] = neg_volume_count
            
        except Exception as e:
            issues.append(f'准确性检查失败: {str(e)}')
        
        return {
            "status": "pass" if len(issues) == 0 else "fail",
            "issues": issues,
            "details": details
        }
    
    def _check_data_logical_df(self, df):
        """
        数据逻辑性检查（基于DataFrame）
        检查开盘价/收盘价是否在高低价范围内等逻辑关系
        """
        issues = []
        details = {}
        
        try:
            # 检查开盘价/收盘价是否在高低价范围内
            if all(col in df.columns for col in ['open', 'close', 'high', 'low']):
                invalid_price_range = ((df['open'] > df['high']) | 
                                      (df['open'] < df['low']) | 
                                      (df['close'] > df['high']) | 
                                      (df['close'] < df['low']))
                price_range_count = invalid_price_range.sum()
                if price_range_count > 0:
                    issues.append(f'存在{price_range_count}条价格范围异常记录')
                    details['price_range_count'] = price_range_count
            
            # 检查涨跌幅合理性（单日涨跌幅超过20%可能异常）
            if 'close' in df.columns and 'prev_close' in df.columns:
                # 计算涨跌幅并检查是否超过20%
                valid_prev_close = df['prev_close'] > 0
                pct_change = abs((df['close'] - df['prev_close']) / df['prev_close'])
                huge_fluctuation = (pct_change > 0.2) & valid_prev_close
                huge_fluctuation_count = huge_fluctuation.sum()
                if huge_fluctuation_count > 0:
                    issues.append(f'存在{huge_fluctuation_count}条涨跌幅异常记录')
                    details['huge_fluctuation_count'] = huge_fluctuation_count
            
        except Exception as e:
            issues.append(f'逻辑性检查失败: {str(e)}')
        
        return {
            "status": "pass" if len(issues) == 0 else "fail",
            "issues": issues,
            "details": details
        }
    
    def _check_data_format_df(self, df):
        """
        数据格式标准化检查（基于DataFrame）
        检查数据字段的格式是否符合标准
        """
        issues = []
        details = {}
        
        try:
            # 检查必填字段是否有空值
            required_columns = ['trade_date', 'open', 'close', 'high', 'low']
            for col in required_columns:
                if col in df.columns:
                    missing_count = df[col].isnull().sum()
                    if missing_count > 0:
                        issues.append(f'{col}列存在{missing_count}条空值记录')
                        details[f'{col}_missing_count'] = missing_count
            
            # 检查数据类型一致性
            numeric_columns = ['open', 'close', 'high', 'low', 'volume', 'amount']
            for col in numeric_columns:
                if col in df.columns:
                    # 检查是否可以转换为数字
                    try:
                        # 创建一个副本并进行转换测试
                        test_df = df.copy()
                        pd.to_numeric(test_df[col], errors='coerce')
                        non_numeric_count = test_df[col].isnull().sum()
                        if non_numeric_count > 0:
                            issues.append(f'{col}列存在{non_numeric_count}条非数字格式记录')
                            details[f'{col}_non_numeric_count'] = non_numeric_count
                    except Exception:
                        issues.append(f'{col}列数据类型转换失败')
            
            # 检查trade_date列是否为日期格式
            if 'trade_date' in df.columns:
                try:
                    pd.to_datetime(df['trade_date'], errors='coerce')
                    invalid_date_count = df['trade_date'].isnull().sum()
                    if invalid_date_count > 0:
                        issues.append(f'trade_date列存在{invalid_date_count}条无效日期记录')
                        details['invalid_date_count'] = invalid_date_count
                except Exception:
                    issues.append('trade_date列日期格式异常')
            
            # 检查adjust_type字段值的合法性
            if 'adjust_type' in df.columns:
                valid_adjust_types = ['qfq', 'hfq','']
                invalid_adjust = ~df['adjust_type'].isin(valid_adjust_types)
                invalid_adjust_count = invalid_adjust.sum()
                if invalid_adjust_count > 0:
                    issues.append(f'存在{invalid_adjust_count}条adjust_type值异常记录')
                    details['invalid_adjust_type_count'] = invalid_adjust_count
            
        except Exception as e:
            issues.append(f'格式检查失败: {str(e)}')
        
        return {
            "status": "pass" if len(issues) == 0 else "fail",
            "issues": issues,
            "details": details
        }
    
    def _check_suspension_days_df(self, df):
        """
        停牌日检测（基于DataFrame）
        识别股票可能的停牌日期
        """
        issues = []
        details = {
            "suspension_days": [],
            "suspicious_days": []
        }
        
        try:
            from datetime import timedelta
            
            # 检查是否有足够的数据进行分析
            if len(df) < 2:
                issues.append('数据量不足，无法进行停牌日检测')
                return {
                    "status": "warn",
                    "issues": issues,
                    "details": details
                }
            
            # 确保trade_date是datetime类型
            if 'trade_date' in df.columns:
                df_copy = df.copy()
                df_copy['trade_date'] = pd.to_datetime(df_copy['trade_date'])
                
                # 获取日期范围
                start_date = df_copy['trade_date'].min().date()
                end_date = df_copy['trade_date'].max().date()
                
                # 获取数据中的交易日期列表
                trading_days = set(df_copy['trade_date'].dt.date.tolist())
                
                # 检测可能的停牌日
                current_date = start_date
                while current_date <= end_date:
                    # 检查是否为工作日（周一到周五）
                    if current_date.weekday() < 5:
                        # 如果是工作日但不在交易日期列表中，且前后都有交易日，则可能是停牌日
                        if current_date not in trading_days:
                            # 检查前后是否有交易日
                            has_prev = any(d < current_date for d in trading_days)
                            has_next = any(d > current_date for d in trading_days)
                            
                            if has_prev and has_next:
                                date_str = current_date.strftime('%Y-%m-%d')
                                details["suspension_days"].append(date_str)
                    
                    current_date += timedelta(days=1)
                
                # 检测可疑数据（成交量为0但有价格数据）
                if all(col in df.columns for col in ['volume', 'trade_date']):
                    zero_volume_rows = df[df['volume'] == 0]
                    for _, row in zero_volume_rows.iterrows():
                        details["suspicious_days"].append({
                            "date": str(row['trade_date']).split()[0],
                            "reason": "成交量为0但有价格数据"
                        })
            
            # 记录检查结果
            if details["suspension_days"]:
                issues.append(f'检测到{len(details["suspension_days"])}个可能的停牌日')
            
            if details["suspicious_days"]:
                issues.append(f'检测到{len(details["suspicious_days"])}个可疑的交易数据记录')
            
        except Exception as e:
            issues.append(f'停牌日检测失败: {str(e)}')
        
        return {
            "status": "pass" if len(issues) == 0 else "warn",
            "issues": issues,
            "details": details
        }
    
    def _check_data_accuracy(self, cursor, stock_code):
        """
        数据准确性检查
        检查数据是否在合理范围内，没有异常值
        """
        issues = []
        details = {}
        
        try:
            # 检查价格是否为负数或零
            cursor.execute("""
                SELECT COUNT(*) FROM stock_daily 
                WHERE code = %s AND (open <= 0 OR close <= 0 OR 
                                    high <= 0 OR low <= 0)
            """, [stock_code])
            invalid_price_count = cursor.fetchone()[0]
            if invalid_price_count > 0:
                issues.append(f'存在{invalid_price_count}条价格异常记录')
                details['invalid_price_count'] = invalid_price_count
            
            # 检查交易量和成交额是否为负数
            cursor.execute("""
                SELECT COUNT(*) FROM stock_daily 
                WHERE code = %s AND (volume < 0 OR amount < 0)
            """, [stock_code])
            invalid_trade_count = cursor.fetchone()[0]
            if invalid_trade_count > 0:
                issues.append(f'存在{invalid_trade_count}条交易量/成交额异常记录')
                details['invalid_trade_count'] = invalid_trade_count
            
            # 检查价格范围合理性（最高价应大于等于最低价）
            cursor.execute("""
                SELECT COUNT(*) FROM stock_daily 
                WHERE code = %s AND high < low
            """, [stock_code])
            invalid_range_count = cursor.fetchone()[0]
            if invalid_range_count > 0:
                issues.append(f'存在{invalid_range_count}条价格范围异常记录')
                details['invalid_range_count'] = invalid_range_count
            
        except Exception as e:
            issues.append(f'准确性检查失败: {str(e)}')
        
        return {
            "status": "pass" if len(issues) == 0 else "fail",
            "issues": issues,
            "details": details
        }
    
    def _check_data_logical(self, cursor, stock_code):
        """
        数据逻辑性检查
        检查数据之间的逻辑关系是否合理
        """
        issues = []
        details = {}
        
        try:
            # 检查开盘价是否在当日最高和最低价之间
            cursor.execute("""
                SELECT COUNT(*) FROM stock_daily 
                WHERE code = %s AND (open < low OR open > high)
            """, [stock_code])
            open_price_issue_count = cursor.fetchone()[0]
            if open_price_issue_count > 0:
                issues.append(f'存在{open_price_issue_count}条开盘价异常记录')
                details['open_price_issue_count'] = open_price_issue_count
            
            # 检查收盘价是否在当日最高和最低价之间
            cursor.execute("""
                SELECT COUNT(*) FROM stock_daily 
                WHERE code = %s AND (close < low OR close > high)
            """, [stock_code])
            close_price_issue_count = cursor.fetchone()[0]
            if close_price_issue_count > 0:
                issues.append(f'存在{close_price_issue_count}条收盘价异常记录')
                details['close_price_issue_count'] = close_price_issue_count
            
            # 检查涨跌幅是否合理（超过20%可能是异常）
            #
            #cursor.execute("""
            #    SELECT COUNT(*) FROM stock_daily 
            #    WHERE code = %s AND abs((close_price - prev_close) / prev_close * 100) > 20
            #    AND prev_close > 0
            #""", [stock_code])
            #huge_fluctuation_count = cursor.fetchone()[0]
            #if huge_fluctuation_count > 0:
            #    issues.append(f'存在{huge_fluctuation_count}条涨跌幅异常记录')
            #    details['huge_fluctuation_count'] = huge_fluctuation_count
            
        except Exception as e:
            issues.append(f'逻辑性检查失败: {str(e)}')
        
        return {
            "status": "pass" if len(issues) == 0 else "fail",
            "issues": issues,
            "details": details
        }
    
    def _check_data_format(self, cursor, stock_code):
        """
        数据格式标准化检查
        检查数据字段的格式是否符合标准
        """
        issues = []
        details = {}
        
        try:
            # 检查必填字段是否有空值
            cursor.execute("""
                SELECT COUNT(*) FROM stock_daily 
                WHERE code = %s AND (trade_date IS NULL OR open IS NULL OR 
                                    close IS NULL OR high IS NULL OR 
                                    low IS NULL)
            """, [stock_code])
            missing_required_count = cursor.fetchone()[0]
            if missing_required_count > 0:
                issues.append(f'存在{missing_required_count}条必填字段缺失记录')
                details['missing_required_count'] = missing_required_count
            
            # 检查数据类型一致性（通过查询有问题的记录）
            # 在QuestDB中，我们可以通过一些基本检查来验证字段格式
            cursor.execute("""
                SELECT COUNT(*) FROM stock_daily 
                WHERE code = %s AND 
                (CAST(volume AS BIGINT) IS NULL OR 
                 CAST(amount AS DOUBLE) IS NULL OR
                 CAST(open AS DOUBLE) IS NULL)
            """, [stock_code])
            format_issue_count = cursor.fetchone()[0]
            if format_issue_count > 0:
                issues.append(f'存在{format_issue_count}条数据格式异常记录')
                details['format_issue_count'] = format_issue_count
            
            # 检查adjust_type字段值的合法性
            cursor.execute("""
                SELECT COUNT(*) FROM stock_daily 
                WHERE code = %s AND adjust_type IS NOT NULL AND 
                adjust_type NOT IN ('qfq', 'hfq')
            """, [stock_code])
            invalid_adjust_type_count = cursor.fetchone()[0]
            if invalid_adjust_type_count > 0:
                issues.append(f'存在{invalid_adjust_type_count}条adjust_type值异常记录')
                details['invalid_adjust_type_count'] = invalid_adjust_type_count
            
        except Exception as e:
            issues.append(f'格式检查失败: {str(e)}')
        
        return {
            "status": "pass" if len(issues) == 0 else "fail",
            "issues": issues,
            "details": details
        }
    
    def _check_suspension_days(self, cursor, stock_code):
        """
        停牌日检测
        识别股票可能的停牌日期
        """
        issues = []
        details = {
            "suspension_days": [],
            "suspicious_days": []
        }
        
        try:
            # 获取该股票的交易日期范围
            cursor.execute("""
                SELECT MIN(trade_date), MAX(trade_date) FROM stock_daily WHERE code = %s
            """, [stock_code])
            date_range = cursor.fetchone()
            
            if not date_range or not date_range[0] or not date_range[1]:
                issues.append('无法获取交易日期范围')
                return {
                    "status": "fail",
                    "issues": issues,
                    "details": details
                }
            
            # 解析日期范围
            start_date = date_range[0]
            end_date = date_range[1]
            
            # 转换为datetime对象
            if isinstance(start_date, str):
                start_date = datetime.strptime(start_date.split()[0], '%Y-%m-%d').date()
            else:
                start_date = start_date.date() if hasattr(start_date, 'date') else start_date
            
            if isinstance(end_date, str):
                end_date = datetime.strptime(end_date.split()[0], '%Y-%m-%d').date()
            else:
                end_date = end_date.date() if hasattr(end_date, 'date') else end_date
            
            # 获取交易日列表（从现有数据中）
            cursor.execute("""
                SELECT DISTINCT CAST(trade_date AS DATE) as trade_date  FROM stock_daily WHERE code = %s
                ORDER BY trade_date
            """, [stock_code])
            trading_days = [row[0].date() if hasattr(row[0], 'date') else row[0] for row in cursor.fetchall()]
            
            # 检测可能的停牌日（交易日列表中的间隔日期）
            current_date = start_date
            while current_date <= end_date:
                # 检查是否为交易日
                try:
                    # 使用 is_trading_day 函数判断是否为交易日
                    if current_date.weekday() <= 5:
                        if is_trading_day(current_date):
                            # 检查是否在交易日列表中
                            if current_date not in trading_days:
                                # 是交易日又没有在交易日列表中，可能是停牌日
                                # 检查前后是否有交易日（确认是中间缺失）
                                has_prev = any(d < current_date for d in trading_days)
                                has_next = any(d > current_date for d in trading_days)
                            
                                if has_prev and has_next:
                                    date_str = current_date.strftime('%Y-%m-%d')
                                    details["suspension_days"].append(date_str)
                
                except DataFetchError as e:
                    # 如果调用 is_trading_day 失败，记录错误并仅检查是否为工作日
                    print(f"检查交易日失败: {str(e)}")
                    # 只检查是否为工作日
                    if current_date.weekday() < 5:
                        # 检查是否在交易日列表中
                        if current_date not in trading_days:
                            # 检查前后是否有交易日（确认是中间缺失）
                            has_prev = any(d < current_date for d in trading_days)
                            has_next = any(d > current_date for d in trading_days)
                            
                            if has_prev and has_next:
                                date_str = current_date.strftime('%Y-%m-%d')
                                details["suspension_days"].append(date_str)
                
                current_date += timedelta(days=1)
            
            # 检测可疑数据（有交易记录但成交量为0）
            cursor.execute("""
                SELECT CAST(trade_date AS DATE), volume FROM stock_daily 
                WHERE code = %s AND volume = 0
            """, [stock_code])
            zero_volume_records = cursor.fetchall()
            
            for record in zero_volume_records:
                details["suspicious_days"].append({
                    "date": record[0].strftime('%Y-%m-%d') if hasattr(record[0], 'strftime') else str(record[0]),
                    "reason": "成交量为0但有价格数据"
                })
            
            # 记录检查结果
            if details["suspension_days"]:
                issues.append(f'检测到{len(details["suspension_days"])}个可能的停牌日')
            
            if details["suspicious_days"]:
                issues.append(f'检测到{len(details["suspicious_days"])}个可疑的交易数据记录')
            
        except Exception as e:
            issues.append(f'停牌日检测失败: {str(e)}')
        
        return {
            "status": "pass" if len(issues) == 0 else "warn",
            "issues": issues,
            "details": details
        }

class StockFormatStandardizationView(APIView):
    """
    股票数据格式标准化检查视图
    实现两个步骤的检测流程：
    1. 第一步：通过stock_daily表获得要检测股票的列表（复用stocks/integrity/check接口的GET方法）
    2. 第二步：对每个股票进行四个方面的检测
        a. 数据准确性检查
        b. 数据逻辑性检查
        c. 数据格式标准化检查
        d. 停牌日检测
    使用QuestDB数据库直接查询实际数据
    """
    
    def post(self, request):
        """执行单股票的数据格式标准化检查"""
        # 1. 参数验证
        stock_code = request.data.get('stock_code')
        if not stock_code:
            return Response({'error': 'stock_code is required'}, status=status.HTTP_400_BAD_REQUEST)
        
        conn = None
        try:
            # 2. 获取数据库连接
            conn = get_conn()
            if not conn:
                return Response({
                    'error': '无法获取数据库连接'
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
            
            with conn.cursor() as cursor:
                # 3. 查询股票基本信息
                cursor.execute("SELECT name FROM stock_basic WHERE code = %s", [stock_code])
                stock_info = cursor.fetchone()
                if not stock_info:
                    return Response({'error': f'Stock {stock_code} not found'}, status=status.HTTP_404_NOT_FOUND)
                
                stock_name = stock_info[0]
                
                # 4. 获取记录总数
                cursor.execute("SELECT COUNT(*) FROM stock_daily WHERE code = %s", [stock_code])
                record_count = cursor.fetchone()[0]
                
                # 5. 执行四个方面的检测
                check_results = {
                    'stock_code': stock_code,
                    'stock_name': stock_name,
                    'total_records': record_count,
                    'accuracy_check': self._check_data_accuracy(cursor, stock_code),
                    'logical_check': self._check_data_logical(cursor, stock_code),
                    'format_check': self._check_data_format(cursor, stock_code),
                    'suspension_check': self._check_suspension_days(cursor, stock_code),
                    'timestamp': timezone.now().isoformat()
                }
                
                # 6. 整体状态判断
                all_passed = (
                    check_results['accuracy_check']['status'] == 'pass' and
                    check_results['logical_check']['status'] == 'pass' and
                    check_results['format_check']['status'] == 'pass'
                )
                check_results['overall_status'] = 'pass' if all_passed else 'fail'
                
                return Response(check_results)
        
        except Exception as e:
            return Response({
                'error': f'数据格式标准化检查失败: {str(e)}'
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            # 确保连接被正确归还到连接池
            if conn:
                put_conn(conn)
    
    def _check_data_accuracy(self, cursor, stock_code):
        """
        数据准确性检查
        检查数据是否在合理范围内，没有异常值
        """
        issues = []
        details = {}
        
        try:
            # 检查价格是否为负数或零
            cursor.execute("""
                SELECT COUNT(*) FROM stock_daily 
                WHERE code = %s AND (open <= 0 OR close <= 0 OR 
                                    high <= 0 OR low <= 0)
            """, [stock_code])
            invalid_price_count = cursor.fetchone()[0]
            if invalid_price_count > 0:
                issues.append(f'存在{invalid_price_count}条价格异常记录')
                details['invalid_price_count'] = invalid_price_count
            
            # 检查交易量和成交额是否为负数
            cursor.execute("""
                SELECT COUNT(*) FROM stock_daily 
                WHERE code = %s AND (volume < 0 OR amount < 0)
            """, [stock_code])
            invalid_trade_count = cursor.fetchone()[0]
            if invalid_trade_count > 0:
                issues.append(f'存在{invalid_trade_count}条交易量/成交额异常记录')
                details['invalid_trade_count'] = invalid_trade_count
            
            # 检查价格范围合理性（最高价应大于等于最低价）
            cursor.execute("""
                SELECT COUNT(*) FROM stock_daily 
                WHERE code = %s AND high < low
            """, [stock_code])
            invalid_range_count = cursor.fetchone()[0]
            if invalid_range_count > 0:
                issues.append(f'存在{invalid_range_count}条价格范围异常记录')
                details['invalid_range_count'] = invalid_range_count
            
        except Exception as e:
            issues.append(f'准确性检查失败: {str(e)}')
        
        return {
            "status": "pass" if len(issues) == 0 else "fail",
            "issues": issues,
            "details": details
        }
    
    def _check_data_logical(self, cursor, stock_code):
        """
        数据逻辑性检查
        检查数据之间的逻辑关系是否合理
        """
        issues = []
        details = {}
        
        try:
            # 检查开盘价是否在当日最高和最低价之间
            cursor.execute("""
                SELECT COUNT(*) FROM stock_daily 
                WHERE code = %s AND (open < low OR open > high)
            """, [stock_code])
            open_price_issue_count = cursor.fetchone()[0]
            if open_price_issue_count > 0:
                issues.append(f'存在{open_price_issue_count}条开盘价异常记录')
                details['open_price_issue_count'] = open_price_issue_count
            
            # 检查收盘价是否在当日最高和最低价之间
            cursor.execute("""
                SELECT COUNT(*) FROM stock_daily 
                WHERE code = %s AND (close < low OR close > high)
            """, [stock_code])
            close_price_issue_count = cursor.fetchone()[0]
            if close_price_issue_count > 0:
                issues.append(f'存在{close_price_issue_count}条收盘价异常记录')
                details['close_price_issue_count'] = close_price_issue_count
            
            # 检查涨跌幅是否合理（超过20%可能是异常）
            #
            #cursor.execute("""
            #    SELECT COUNT(*) FROM stock_daily 
            #    WHERE code = %s AND abs((close_price - prev_close) / prev_close * 100) > 20
            #    AND prev_close > 0
            #""", [stock_code])
            #huge_fluctuation_count = cursor.fetchone()[0]
            #if huge_fluctuation_count > 0:
            #    issues.append(f'存在{huge_fluctuation_count}条涨跌幅异常记录')
            #    details['huge_fluctuation_count'] = huge_fluctuation_count
            
        except Exception as e:
            issues.append(f'逻辑性检查失败: {str(e)}')
        
        return {
            "status": "pass" if len(issues) == 0 else "fail",
            "issues": issues,
            "details": details
        }
    
    def _check_data_format(self, cursor, stock_code):
        """
        数据格式标准化检查
        检查数据字段的格式是否符合标准
        """
        issues = []
        details = {}
        
        try:
            # 检查必填字段是否有空值
            cursor.execute("""
                SELECT COUNT(*) FROM stock_daily 
                WHERE code = %s AND (trade_date IS NULL OR open IS NULL OR 
                                    close IS NULL OR high IS NULL OR 
                                    low IS NULL)
            """, [stock_code])
            missing_required_count = cursor.fetchone()[0]
            if missing_required_count > 0:
                issues.append(f'存在{missing_required_count}条必填字段缺失记录')
                details['missing_required_count'] = missing_required_count
            
            # 检查数据类型一致性（通过查询有问题的记录）
            # 在QuestDB中，我们可以通过一些基本检查来验证字段格式
            cursor.execute("""
                SELECT COUNT(*) FROM stock_daily 
                WHERE code = %s AND 
                (CAST(volume AS BIGINT) IS NULL OR 
                 CAST(amount AS DOUBLE) IS NULL OR
                 CAST(open AS DOUBLE) IS NULL)
            """, [stock_code])
            format_issue_count = cursor.fetchone()[0]
            if format_issue_count > 0:
                issues.append(f'存在{format_issue_count}条数据格式异常记录')
                details['format_issue_count'] = format_issue_count
            
            # 检查adjust_type字段值的合法性
            cursor.execute("""
                SELECT COUNT(*) FROM stock_daily 
                WHERE code = %s AND adjust_type IS NOT NULL AND 
                adjust_type NOT IN ('qfq', 'hfq')
            """, [stock_code])
            invalid_adjust_type_count = cursor.fetchone()[0]
            if invalid_adjust_type_count > 0:
                issues.append(f'存在{invalid_adjust_type_count}条adjust_type值异常记录')
                details['invalid_adjust_type_count'] = invalid_adjust_type_count
            
        except Exception as e:
            issues.append(f'格式检查失败: {str(e)}')
        
        return {
            "status": "pass" if len(issues) == 0 else "fail",
            "issues": issues,
            "details": details
        }
    
    def _check_suspension_days(self, cursor, stock_code):
        """
        停牌日检测
        识别股票可能的停牌日期
        """
        issues = []
        details = {
            "suspension_days": [],
            "suspicious_days": []
        }
        
        try:
            # 获取该股票的交易日期范围
            cursor.execute("""
                SELECT MIN(trade_date), MAX(trade_date) FROM stock_daily WHERE code = %s
            """, [stock_code])
            date_range = cursor.fetchone()
            
            if not date_range or not date_range[0] or not date_range[1]:
                issues.append('无法获取交易日期范围')
                return {
                    "status": "fail",
                    "issues": issues,
                    "details": details
                }
            
            # 解析日期范围
            start_date = date_range[0]
            end_date = date_range[1]
            
            # 转换为datetime对象
            if isinstance(start_date, str):
                start_date = datetime.strptime(start_date.split()[0], '%Y-%m-%d').date()
            else:
                start_date = start_date.date() if hasattr(start_date, 'date') else start_date
            
            if isinstance(end_date, str):
                end_date = datetime.strptime(end_date.split()[0], '%Y-%m-%d').date()
            else:
                end_date = end_date.date() if hasattr(end_date, 'date') else end_date
            
            # 获取交易日列表（从现有数据中）
            cursor.execute("""
                SELECT DISTINCT CAST(trade_date AS DATE) as trade_date  FROM stock_daily WHERE code = %s
                ORDER BY trade_date
            """, [stock_code])
            trading_days = [row[0].date() if hasattr(row[0], 'date') else row[0] for row in cursor.fetchall()]
            
            # 检测可能的停牌日（交易日列表中的间隔日期）
            current_date = start_date
            while current_date <= end_date:
                # 检查是否为交易日
                try:
                    # 使用 is_trading_day 函数判断是否为交易日
                    if current_date.weekday() <= 5:
                        if is_trading_day(current_date):
                            # 检查是否在交易日列表中
                            if current_date not in trading_days:
                                # 是交易日又没有在交易日列表中，可能是停牌日
                                # 检查前后是否有交易日（确认是中间缺失）
                                has_prev = any(d < current_date for d in trading_days)
                                has_next = any(d > current_date for d in trading_days)
                            
                                if has_prev and has_next:
                                    date_str = current_date.strftime('%Y-%m-%d')
                                    details["suspension_days"].append(date_str)
                
                except DataFetchError as e:
                    # 如果调用 is_trading_day 失败，记录错误并仅检查是否为工作日
                    print(f"检查交易日失败: {str(e)}")
                    # 只检查是否为工作日
                    if current_date.weekday() < 5:
                        # 检查是否在交易日列表中
                        if current_date not in trading_days:
                            # 检查前后是否有交易日（确认是中间缺失）
                            has_prev = any(d < current_date for d in trading_days)
                            has_next = any(d > current_date for d in trading_days)
                            
                            if has_prev and has_next:
                                date_str = current_date.strftime('%Y-%m-%d')
                                details["suspension_days"].append(date_str)
                
                current_date += timedelta(days=1)
            
            # 检测可疑数据（有交易记录但成交量为0）
            cursor.execute("""
                SELECT CAST(trade_date AS DATE), volume FROM stock_daily 
                WHERE code = %s AND volume = 0
            """, [stock_code])
            zero_volume_records = cursor.fetchall()
            
            for record in zero_volume_records:
                details["suspicious_days"].append({
                    "date": record[0].strftime('%Y-%m-%d') if hasattr(record[0], 'strftime') else str(record[0]),
                    "reason": "成交量为0但有价格数据"
                })
            
            # 记录检查结果
            if details["suspension_days"]:
                issues.append(f'检测到{len(details["suspension_days"])}个可能的停牌日')
            
            if details["suspicious_days"]:
                issues.append(f'检测到{len(details["suspicious_days"])}个可疑的交易数据记录')
            
        except Exception as e:
            issues.append(f'停牌日检测失败: {str(e)}')
        
        return {
            "status": "pass" if len(issues) == 0 else "warn",
            "issues": issues,
            "details": details
        }
    