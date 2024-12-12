import baostock as bs
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from collections import defaultdict
import time
import os
from enum import Enum

class IndexType(Enum):
    HS300 = "hs300"  # 沪深300
    ZZ500 = "zz500"  # 中证500
    SZ50 = "sz50"    # 上证50

def get_index_stocks(date, index_type):
    """
    获取指定日期的指数成分股
    
    Args:
        date (str): 日期，格式：YYYY-MM-DD
        index_type (IndexType): 指数类型
    
    Returns:
        set: 成分股代码集合
    """
    if index_type == IndexType.HS300:
        rs = bs.query_hs300_stocks(date)
    elif index_type == IndexType.ZZ500:
        rs = bs.query_zz500_stocks(date)
    elif index_type == IndexType.SZ50:
        rs = bs.query_sz50_stocks(date)
    else:
        raise ValueError(f"不支持的指数类型: {index_type}")
        
    stocks = set()
    while (rs.error_code == '0') & rs.next():
        code = rs.get_row_data()[1]
        std_code = code.replace('sh.', 'SH').replace('sz.', 'SZ')
        stocks.add(std_code)
    return stocks

def track_index_changes(start_date, end_date, index_type, output_file):
    """
    按月追踪指数成分股的变动
    
    Args:
        start_date (str): 起始日期，格式：YYYY-MM-DD
        end_date (str): 结束日期，格式：YYYY-MM-DD
        index_type (IndexType): 指数类型
        output_file (str): 输出文件路径
    """
    bs.login()
    
    try:
        stock_periods = defaultdict(lambda: {'enter': None, 'exit': None})
        
        # 将日期转换为datetime对象
        current_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_datetime = datetime.strptime(end_date, '%Y-%m-%d')
        
        # 获取起始日期的成分股作为基准
        prev_stocks = get_index_stocks(start_date, index_type)
        for stock in prev_stocks:
            stock_periods[stock]['enter'] = start_date
        
        # 按月遍历检查变动
        while True:
            date_str = current_date.strftime('%Y-%m-%d')
            print(f"处理日期: {date_str}")
            
            # 获取当前日期的成分股
            current_stocks = get_index_stocks(date_str, index_type)
            
            # 检查退出的股票
            for stock in prev_stocks - current_stocks:
                if stock_periods[stock]['exit'] is None:
                    stock_periods[stock]['exit'] = date_str
            
            # 检查新进入的股票
            for stock in current_stocks - prev_stocks:
                stock_periods[stock]['enter'] = date_str
            
            # 更新比较基准
            prev_stocks = current_stocks
            
            # 如果已经到达结束日期，跳出循环
            if current_date >= end_datetime:
                break
                
            # 计算下一个月的日期
            next_date = current_date + relativedelta(months=1)
            
            # 如果下一个月会超过结束日期，则使用结束日期
            if next_date > end_datetime:
                current_date = end_datetime
            else:
                current_date = next_date
                
            time.sleep(0.5)
        
        # 对于截止日期仍在的股票，设置退出时间为end_date
        for stock in prev_stocks:
            stock_periods[stock]['exit'] = end_date
        
        # 创建输出目录（如果不存在）
        os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)
        
        # 将结果写入文件
        with open(output_file, 'w', encoding='utf-8') as f:
            # 按股票代码排序
            for stock in sorted(stock_periods.keys()):
                period = stock_periods[stock]
                enter_date = period['enter'] or start_date
                exit_date = period['exit'] or end_date
                f.write(f"{stock:<12}{enter_date:<15}{exit_date}\n")
                
    finally:
        bs.logout()

if __name__ == '__main__':
    START_DATE = '2014-01-01'
    END_DATE = '2024-12-01'
    
    # 为每个指数创建单独的输出文件
    index_configs = [
        (IndexType.HS300, '~/.qlib/qlib_data/cn_data/instruments/hs300.txt'),
        (IndexType.ZZ500, '~/.qlib/qlib_data/cn_data/instruments/zz500.txt'),
        (IndexType.SZ50, '~/.qlib/qlib_data/cn_data/instruments/sz50.txt')
    ]
    
    for index_type, output_file in index_configs:
        print(f"\n开始追踪{index_type.value}成分股变动...")
        track_index_changes(START_DATE, END_DATE, index_type, output_file)
        print(f"{index_type.value}处理完成! 结果已保存到 {output_file}")