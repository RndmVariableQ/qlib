import baostock as bs
import pandas as pd
from datetime import datetime
import time
from tqdm import tqdm
from pathlib import Path
from dateutil.relativedelta import relativedelta

def get_all_stocks_in_period(start_date, end_date):
    """
    获取指定时间段内所有出现过的股票代码
    
    Args:
        start_date (str): 起始日期，格式：YYYY-MM-DD
        end_date (str): 结束日期，格式：YYYY-MM-DD
    
    Returns:
        set: 股票代码集合
    """
    all_stocks = set()
    
    # 将日期字符串转换为datetime对象
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    # 获取每年年底（或当前日期）的股票列表
    current = start
    while current <= end:
        # 获取当前日期的股票列表
        query_date = current.strftime('%Y-%m-%d')
        print(f"获取 {query_date} 的股票列表...")
        
        stock_rs = bs.query_all_stock(query_date)
        stock_df = stock_rs.get_data()
        
        if not stock_df.empty:
            # 将新的股票代码添加到集合中
            all_stocks.update(stock_df['code'].tolist())
            
        # 前进到下一年的第一天
        current = current + relativedelta(years=1)
        if current > end:
            break
            
    print(f"共获取到 {len(all_stocks)} 只股票")
    return all_stocks

def download_stock_data(start_date, end_date, output_dir):
    """
    下载指定日期范围内所有股票的历史数据，每只股票保存为独立的CSV文件
    
    Args:
        start_date (str): 起始日期，格式：YYYY-MM-DD
        end_date (str): 结束日期，格式：YYYY-MM-DD
        output_dir (str): 输出文件夹路径
    """
    # 将输出目录转换为Path对象并展开用户目录（~）
    output_path = Path(output_dir).expanduser()
    
    # 创建输出目录（如果父目录不存在，也会创建）
    output_path.mkdir(parents=True, exist_ok=True)
    
    # 登录系统
    lg = bs.login()
    if lg.error_code != '0':
        print(f'登录失败: {lg.error_msg}')
        return
    
    try:
        # 获取整个时间段内的所有股票代码
        all_stocks = get_all_stocks_in_period(start_date, end_date)
            
        # 设置要获取的数据字段
        fields = "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST"
        
        # 对每个股票代码进行数据下载
        for code in tqdm(all_stocks, desc="下载进度"):
            # 构建输出文件路径
            output_file = output_path / f"{code.replace('.', '')}.csv"
            
            # 如果文件已存在则跳过
            if output_file.exists():
                continue
                
            try:
                # 查询历史数据
                rs = bs.query_history_k_data_plus(
                    code,
                    fields,
                    start_date=start_date,
                    end_date=end_date,
                    frequency="d",
                    adjustflag="2"  # 前复权
                )
                
                if rs.error_code != '0':
                    print(f"获取 {code} 数据失败: {rs.error_msg}")
                    continue
                
                # 获取数据
                data_list = []
                while (rs.error_code == '0') & rs.next():
                    data_list.append(rs.get_row_data())
                
                # 转换为DataFrame并保存
                if data_list:
                    result = pd.DataFrame(data_list, columns=rs.fields)
                    if 'code' in result.columns:
                        result['code'] = result['code'].str.replace('.', '', regex=False)
                    result.to_csv(output_file, index=False, encoding='utf-8')
                    
                # 添加延时避免请求过于频繁
                time.sleep(0.5)
                
            except Exception as e:
                print(f"处理 {code} 时发生错误: {str(e)}")
                continue
                
    finally:
        # 确保退出登录
        bs.logout()

if __name__ == '__main__':
    # 设置参数
    START_DATE = '2016-12-11'
    END_DATE = '2024-12-11'
    DATA_DIR = '~/.qlib/qlib_data/my_data/baostock_raw_2016-2024'
    
    # 下载数据
    print("开始下载股票数据...")
    download_stock_data(START_DATE, END_DATE, DATA_DIR)
    
    print("下载完成!")