from multiprocessing import Pool
import multiprocessing
import pandas as pd
import numpy as np
from pathlib import Path
import shutil
from datetime import datetime
from tqdm import tqdm
import qlib

def create_qlib_structure(base_dir="~/.qlib/qlib_data/custom_cn_data"):
    """Create standard Qlib directory structure"""
    base_path = Path(base_dir).expanduser()
    
    # Create main directories
    directories = ['calendars', 'features', 'instruments']
    for dir_name in directories:
        (base_path / dir_name).mkdir(parents=True, exist_ok=True)
    
    return base_path

def prepare_calendars(df, base_path):
    """Prepare trading calendar data"""
    trading_dates = df.index.get_level_values('datetime').unique()
    trading_dates = sorted(trading_dates.strftime('%Y-%m-%d').tolist())
    
    calendar_file = base_path / 'calendars' / 'day.txt'
    with open(calendar_file, 'w') as f:
        f.write('\n'.join(trading_dates))

def prepare_instruments(df, base_path):
    """Prepare stock list data in format: stock_code start_date end_date"""
    stocks = df.index.get_level_values('instrument').unique()
    
    instrument_records = []
    for stock in stocks:
        stock_data = df.xs(stock, level='instrument')
        start_date = stock_data.index.min().strftime('%Y-%m-%d')
        end_date = stock_data.index.max().strftime('%Y-%m-%d')
        stock_name = "".join([stock.split(".")[0].upper(), stock.split(".")[1]])
        instrument_records.append(f"{stock_name}\t{start_date}\t{end_date}")
    
    instruments_file = base_path / 'instruments' / 'all.txt'
    with open(instruments_file, 'w') as f:
        f.write('\n'.join(instrument_records))


# def process_single_stock_features(args):
#     """Process features for a single stock"""
#     stock, stock_data, features_dir = args
#     stock_data = stock_data.ffill()
    
#     try:
#         stock_dir_name = stock
#         stock_dir = features_dir / stock_dir_name
#         stock_dir.mkdir(exist_ok=True)
        
#         # 获取第一个特征的长度作为标准
#         first_feature = next(col for col in stock_data.columns if col.startswith('$'))
#         standard_length = len(stock_data[first_feature])
        
#         # 首先创建并保存factor文件
#         factor_array = np.ones(standard_length, dtype="<f")
#         factor_file = stock_dir / 'factor.day.bin'
#         with factor_file.open("wb") as fp:
#             factor_array.tofile(fp)
        
#         # 然后处理每个特征
#         for column in stock_data.columns:
#             if column.startswith('$'):
#                 feature_data = stock_data[column].sort_index()

                
#                 # 确保数据是数值类型
#                 if feature_data.dtype == object:
#                     feature_data = pd.to_numeric(feature_data, errors='coerce')
                
#                 feature_array = feature_data.to_numpy().astype("<f")
                

                
#                 # 保存特征数据
#                 feature_name = column[1:]  # Remove '$' prefix
#                 feature_file = stock_dir / f'{feature_name}.day.bin'
#                 with feature_file.open("wb") as fp:
#                     feature_array.tofile(fp)
                
#                 if stock == 'sh.000300':
#                     print(f"Processing {column}...")  # Debug print
#                     # print(f"Feature data sample: {feature_data.head()}")  # Debug print
#                     # print(f"Feature data dtype: {feature_data.dtype}")  # Debug print
#                     print(f"Feature data sample: {feature_data.head()}")  # Debug print
#                     print(f"Feature data dtype: {feature_data.dtype}")  # Debug print
#                     # 验证数组是否包含数据
#                     print(f"Array stats - min: {feature_array.min()}, max: {feature_array.max()}, mean: {feature_array.mean()}")
#                     print(f"Array shape: {feature_array.shape}")
#                     # 验证写入的文件
#                     loaded_data = np.fromfile(feature_file, dtype="<f")
#                     print(f"Loaded data shape: {loaded_data.shape}")
#                     print(f"Loaded data stats - min: {loaded_data.min()}, max: {loaded_data.max()}, mean: {loaded_data.mean()}\n")
        
#         return f"Successfully processed {stock}"
#     except Exception as e:
#         return f"Error processing {stock}: {str(e)}"
    
# def process_single_stock_features(args):
#     """Process features for a single stock"""
#     stock, stock_data, features_dir = args
#     stock_data = stock_data.ffill()
    
#     try:
#         stock_dir_name = stock
#         stock_dir = features_dir / stock_dir_name
#         stock_dir.mkdir(exist_ok=True)
        
#         # 获取第一个特征的长度作为标准
#         first_feature = next(col for col in stock_data.columns if col.startswith('$'))
#         standard_length = len(stock_data[first_feature])
        
#         # 将索引转换为datetime并获取日期
#         dates = pd.to_datetime(stock_data.index.get_level_values('datetime'))
#         dates_array = dates.map(lambda x: x.toordinal() - 719163).values  # convert dates to qlib format
        
#         # 创建并保存factor文件
#         factor_array = np.ones(standard_length, dtype="<f4")
#         full_factor_array = np.concatenate([dates_array.astype("<f4"), factor_array])
        
#         factor_file = stock_dir / 'factor.day.bin'
#         with factor_file.open("wb") as fp:
#             full_factor_array.tofile(fp)
        
#         # 处理每个特征
#         for column in stock_data.columns:
#             if column.startswith('$'):
#                 feature_data = stock_data[column].sort_index()
#                 feature_array = feature_data.to_numpy().astype("<f4")
                
#                 # 添加日期信息
#                 full_feature_array = np.concatenate([dates_array.astype("<f4"), feature_array])
                
#                 feature_name = column[1:]  # Remove '$' prefix
#                 feature_file = stock_dir / f'{feature_name}.day.bin'
#                 with feature_file.open("wb") as fp:
#                     full_feature_array.tofile(fp)
        
#         return f"Successfully processed {stock}"
#     except Exception as e:
#         return f"Error processing {stock}: {str(e)}"
    

def process_single_stock_features(args):
    """Process features for a single stock"""
    stock, stock_data, features_dir = args
    stock_data = stock_data.ffill()
    
    try:
        stock_dir_name = stock
        stock_dir = features_dir / stock_dir_name
        stock_dir.mkdir(exist_ok=True)
        
        for column in stock_data.columns:
            if column.startswith('$'):
                feature_data = stock_data[column]
                
                # 转换为numpy数组并确保类型为float32
                feature_array = feature_data.to_numpy().astype('<f4')
                
                print(f"\nProcessing {column}...")
                print(f"Original values: {feature_data.head().values}")
                print(f"Converted values: {feature_array[:5]}")
                
                # 保存特征数据
                feature_name = column[1:]
                feature_file = stock_dir / f'{feature_name}.day.bin'
                
                # 直接写入feature数据
                with feature_file.open("wb") as fp:
                    feature_array.tofile(fp)
                
                # 验证写入的数据
                loaded_data = np.fromfile(feature_file, dtype='<f4')
                print(f"Loaded values: {loaded_data[:5]}")
                assert np.allclose(feature_array[:5], loaded_data[:5]), "Data mismatch!"
        
        return f"Successfully processed {stock}"
    except Exception as e:
        print(f"Error details for {stock}:\n{str(e)}")
        return f"Error processing {stock}: {str(e)}"
    
    
def prepare_features(df, base_path):
    """Prepare feature data using parallel processing"""
    features_dir = base_path / 'features'
    features_dir.mkdir(exist_ok=True)
    
    # Print debug info for first stock
    first_stock = df.index.get_level_values('instrument')[0]
    stock_data = df.xs(first_stock, level='instrument')
    print(f"\nDebug info for first stock: {first_stock}")
    print("Stock data sample:")
    print(stock_data.head())
    print("\nColumns:", stock_data.columns.tolist())
    
    
    # Prepare arguments for parallel processing
    stocks = df.index.get_level_values('instrument').unique()
    
    
    # for stock in stocks:
    #     if stock == 'sh.000300':
    #         import pdb; pdb.set_trace()
    #     stock_data = df.xs(stock, level='instrument')
    #     args = (stock, stock_data, features_dir)
    #     process_single_stock_features(args)
    
    
    # 多进程
    process_args = []
    for stock in stocks:
        stock_data = df.xs(stock, level='instrument')
        process_args.append((stock, stock_data, features_dir))
    
    # Determine number of processes
    n_processes = min(multiprocessing.cpu_count(), len(stocks))
    
    # Process stocks in parallel
    with Pool(processes=n_processes) as pool:
        results = list(tqdm(
            pool.imap(process_single_stock_features, process_args),
            total=len(process_args),
            desc="Processing stocks"
        ))
    
    # Print results summary
    success_count = sum(1 for r in results if r.startswith("Successfully"))
    error_count = len(results) - success_count
    
    print(f"\nProcessing complete:")
    print(f"Successfully processed: {success_count} stocks")
    print(f"Errors encountered: {error_count} stocks")
    
    # Print errors if any
    if error_count > 0:
        print("\nError details:")
        for result in results:
            if not result.startswith("Successfully"):
                print(result)


def process_single_stock(csv_file_path):
    """Process single stock data"""
    df = pd.read_csv(csv_file_path)
    # df = df.dropna(subset=['close', 'open', 'high', 'low'])  # Only drop rows with NaN in essential columns
    # df = df.drop(["adjustflag"], axis=1)
    
    # Basic data processing
    df['date'] = pd.to_datetime(df['date'])
    
    # New column mappings based on your data format
    column_mappings = {
        'code': 'instrument',
        'date': 'datetime',
        'open': '$open',
        'close': '$close',
        'high': '$high',
        'low': '$low',
        'preclose': '$prev_close',
        'volume': '$volume',
        'amount': '$amount',
        'turn': '$turnover_rate',
        'pctChg': '$returns',
        'peTTM': '$pe',
        'pbMRQ': '$pb',
        'psTTM': '$ps',
        'pcfNcfTTM': '$pcf',
        'isST': '$isST'
    }
    
    
    
    # Only rename columns that exist in the data
    existing_columns = {k: v for k, v in column_mappings.items() if k in df.columns}
    df = df.rename(columns=existing_columns)
    
    # Set index
    df = df.set_index(['datetime', 'instrument'])
    
    # Convert numeric columns to float
    numeric_columns = [col for col in df.columns if col.startswith('$')]
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df

def import_to_qlib_structure(csv_dir="~/.qlib/qlib_data/custom_cn_data/baostock_raw", 
                           output_dir="~/.qlib/qlib_data/my_data"):
    """Main function: Import CSV data into Qlib structure"""
    base_path = create_qlib_structure(output_dir)
    
    # csv_path = Path(csv_dir).expanduser()
    # all_data = []
    
    # # Process all stock files
    # stock_files = list(csv_path.glob('*.csv'))
    # print(f"Found {len(stock_files)} stock files")
    
    # for file in tqdm(stock_files, desc="Processing files"):
    #     if file.name.startswith(('sh.', 'sz.')):  # Only process stock files
    #         try:
    #             df = process_single_stock(file)
    #             all_data.append(df)
    #         except Exception as e:
    #             print(f"Error processing {file}: {str(e)}")
    
    # print("Combining all data...")
    # combined_df = pd.concat(all_data)
    
    
    # print("Preparing calendars...")
    # # prepare_calendars(combined_df, base_path)
    
    # combined_df.to_csv('combined_df.csv')
    
    # 读取CSV,需要指定index_col参数
    combined_df = pd.read_csv('combined_df.csv', index_col=[0,1])  # [0,1]表示第0、1列作为索引
    # import pdb; pdb.set_trace()
    # print("Preparing instruments...")
    # prepare_instruments(combined_df, base_path)
    
    print("Preparing features...")
    prepare_features(combined_df, base_path)
    
    return base_path



def validate_qlib_data(provider_uri, instruments=["sh.000300"]):
    """Validate the imported data with improved error handling"""
    qlib.init(
        provider_uri=provider_uri,
        region="cn",
        expression_cache=None,
    )
    
    from qlib.data import D
    
    # 规范化股票代码 - 这里需要注意大小写
    normalized_instruments = [inst.split('.')[0].upper() + "." + inst.split('.')[1] for inst in instruments]
    
    fields = [
        "$open",
        "$high", 
        "$low",
        "$close",
        "$prev_close",
        "$volume",
        "$amount",
        "$turnover_rate",
        "$returns",
        "$pe",
        "$pb",
        "$ps",
        "$pcf",
        "$isST",
        "$factor"
    ]
    
    try:
        df = D.features(
            normalized_instruments,
            fields=fields,
            freq="day",
            start_time='2018-01-01',
            end_time='2024-11-29'
        )
        print("\nData validation sample:")
        print(df.head())
        
        # 检查每个字段的文件是否存在
        import os
        base_path = Path(provider_uri)
        stock_path = base_path / 'features' / normalized_instruments[0].lower()
        print("\nChecking files in directory:", stock_path)
        for field in fields:
            field_name = field[1:]  # remove $
            file_path = stock_path / f"{field_name}.day.bin"
            if os.path.exists(file_path):
                data = np.fromfile(file_path, dtype='<f')
                print(f"\n{field} file:")
                print(f"- Path exists: {os.path.exists(file_path)}")
                print(f"- File size: {os.path.getsize(file_path)} bytes")
                print(f"- Data points: {len(data)}")
                print(f"- Sample values: {data[:5]}")
        
        return df
    except Exception as e:
        print(f"Error during validation: {str(e)}")
        raise

# def validate_qlib_data(provider_uri, instruments=["sh.000300"]):
#     """Validate the imported data with improved error handling"""
#     qlib.init(
#         provider_uri=provider_uri,
#         region="cn",
#         expression_cache=None,
#     )
    
#     from qlib.data import D
    
#     # 规范化股票代码
#     normalized_instruments = [inst.upper() for inst in instruments]
    
#     # 使用所有的字段
#     fields = [
#         "$open",
#         "$high", 
#         "$low",
#         "$close",
#         "$prev_close",
#         "$volume",
#         "$amount",
#         "$turnover_rate",
#         "$returns",
#         "$pe",
#         "$pb",
#         "$ps",
#         "$pcf",
#         "$isST",
#         "$factor"
#         # '$adjclose'
#     ]
    
#     try:
#         df = D.features(
#             normalized_instruments,
#             fields=fields,
#             freq="day",
#             start_time='2018-01-01',
#             end_time='2024-11-29'
#         )
#         print("\nData validation sample:")
#         print(df.head())
#         print("\nData info:")
#         print(df.info())
        
#         # 检查异常值
#         print("\nValue ranges:")
#         for col in df.columns:
#             print(f"\n{col}:")
#             print(f"Min: {df[col].min()}")
#             print(f"Max: {df[col].max()}")
#             print(f"Mean: {df[col].mean()}")
#             print(f"NaN count: {df[col].isna().sum()}")
        
#         return df
#     except Exception as e:
#         print(f"Error during validation: {str(e)}")
#         raise
    
    
    
def read_binary_file(file_path, n_display=10):
    """
    读取并分析factor.day.bin文件
    
    Parameters:
    -----------
    file_path : str or Path
        二进制文件路径
    n_display : int
        显示前n个数据点
        
    Returns:
    --------
    dict : 包含文件分析结果
    """
    file_path = Path(file_path)
    
    # 读取二进制文件
    data = np.fromfile(file_path, dtype='<f')  # little-endian float32
    
    # 分析结果
    result = {
        "文件路径": str(file_path),
        "文件大小": f"{file_path.stat().st_size:,} bytes",
        "数据点数量": len(data),
        "第一个值(date_index)": data[0],
        f"前{n_display}个值": data[:n_display],
        f"后{n_display}个值": data[-n_display:],
        "全部是1？": np.allclose(data[1:], 1.0),
        "数值统计": {
            "最小值": data[1:].min(),  # 跳过date_index
            "最大值": data[1:].max(),
            "平均值": data[1:].mean(),
            "标准差": data[1:].std(),
        }
    }
    
    return result

if __name__ == "__main__":
    output_path = "/home/tangziyi/.qlib/qlib_data/my_data"
    
    # # Import data
    # output_path = import_to_qlib_structure(output_dir=str(output_path))
    # print(f"Data successfully imported to: {output_path}")
    
    # # Validate data
    validation_df = validate_qlib_data(str(output_path), instruments=["sz.300452"])
    
    
    # r = read_binary_file("/home/tangziyi/.qlib/qlib_data/my_data/features/sh.000300/open.day.bin")
    # print(r)
    # r = read_binary_file("/home/tangziyi/.qlib/qlib_data/my_data/features/sh.000300/factor.day.bin")
    # print(r)
    # r = read_binary_file("/home/tangziyi/.qlib/qlib_data/my_data/features/sh.000300/high.day.bin")
    # print(r)
    # r = read_binary_file("/home/tangziyi/.qlib/qlib_data/my_data/features/sh.000300/close.day.bin")
    # print(r)