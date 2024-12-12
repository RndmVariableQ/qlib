import pandas as pd
import os
import numpy as np
import shutil

def add_factor_column(source_dir, target_dir):
    """
    读取源目录下的CSV文件，添加factor列后保存到目标目录
    同时处理股票代码格式：将 sh.xxxxxx 和 sz.xxxxxx 转换为 shxxxxxx 和 szxxxxxx
    
    Parameters:
    source_dir (str): 源CSV文件所在的目录路径
    target_dir (str): 处理后文件保存的目标目录路径
    """
    # 创建目标目录（如果不存在）
    os.makedirs(target_dir, exist_ok=True)
    
    # 获取源目录下所有CSV文件
    csv_files = [f for f in os.listdir(source_dir) if f.endswith('.csv')]
    
    for csv_file in csv_files:
        source_path = os.path.join(source_dir, csv_file)
        # 处理文件名中的股票代码格式
        target_path = os.path.join(target_dir, csv_file.replace("sh.", "sh").replace("sz.", "sz"))
        
        try:
            # 读取CSV文件
            df = pd.read_csv(source_path)
            
            # 如果存在code列，处理股票代码格式
            if 'code' in df.columns:
                df['code'] = df['code'].str.replace('sh.', 'sh', regex=False).str.replace('sz.', 'sz', regex=False)
            
            # 添加factor列，与文件长度相同
            df['factor'] = np.ones(len(df))
            
            # 保存到新目录
            df.to_csv(target_path, index=False)
            print(f"Successfully processed {csv_file}")
            
        except Exception as e:
            print(f"Error processing {csv_file}: {str(e)}")

# 使用示例
source_dir = "~/.qlib/qlib_data/custom_cn_data/baostock_raw/"
target_dir = "~/.qlib/qlib_data/my_data/baostock_raw/"

# 展开用户目录的波浪符号
source_dir = os.path.expanduser(source_dir)
target_dir = os.path.expanduser(target_dir)

add_factor_column(source_dir, target_dir)