import pandas as pd
import argparse

def convert_stock_code(input_file):
    # 读取原始文件
    with open(input_file, 'r') as f:
        lines = f.readlines()
    
    # 转换格式
    converted_lines = []
    for line in lines:
        parts = line.strip().split('\t')
        if len(parts) >= 1:
            code = parts[0]
            # 转换代码格式
            if code.startswith('SH'):
                new_code = f"sh{code[2:]}"
            elif code.startswith('SZ'):
                new_code = f"sz{code[2:]}"
            else:
                assert code.startswith('sh') or code.startswith('sz')
                new_code = code
                
            # 保持原始的日期信息
            if len(parts) >= 3:
                converted_line = f"{new_code}\t{parts[1]}\t{parts[2]}"
            else:
                converted_line = new_code
                
            converted_lines.append(converted_line)
    
    # 直接覆盖原文件
    with open(input_file, 'w') as f:
        for line in converted_lines:
            f.write(line + '\n')

if __name__ == "__main__":
    # 设置命令行参数
    parser = argparse.ArgumentParser(description='Convert stock codes format in a file')
    parser.add_argument('--input_file', required=True, type=str, help='Path to the input file')
    
    # 解析命令行参数
    args = parser.parse_args()
    
    # 执行转换
    convert_stock_code(args.input_file)