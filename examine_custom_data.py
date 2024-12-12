import qlib
from qlib.data.dataset.handler import DataHandlerLP
from qlib.config import REG_CN

# 定义要获取的字段
fields = [
    "$open", "$high", "$low", "$close", "$volume", "$turn", "$pettm"
]

# 定义数据处理器配置
data_handler_config = {
    "start_time": "2018-01-01",
    "end_time": "2025-08-01",
    # "fit_start_time": "2008-01-01",
    # "fit_end_time": "2014-12-31",
    "instruments": "csi500",
    # 定义数据加载器配置
    "data_loader": {
        "class": "QlibDataLoader",
        "kwargs": {
            "config": {
                "feature": fields,
                "label": ["Ref($close, -1) / $close - 1"]
            },
            "freq": "day"
        }
    },
}

if __name__ == "__main__":
    # 初始化 Qlib
    qlib.init(provider_uri='~/.qlib/qlib_data/my_data', region=REG_CN)
    
    # 创建数据处理器实例
    h = DataHandlerLP(**data_handler_config)
    
    # 获取所有列名
    print("All columns:")
    print(h.get_cols())
    
    # 获取标签数据
    print("\nLabels:")
    print(h.fetch(col_set="label"))
    
    # 获取特征数据
    print("\nFeatures (OHLCV):")
    print(h.fetch(col_set="feature"))