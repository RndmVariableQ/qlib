import qlib
import pandas as pd
from qlib.constant import REG_CN
from qlib.utils import exists_qlib_data, init_instance_by_config
from qlib.workflow import R
from qlib.workflow.record_temp import SignalRecord, PortAnaRecord
from qlib.utils import flatten_dict



import qlib
import pandas as pd
from qlib.constant import REG_CN
from qlib.utils import exists_qlib_data, init_instance_by_config
from qlib.workflow import R
from qlib.workflow.record_temp import SignalRecord, PortAnaRecord
from qlib.utils import flatten_dict




# use default data
# NOTE: need to download data from remote: python scripts/get_data.py qlib_data_cn --target_dir ~/.qlib/qlib_data/cn_data
provider_uri = "~/.qlib/qlib_data/my_data"  # target_dir
if not exists_qlib_data(provider_uri):
    print(f"Qlib data is not found in {provider_uri}")
    sys.path.append(str(scripts_dir))
    from get_data import GetData

    GetData().qlib_data(target_dir=provider_uri, region=REG_CN)
qlib.init(provider_uri=provider_uri, region=REG_CN)



market = "csi300"
benchmark = "SH000300"
fields = [
    "$open", "$high", "$low", "$close", "$preclose", "$volume", "$turn", "$pctChg", "$pettm", "$pbmrq", # "$adjclose"
]


###################################
# prediction, backtest & analysis
###################################
# 定义数据处理器配置
data_handler_config = {
    "start_time": "2018-12-01",
    "end_time": "2024-12-01",
    "instruments": "csi300",
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
    "learn_processors": [
        {"class": "Fillna", "kwargs": {"fields_group": "feature", "fill_value": 0}},  # 首先填充特征的NaN
        {"class": "DropnaLabel", "kwargs": {"fields_group": "label"}},    # 填充标签的NaN
        {"class": "CSZScoreNorm", "kwargs": {"fields_group": "feature"}},
        {"class": "CSRankNorm", "kwargs": {"fields_group": "label"}},
    ],
    "infer_processors": [
        {"class": "Fillna", "kwargs": {"fields_group": "feature", "fill_value": 0}},
        {"class": "CSZScoreNorm", "kwargs": {
            "fields_group": "feature"
        }},
        
    ],
}

task = {
    "model": {
        "class": "LGBModel",
        "module_path": "qlib.contrib.model.gbdt",
        "kwargs": {
            "loss": "mse",
            "colsample_bytree": 0.8879,
            "learning_rate": 0.0421,
            "subsample": 0.8789,
            "lambda_l1": 205.6999,
            "lambda_l2": 580.9768,
            "max_depth": 8,
            "num_leaves": 210,
            "num_threads": 20,
            "num_boost_round": 500,
            "early_stopping_rounds": 50,
            "verbose_eval": 100,
        },
    },
    "dataset": {
        "class": "DatasetH",
        "module_path": "qlib.data.dataset",
        "kwargs": {
            "handler": {
                "class": "DataHandlerLP",
                "module_path": "qlib.data.dataset.handler",
                "kwargs": data_handler_config,
            },
            "segments": {
                "train": ("2018-12-01", "2022-12-01"),
                "valid": ("2022-12-01", "2023-12-01"),
                "test": ("2023-12-01", "2024-12-01"),
            },
        },
    },
}

# model initialization
model = init_instance_by_config(task["model"])
dataset = init_instance_by_config(task["dataset"])

# start exp to train model
with R.start(experiment_name="train_model"):
    R.log_params(**flatten_dict(task))
    model.fit(dataset)
    R.save_objects(trained_model=model)
    rid = R.get_recorder().id
    
    
    

###################################
# prediction, backtest & analysis
###################################
port_analysis_config = {
    "executor": {
        "class": "SimulatorExecutor",
        "module_path": "qlib.backtest.executor",
        "kwargs": {
            "time_per_step": "day",
            "generate_portfolio_metrics": True,
        },
    },
    "strategy": {
        "class": "TopkDropoutStrategy",
        "module_path": "qlib.contrib.strategy",
        "kwargs": {"signal": "<PRED>", "topk": 50, "n_drop": 5},
                },
    "backtest": {
        "start_time": "2023-12-01",
        "end_time": "2024-12-01",
        "account": 100000000,
        "benchmark": benchmark,
        "exchange_kwargs": {
            "freq": "day",
            "limit_threshold": 0.0095,
            "deal_price": "open",
            "open_cost": 0.0005,
            "close_cost": 0.0015,
            "min_cost": 5,
        },
    },
}

# backtest and analysis
with R.start(experiment_name="backtest_analysis"):
    recorder = R.get_recorder(recorder_id=rid, experiment_name="train_model")
    model = recorder.load_object("trained_model")

    # prediction
    recorder = R.get_recorder()
    ba_rid = recorder.id
    sr = SignalRecord(model, dataset, recorder)
    sr.generate()

    # backtest & analysis
    par = PortAnaRecord(recorder, port_analysis_config, "day")
    par.generate()
    
    
    
    
    
from qlib.contrib.report import analysis_model, analysis_position
from qlib.data import D
recorder = R.get_recorder(recorder_id=ba_rid, experiment_name="backtest_analysis")
print(recorder)
pred_df = recorder.load_object("pred.pkl")
report_normal_df = recorder.load_object("portfolio_analysis/report_normal_1day.pkl")
positions = recorder.load_object("portfolio_analysis/positions_normal_1day.pkl")
analysis_df = recorder.load_object("portfolio_analysis/port_analysis_1day.pkl")

fig = analysis_position.report_graph(report_normal_df, False)[0]
# fig.show()
fig.write_image('backtest_report.pdf')