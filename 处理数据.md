First, download raw data from BaoStock:
```
python get_baostock_data.py
```


Convert CSV Format into Qlib Format:
```
python scripts/dump_bin.py dump_all ... \
--include_fields open,high,low,close,preclose,volume,amount,turn,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST,factor \
--csv_path  ~/.qlib/qlib_data/my_data/baostock_raw/ \
--qlib_dir ~/.qlib/qlib_data/my_data \
--date_field_name date \
--symbol_field_name code
```


Collect calendar data:
```
python scripts/data_collector/future_calendar_collector.py --qlib_dir ~/.qlib/qlib_data/my_data/ --region cn
```


Download CSI consistuent stocks and transform the stock code format:
```
python scripts/data_collector/cn_index/collector.py --index_name CSI100 --qlib_dir ~/.qlib/qlib_data/my_data/ --method parse_instruments
python scripts/data_collector/cn_index/collector.py --index_name CSI300 --qlib_dir ~/.qlib/qlib_data/my_data/ --method parse_instruments
python scripts/data_collector/cn_index/collector.py --index_name CSI500 --qlib_dir ~/.qlib/qlib_data/my_data/ --method parse_instruments
```