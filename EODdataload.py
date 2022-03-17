import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType, DecimalType

def applyLatest(trade):
    latest_arrival = trade.groupBy("trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb").agg(F.collect_set("trade_price").alias("trade_prices"))
    corrected_data = trade.join(latest_arrival, ["trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb"])
    return corrected_data

spark = SparkSession.builder.master('local').appName('app').getOrCreate()

trade_common = spark.read.parquet("output_dir/partition=T")
trade = trade_common.select('trade_dt', 'symbol', 'exchange', 'event_tm','event_seq_nb', 'trade_price')

quote_common = spark.read.parquet("output_dir/partition=Q")
quote = quote_common.select ('trade_dt', 'symbol', 'exchange', 'event_tm','event_seq_nb', 'trade_price')

trade_corrected = applyLatest(trade)
quote_corrected = applyLatest(quote)

