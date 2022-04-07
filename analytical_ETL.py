from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, TimestampType, DecimalType

#load the spark session
spark = SparkSession.builder.master('local').appName('app').getOrCreate()
#read parqut files from local storage
df = spark.read.parquet("/Users/kobihancz/Desktop/Springboard_Data_Engineering/Equity_Market_Analysis_Pipeline/output_dir/partition=Q".format("2020-07-29"))
#create a temporary trade table with date partition '202-07-29'
df = spark.sql("select trade_dt, symbol, exchange, event_tm, event_seq_nb, trade_price from trades where trade_dt = '2020-07-29'")
#create a spark temporary view
df.createOrReplaceGlobalTempView("tmp_trade_moving_avg")
#aclculate 30 min moving average 
mov_avg_df = spark.sql("""
    select trade_dt, symbol, exchange, event_tm, event_seq_nb, trade_price,
    AVG(trade_price) OVER (PARTITION BY (symbol, exchange)
    ORDER BY CAST(event_tm AS timestamp)
    RANGE BETWEEN INTERVAL 30 MINUTES PRECEDING AND CURRENT ROW) as mov_avg_pr
    from tmp_trade_moving_avg""")
#save temporary view 
mov_avg_df.write.saveAsTable("temp_trade_moving_avg")
mov_avg_df.show()


#Previouse date value
prev_date_str = "2020-07-28"
#use spark to read the trade table with date partition on previouse value 
prev_date_df = spark.sql("select symbol, event_tm, event_seq_nb, trade_price from trades where trade_dt = '{}'".format(prev_date_str))
prev_date_df.show()
#create a temporary view
prev_date_df.createOrReplaceTempView("tmp_last_trade")
#Calculate last trade price
last_trade_df = spark.sql("""select symbol, exchange, event_tm from
(select trade_dt, symbol, exchange, event_tm, event_seq_nb, trade_price,
AVG(trade_price)
OVER (PARTITION BY (symbol)
ORDER BY CAST(event_tm as timestamp)
RANGE BETWEEN INTERVAL 30 MINUTES PRECEDING AND CURRENT ROW) as last_pr
FROM tmp_trade_moving_avg) a
""")
#Save temporary view(calculated tables)
last_trade_df.write.saveAsTable("temp_last_trade")


#store quotes data in a df then create a temporary view
quotes_df = spark.read.parquet("/Users/kobihancz/Desktop/Springboard_Data_Engineering/Equity_Market_Analysis_Pipeline/output_dir/partition=Q".format("2020-07-29"))
quotes_df.createOrReplaceTempView("quotes") 
#create union of both tables
quote_union = spark.sql("""
SELECT trade_dt, rec_type, event_seq_nb, exchange, symbol, event_tm, rec_type,  bid_price, bid_size, ask_price,
ask_size , null as mov_avg_pr, null as trade_price FROM quotes
UNION ALL
SELECT trade_dt, "Q" as rec_type, symbol, exchange, event_tm,event_seq_nb, null as bid_size, null as bid_price, null as ask_price,
null as ask_size,trade_price,mov_avg_pr FROM temp_trade_moving_avg
""")
quote_union.createOrReplaceTempView("quote_union")
quote_union.show()


#gt latest trade_pr and move_avg_pr then populate
quote_union_update = spark.sql("""
select *,
last_value(trade_price, true) OVER (PARTITION BY symbol, exchange ORDER BY event_tm ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as last_trade_price,
last_value(mov_avg_pr, true) OVER (PARTITION BY symbol, exchange ORDER BY event_tm ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as last_mov_avg_pr
from quote_union
""")
quote_union_update.createOrReplaceTempView("quote_union_update")
# quote_union_update.show()


#filter for quote records
quote_update = spark.sql("""
    select trade_dt, symbol, event_tm, event_seq_nb, exchange,
    bid_price, bid_size, ask_price, ask_size, last_trade_price, last_mov_avg_pr
    from quote_union_update
    where rec_type = 'Q' 
""")
quote_update.createOrReplaceTempView("quote_update")
#quote_update.show()


#join the quote records with temp_last_trade to get prior day close price
quote_final = spark.sql("""
    select trade_dt, symbol, event_tm, event_seq_nb, exchange,
    bid_price, bid_size, ask_price, ask_size, last_trade_price, last_mov_avg_pr,
    bid_price - close_price as bid_pr_mv,
    ask_price - close_price as ask_pr_mv
    from (
    select /* + BROADCAST(t) */
    q.trade_dt, q.symbol, q.event_tm, q.event_seq_nb, q.exchange, q.bid_price, q.bid_size, q.ask_price, q.ask_size, q.last_trade_price,q.last_mov_avg_pr,
    t.last_pr as close_price
    from quote_update q
    left outer join temp_last_trade t on
    (q.symbol = t.symbol and q.exchange = t.exchange))
""")
quote_final.show()


