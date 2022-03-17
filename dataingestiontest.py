from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, FloatType
from datetime import datetime
import json

def parse_csv(line: str):
    record_type_pos = 2
    record = line.split(',')
    try:
        if record[record_type_pos] == 'T':
            partition = 'T'
            trade_dt = datetime.strptime(record[1], '%Y-%m-%d %H:%M:%S.%f').date()
            rec_type = record[2]
            symbol = record[3]
            event_time = datetime.timestamp(datetime.strptime(record[4], '%Y-%m-%d %H:%M:%S.%f'))
            event_seq_num = int(record[5])
            exchange = record[6]
            trade_price = float(record[7])
            trade_size = int(record[8])
            bid_price = None
            bid_size = None
            ask_price = None
            ask_size = None

            event = (partition, trade_dt, rec_type, symbol, event_time, event_seq_num, exchange,
                                trade_price, trade_size, bid_price, bid_size, ask_price, ask_size,
                                )
            print("T", event)
            return event
        elif record[record_type_pos] == 'Q':
            partition = 'Q'
            trade_dt = datetime.strptime(record[1], '%Y-%m-%d %H:%M:%S.%f').date()
            rec_type = record[2]
            symbol = record[3]
            event_time = datetime.timestamp(datetime.strptime(record[4], '%Y-%m-%d %H:%M:%S.%f'))
            event_seq_num = int(record[5])
            exchange = record[6]
            trade_price = None
            trade_size = None
            bid_price = float(record[7])
            bid_size = int(record[8])
            ask_price = float(record[9])
            ask_size = int(record[10])

            event = (partition, trade_dt, rec_type, symbol, event_time, event_seq_num, exchange,
                                trade_price, trade_size, bid_price, bid_size, ask_price, ask_size,
                                )

            print("Q", event)
            return event
        else:
            raise Exception
    except Exception as e:
        partition = "B"
        trade_dt = None
        rec_type = None
        symbol = None
        event_time = None
        event_seq_num = None
        exchange = None
        trade_price = None
        trade_size = None
        bid_price = None
        bid_size = None
        ask_price = None
        ask_size = None
        event = (partition, trade_dt, rec_type, symbol, event_time, event_seq_num, exchange,
                 trade_price, trade_size, bid_price, bid_size, ask_price, ask_size,
                 )
        return event
#
def parse_json(line: str) :
    record_type_pos = 2
    record = json.loads(line)


    try:

        if record['event_type'] == 'T':

            partition = 'T'
            trade_dt = datetime.strptime(record['trade_dt'], '%Y-%m-%d').date()
            rec_type = record['event_type']
            symbol = record['symbol']
            event_time = datetime.timestamp(datetime.strptime(record['event_tm'], '%Y-%m-%d %H:%M:%S.%f'))
            event_seq_num = int(record['event_seq_nb'])
            exchange = record['exchange']
            trade_price = float(record['price'])
            trade_size = int(record['size'])
            bid_price = None
            bid_size = None
            ask_price = None
            ask_size = None

            event = (partition, trade_dt, rec_type, symbol, event_time, event_seq_num, exchange,
                     trade_price, trade_size, bid_price, bid_size, ask_price, ask_size
                     )
            print(event)
            return event
        elif record['event_type'] == 'Q':
            partition = 'Q'
            trade_dt = datetime.strptime(record['trade_dt'], '%Y-%m-%d').date()
            rec_type = record['event_type']
            symbol = record['symbol']
            event_time = datetime.timestamp(datetime.strptime(record['event_tm'], '%Y-%m-%d %H:%M:%S.%f'))
            event_seq_num = int(record['event_seq_nb'])
            exchange = record['exchange']
            trade_price = None
            trade_size = None
            bid_price = float(record['bid_pr'])
            bid_size = int(record['bid_size'])
            ask_price = float(record['ask_pr'])
            ask_size = int(record['ask_size'])

            event = (partition, trade_dt, rec_type, symbol, event_time, event_seq_num, exchange,
                     trade_price, trade_size, bid_price, bid_size, ask_price, ask_size
                     )

            print(event)
            return event
        else:
            raise Exception
    except Exception as e:
        partition = "B"
        trade_dt = None
        rec_type = None
        symbol = None
        event_time = None
        event_seq_num = None
        exchange = None
        trade_price = None
        trade_size = None
        bid_price = None
        bid_size = None
        ask_price = None
        ask_size = None
        event = (partition, trade_dt, rec_type, symbol, event_time, event_seq_num, exchange,
                 trade_price, trade_size, bid_price, bid_size, ask_price, ask_size,
                 )

        return event


schema = StructType([
    StructField("partition",StringType()),
    StructField("trade_dt",DateType()), #DateType
    StructField("rec_type",StringType()),
    StructField("symbol",StringType()),
    StructField("event_tm", FloatType()), #TimeStamptype
    StructField("event_seq_nb", IntegerType()),
    StructField("exchange", StringType()),
    StructField("trade_price", FloatType()),
    StructField("trade_size", IntegerType()),
    StructField("bid_price", FloatType()),
    StructField("bid_size", IntegerType()),
    StructField("ask_price", FloatType()),
    StructField("ask_size", IntegerType())
  ])

spark = SparkSession.builder.master('local').appName('app').getOrCreate()

filenamecsv1 = "/Users/kobihancz/Desktop/Springboard_Data_Engineering/Equity_Market_Analysis_Pipeline/data/csv/2020-08-05/NYSE/part-00000-5e4ced0a-66e2-442a-b020-347d0df4df8f-c000.txt"
filenamecsv2 = "/Users/kobihancz/Desktop/Springboard_Data_Engineering/Equity_Market_Analysis_Pipeline/data/csv/2020-08-06/NYSE/part-00000-214fff0a-f408-466c-bb15-095cd8b648dc-c000.txt" 
filenamejson1 = "/Users/kobihancz/Desktop/Springboard_Data_Engineering/Equity_Market_Analysis_Pipeline/data/json/2020-08-05/NASDAQ/part-00000-c6c48831-3d45-4887-ba5f-82060885fc6c-c000.txt"
filenamejson2 = "/Users/kobihancz/Desktop/Springboard_Data_Engineering/Equity_Market_Analysis_Pipeline/data/json/2020-08-06/NASDAQ/part-00000-092ec1db-39ab-4079-9580-f7c7b516a283-c000.txt"

#parse csv file 1
rawcsv1 = spark.sparkContext.textFile(filenamecsv1)
parsedcsv1 = rawcsv1.map(lambda line: parse_csv(line))
datacsv1 = spark.createDataFrame(parsedcsv1, schema)
datacsv1.show()
datacsv1.write.partitionBy("partition").mode("overwrite").parquet("output_dir")

#parse csv file 2
rawcsv2 = spark.sparkContext.textFile(filenamecsv2)
parsedcsv2 = rawcsv2.map(lambda line: parse_csv(line))
datacsv2 = spark.createDataFrame(parsedcsv2, schema)
datacsv2.show()
datacsv2.write.partitionBy("partition").mode("overwrite").parquet("output_dir")

# using json_parser to parse the source data
#parse json file 1 
rawj1 = spark.sparkContext.textFile(filenamejson1)
parsedj1 = rawj1.map(lambda line: parse_json(line))
dataj1 = spark.createDataFrame(parsedj1, schema)
dataj1.show()
dataj1.write.partitionBy("partition").mode("overwrite").parquet("output_dir")

#parse json file 2
rawj2 = spark.sparkContext.textFile(filenamejson2)
parsedj2 = rawj2.map(lambda line: parse_json(line))
dataj2 = spark.createDataFrame(parsedj2, schema)
dataj2.show()
dataj2.write.partitionBy("partition").mode("overwrite").parquet("output_dir")


