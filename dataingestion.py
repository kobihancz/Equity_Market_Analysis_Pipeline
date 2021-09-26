import pyspark
import json
from pyspark.sql import SparkSession
from typing import List
from datetime import datetime
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType, FloatType, DateType, \
    TimestampType

# open a spark session,give access to azure storageaccount, and provide location of source data in azure container 
spark = SparkSession.builder.master('local').appName('app').getOrCreate()
spark.conf.set("fs.azure.account.key.equitystorageaccount.blob.core.windows.net", \
    "/6U8afbBOiSnfyZTTrr0DUs9vkIK+SDiqnnnyaN2BD3KBy+r9gcb8Voie6a5zJ4y2rNQYNKK2F7HYYf0DTjQWw==")

filenamecsv = "wasbs://equitycontainer@equitystorageaccount.blob.core.windows.net/csv"
filenamejson = "wasbs://equitycontainer@equitystorageaccount.blob.core.windows.net/json"

# using csv_parser to process the source data 
raw = spark.sparkContext.textFile(filenamecsv)
parsed = raw.map(lambda line: parse_csv(line))
data = spark.createDataFrame(parsed)
data.show()
data.write.partitionBy("partition").mode("overwrite").parquet("output_dir")

# using json_parser to parse the source data
rawj = spark.sparkContext.textFile(filenamejson)
parsedj = rawj.map(lambda line: parse_json(line))
dataj = spark.createDataFrame(parsedj)
dataj.write.partitionBy("partition").mode("overwrite").parquet("output_dir")

print(data.count())
print(dataj.count())

# csv parser
def parse_csv(line):
    record_type_pos = 2

    record = line.split(",")

    trade_dt = datetime.strptime(record[0], '%Y-%m-%d').date()
    event_tm = datetime.strptime(record[4], "%Y-%m-%d %H:%M:%S.%f")
    file_tm = datetime.strptime(record[1], "%Y-%m-%d %H:%M:%S.%f")
    event_seq_nb = int(record[5])
    trade_pr = float(record[7])
    trade_size = int(record[8])
    bid_pr = float(record[7])
    bid_size = int(record[8])

    try:
        if record[record_type_pos] == "T":
            event = (trade_dt, record[2], record[3], record[6], event_tm, event_seq_nb, file_tm, trade_pr, trade_size,
                     None, None, None, None, "T")
            return event
        elif record[record_type_pos] == "Q":
            ask_pr = float(record[9])
            ask_size = int(record[10])
            event = (trade_dt, record[2], record[3], record[6], event_tm, event_seq_nb, file_tm, None,
                     None, bid_pr, bid_size, ask_pr, ask_size, "Q")
            return event
    except Exception as e:
            event = (None, None, None, None, None, None, None, None, None, None, None, None, None, "B")
            return event


# json parser
def parse_json(line):
    record = json.loads(line)

    record_type = record['event_type']
    trade_dt = datetime.strptime(record['trade_dt'], '%Y-%m-%d').date()
    event_tm = datetime.strptime(record['event_tm'], "%Y-%m-%d %H:%M:%S.%f")
    file_tm = datetime.strptime(record['file_tm'], "%Y-%m-%d %H:%M:%S.%f")
    event_seq_nb = int(record['event_seq_nb'])

    try:
        if record_type == "T":  
            if record['trade_dt'] != "" and record['event_type'] != "" and record['symbol'] != "" and record[
                'event_tm'] != "" and record['event_seq_nb'] != "":
                eventj = (trade_dt, record['event_type'], record['symbol'], record['exchange'],
                          event_tm, event_seq_nb, file_tm, record['price'],
                          record['size'], None, None, None, None, "T")
            else:
                eventj = (None, None, None, None, None, None, None, None, None, None, None, None, None, "B")

            return eventj

        elif record_type == "Q":
            if record['trade_dt'] != "" and record['event_type'] != "" and record['symbol'] != "" and record[
                'event_tm'] != "" and record['event_seq_nb'] != "":
                eventj = (trade_dt, record['event_type'], record['symbol'], record['exchange'],
                          event_tm, event_seq_nb, file_tm, None,
                          None, record['bid_pr'], record['bid_size'], record['ask_pr'],
                          record['ask_size'], "Q")
            else:
                eventj = (None, None, None, None, None, None, None, None, None, None, None, None, None, "B")

            return eventj

    except Exception as e:
        return (None, None, None, None, None, None, None, None, None, None, None, None, None, "B")