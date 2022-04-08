import datetime
import uuid
import json
import logging
from random import randint
import mysql.connector
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType #,StructField, StringType, IntegerType, DateType, FloatType

class Tracker:

    def __init__(self, jobname):
        self.jobname = jobname

    def assign_job_id(self):
        job_id = self.jobname + datetime.today().strftime("%Y%m%d")
        return job_id

    def update_job_status(self, status, assigned_job_id, connection_obj):
        job_id = self.assign_job_id()
        print("Job ID Assigned: {}".format(job_id))
        update_time = datetime.now()

        try:
            dbCursor = connection_obj.cursor()
            job_sql_command = "INSERT INTO tracker_table(id, status, time) " \
                              "VALUES('" + "1" + "', '" + status + "', '" + str(update_time) + "')"
            dbCursor.execute(job_sql_command)
            dbCursor.close()
            print("Inserted data into job tracker table.")
        except (Exception, mysql.connector.Error) as error:
            return logging.error("Error executing db statement for job tracker.", error)

    def get_job_status(self, job_id):
        # connect db and send sql query
        table = self.get_db_connection()
        try:
            query = "JobID eq '%s'" % (job_id)
            records = list(table.query_entities(query))
            last_record = dict(records[-1])
            last_record_status = last_record['Status']
            return last_record_status
        except Exception as error:
            print("Error getting job status: ", error)
        return

    def get_db_connection(self):
        try:

            mydb = mysql.connector.connect(
                host="host",  # hostname
                user="user",  # the user who has privilege to the db
                password="password",  # password for user
                database="guidedcapstone",  # database name
                auth_plugin='mysql_native_password',

            )
class Reporter(object):
    """
    job_id, status, updated_time
    """
    def __init__(self, jobname, dbconfig):
        self.jobname = jobname
        self.dbconfig = dbconfig
    
    def report(self):
        return

def run_reporter_etl(my_config):
    trade_date = my_config.get('production', 'processing_date')
    reporter = Reporter(spark, my_config)
    tracker = Tracker('analytical_etl', my_config)
    try:
        reporter.report(spark, trade_date, eod_dir)
        tracker.update_job_status("success")
    except Exception as e:
        print(e)
        tracker.update_job_status("failed")
    return