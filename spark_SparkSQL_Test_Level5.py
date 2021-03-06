from numpy import *
import time
from datetime import datetime
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import *

if __name__ == "__main__":
  import argparse
  parser = argparse.ArgumentParser(description="Insight Analytics: Apache Spark [PySpark]")
  parser.add_argument("--min", dest="kpi_min", default=0.0, type=float, help="Min value")
  parser.add_argument("--max", dest="kpi_max", default=0.0, type=float, help="Max value")
  parser.add_argument("--inputLevel3", dest="input_l3", help="Level3 input file location")
  parser.add_argument("--inputLevel5", dest="input_l5", help="Level5 input file location")
  parser.add_argument("--outputLevel5", dest="output_l5", help="Level5 output file location")
  parser.add_argument("--startDate", dest="start_date", help="Start date")
  parser.add_argument("--endDate", dest="end_date", help="end date")
	
  args = parser.parse_args()
	
  kpi_min, kpi_max, input_l3, input_l5, output_l5, start_date, end_date = args.kpi_min, args.kpi_max, args.input_l3, args.input_l5, args.output_l5, args.start_date, args.end_date
	
  if kpi_min and kpi_max and input_l5 and start_date and end_date:
    conf = SparkConf().setAppName("SparkSQL Evaluation Level5")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    l3 = sc.textFile(input_l3).coalesce(4)
    d3 = l3.map(lambda z: z.replace('"','')).map(lambda z: z.split(',')).map(lambda p: (str(p[0]), float(p[1]), float(p[2])))
    field3 = [StructField("CONSUMER_ID", StringType(), False), StructField("KPI_1", FloatType(), True), StructField("KPI_2", FloatType(), True)]
    s3 = StructType(field3)
    schema3 = sqlContext.applySchema(d3, s3)
    schema3.registerTempTable("Level3")
            
    l5 = sc.textFile(input_l5).coalesce(24)
    d5= l5.map(lambda z: z.replace('"','')).map(lambda z: z.split(',')).map(lambda p: (p[0], p[1], datetime.strptime(p[2],"%Y-%m-%d %H:%M:%S").date(), int(p[3])))
    field5 = [StructField("CONSUMER_ID", StringType(), False), StructField("CAMPAIGN_NAME", StringType(), True), StructField("EVENT_DATE", DateType(), True), StructField("EVENT_TYPE_ID", IntegerType(), True)]
    s5 = StructType(field5)
    schema5 = sqlContext.applySchema(d5, s5)
    schema5.registerTempTable("Level5")
     
    level3_1 = sqlContext.sql("SELECT CONSUMER_ID, KPI_1 FROM Level3 \
                            WHERE KPI_1 >= " + str(kpi_min) + " AND KPI_1 <= "+ str(kpi_max) +" ")
    data3_1 = level3_1.map(lambda z: (str(z[0]), float(z[1])))
    field3_1 = [StructField("CONSUMER_ID", StringType(), False), StructField("KPI", FloatType(), True)]
    s3_1 = StructType(field3_1)
    schema3_1 = sqlContext.applySchema(data3_1, s3_1)
    schema3_1.registerTempTable("Level3_1")
    sqlContext.cacheTable("Level3_1") 


    level5_1 = sqlContext.sql("SELECT CONSUMER_ID, CAMPAIGN_NAME, EVENT_TYPE_ID FROM Level5 WHERE EVENT_DATE BETWEEN '"+ start_date +"' AND '" + end_date + "'")
    data5_1 = level5_1.map(lambda z: (str(z[0]), str(z[1]), int(z[2])))
    field5_1 = [StructField("CONSUMER_ID", StringType(), False), StructField("CAMPAIGN_NAME", StringType(), False), StructField("EVENT_TYPE_ID", IntegerType(), False)]
    s5_1 = StructType(field5_1)
    schema5_1 = sqlContext.applySchema(data5_1, s5_1)
    schema5_1.registerTempTable("Level5_1")
    sqlContext.cacheTable("Level5_1")
    
    queryJoin = sqlContext.sql("SELECT CAMPAIGN_NAME, SUM(CASE WHEN EVENT_TYPE_ID = 5 THEN 1 ELSE 0 END) targeted,\
                            SUM(CASE WHEN EVENT_TYPE_ID = 15 THEN 1 ELSE 0 END) converted, \
                            SUM(CASE WHEN EVENT_TYPE_ID = 30 THEN 1 ELSE 0 END) cg, \
                            SUM(CASE WHEN EVENT_TYPE_ID = 32 THEN 1 ELSE 0 END) cg_converted \
                            FROM (SELECT l5.CAMPAIGN_NAME CAMPAIGN_NAME, l5.EVENT_TYPE_ID EVENT_TYPE_ID FROM Level5_1 l5, Level3_1 l3\
                            WHERE l5.CONSUMER_ID = l3.CONSUMER_ID) A GROUP BY CAMPAIGN_NAME") 
                            
    queryJoin.coalesce(1).saveAsTextFile(output_l5)
