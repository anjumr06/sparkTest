from numpy import *
import time
from datetime import datetime
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
  import argparse
  parser = argparse.ArgumentParser(description="Insight Analytics: Apache Spark [PySpark]")
  parser.add_argument("--min", dest="kpi_min", default=0.0, type=float, help="Min value")
  parser.add_argument("--max", dest="kpi_max", default=0.0, type=float, help="Max value")
  parser.add_argument("--inputLevel3", dest="input_l3", help="Level3 input file location")

  parser.add_argument("--outputLevel3", dest="output_l3", help="Level3 output file location")

  parser.add_argument("--startDate", dest="start_date", help="Start date")
  parser.add_argument("--endDate", dest="end_date", help="end date")
	
  args = parser.parse_args()
	
  kpi_min, kpi_max, input_l3, output_l3, start_date, end_date = args.kpi_min, args.kpi_max, args.input_l3, args.output_l3, args.start_date, args.end_date
  
  if kpi_min and kpi_max and input_l3 and output_l3 and start_date and end_date :
    conf = SparkConf().setAppName("SparkSQL Evaluation Level3")
	sc = SparkContext(conf=conf)
	l3 = sc.textFile(input_l3).coalesce(4)
	d3 = l3.map(lambda z: z.replace('"','')).map(lambda z: z.split(',')).map(lambda p: (str(p[0]), float(p[1]), float(p[2])))
	field3 = [StructField("CONSUMER_ID", StringType(), False), StructField("KPI_1", FloatType(), True), StructField("KPI_2", FloatType(), True)]
	s3 = StructType(field3)
	schema3 = sqlContext.applySchema(d3, s3)
	schema3.registerTempTable("Level3")
	sqlContext.cacheTable("Level3")
	
	query3 = sqlContext.sql("SELECT SEGMENT_ID, SLAB, COUNT(*), SUM(KPI_1), MIN(KPI_1), MAX(KPI_1), AVG(KPI_1)\
                FROM \
                (SELECT KPI_1, CASE WHEN KPI_1 >= 10.0 AND KPI_1 <= 50.0 THEN 1 ELSE 0 END SEGMENT_ID, \
                CASE WHEN KPI_1 >= 10.0 AND KPI_1 <= 50.0 THEN '10.0 - 50.0' ELSE NULL END SLAB \
                FROM Level3) DUMP \
                WHERE SLAB IS NOT NULL \
                GROUP BY SLAB, SEGMENT_ID \
                ORDER BY SEGMENT_ID")
	query3.saveAsTextFile(output_l3)
