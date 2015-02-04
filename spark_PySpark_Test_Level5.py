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
  parser.add_argument("--inputLevel5", dest="input_l5", help="Level5 input file location")
  parser.add_argument("--outputLevel5", dest="output_l5", help="Level5 output file location")
  parser.add_argument("--startDate", dest="start_date", help="Start date")
  parser.add_argument("--endDate", dest="end_date", help="end date")
	
  args = parser.parse_args()
	
  kpi_min, kpi_max, input_l3, input_l5, output_l5, start_date, end_date = args.kpi_min, args.kpi_max, args.input_l3, args.input_l5, args.output_l5, args.start_date, args.end_date
	
  if kpi_min and kpi_max and input_l5 and start_date and end_date:
    conf = SparkConf().setAppName("PySpark Evaluation - Level 5")
    sc = SparkContext(conf=conf)
    l3 = sc.textFile(input_l3)
    l5 = sc.textFile(input_l5).coalesce(24)
    p3 = l3.map(lambda z: z.replace('"','')).map(lambda z: (z.split(',')[0],(z.split(',')[1],z.split(',')[2])))
    p5 = l5.map(lambda z: z.replace('"','')).map(lambda z: (z.split(',')[0],(z.split(',')[1:])))
    p5.filter(lambda z: (z[1][1] >= start_date) and (z[1][1] <= end_date)).join(p3.filter(lambda z: (float(z[1][1])  >= kpi_min) and (float(z[1][1]) <= kpi_max))).map(lambda z: (z[1][0][0], (1 if z[1][0][2] == '5' else 0, 1 if z[1][0][2] == '15' else 0, 1 if z[1][0][2] == '30' else 0, 1 if z[1][0][2] == '32' else 0))).reduceByKey(add).coalesce(1).saveAsTextFile(output_l5)
