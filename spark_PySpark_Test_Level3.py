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
	
  if kpi_min and kpi_max and input_l3 and start_date and end_date:
    conf = SparkConf().setAppName("PySpark Evaluation - Level3")
    sc = SparkContext(conf=conf)
    l3 = sc.textFile(input_l3).coalesce(2)
    p3 = l3.map(lambda z: z.replace('"','')).map(lambda z: (z.split(',')[0],(z.split(',')[1],z.split(',')[2])))
    x = p3.filter(lambda z: (float(z[1][0])  >= kpi_min) and (float(z[1][0]) <= kpi_max)).map(lambda z: float(z[1][0])).stats()
    l3_out = sc.parallelize([x.count(), x.sum(), x.min(), x.max(),  x.mean(), x.stdev()])
    l3_out.coalesce(1).saveAsTextFile(output_l3)
