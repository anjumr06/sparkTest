import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.Row

var input = List(1,2)

val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

def getString(n: Int) : String = { 
  var str = ""; 
  for (i <- 1 to n) str = str.concat("kpi" + i.toString + " ")
  str 
}

def f(n: List[Int], s: String) : Row =
  Row.fromSeq(s.split(",").zipWithIndex.collect{case (a,b) if n.contains(b) => a.toDouble}.toSeq)


def f(input: List[Int], s: String) : Row = {
  val n = input.length
  var out = new Array[Any](n+1)
  var r = s.split(",")
  out(0) = r(0)
  for (i <- 1 to n)
    out(i) = r(input(i-1)).toDouble
  Row.fromSeq(out)
}

val str3 = getString(input.length)
val f3 = new Array[StructField](input.length+1)
val temp = str3.split(" ").map(fieldName => StructField(fieldName, DoubleType, true))
f3(0) = StructField("consumer_id", StringType, true);
for (i <- 1 to input.length) f3(i) = temp(i-1)
val s3 = StructType(f3)

val f5 = new Array[StructField](4)
f5(0) = StructField("consumer_id", StringType, true)
f5(1) = StructField("campaign_name", StringType, true)
f5(2) = StructField("event_date", LongType, true)
f5(3) = StructField("event_type_id", IntegerType, true)
val s5 = StructType(f5)

//Array[org.apache.spark.sql.Row] = Array([[Ljava.lang.Object;@234d2916])
val l3 = sc.textFile("/external/SparkTest/REPORT_IA_LEVEL3_1_3224/REPORT_IA_LEVEL3_1_3224.csv").map(_.split(" ")).map(r => (f(input,r(0))))
val l5 = sc.textFile("/external/SparkTest/REPORT_IA_LEVEL5_3224/REPORT_IA_LEVEL5_3224.csv").map(_.split(",")).map(r => Row(r(0),r(1),format.parse(r(2)).getTime(),r(3).toInt))


val l3RDD = sqlContext.applySchema(l3,s3)
val l5RDD = sqlContext.applySchema(l5,s5)

l3RDD.saveAsParquetFile("/SparkTest/Input/p_31.parquet")
l5RDD.saveAsParquetFile("/SparkTest/Input/p_51.parquet")

val Level3 = sqlContext.parquetFile("/SparkTest/Input/p_31.parquet")
val Level5 = sqlContext.parquetFile("/SparkTest/Input/p_51.parquet")

Level3.registerTempTable("Level3")
Level5.registerTempTable("Level5")

val targeted = sqlContext.sql("SELECT SEGMENT_ID, SLAB, COUNT(*), SUM(kpi1), MIN(kpi1), MAX(kpi1), AVG(kpi1) FROM (SELECT kpi1, CASE WHEN kpi1 >= 10.0 AND kpi1 <= 50.0 THEN 1 ELSE 0 END SEGMENT_ID, CASE WHEN kpi1 >= 10.0 AND kpi1 <= 50.0 THEN '10.0 - 50.0' ELSE NULL END SLAB FROM Level3) DUMP WHERE SLAB IS NOT NULL GROUP BY SLAB, SEGMENT_ID ORDER BY SEGMENT_ID")

targeted.coalesce(1).saveAsTextFile("/SparkTest/OutputScala/SQL/Level3_p")


val start_date = format.parse("2014-12-01 05:36:55").getTime()
val end_date = format.parse("2014-12-15 05:36:55").getTime()

val pivot_query = sqlContext.sql("SELECT campaign_name, SUM(CASE WHEN event_type_id = 5 THEN 1 ELSE 0 END) targeted, SUM(CASE WHEN event_type_id = 15 THEN 1 ELSE 0 END) converted, SUM(CASE WHEN event_type_id = 30 THEN 1 ELSE 0 END) cg, SUM(CASE WHEN event_type_id = 32 THEN 1 ELSE 0 END) cg_converted FROM (SELECT * FROM (SELECT DISTINCT l5.event_type_id, l5.consumer_id, l5.campaign_name FROM Level5 l5 WHERE l5.event_date >= " + start_date +" AND l5.event_date <= " + end_date +") l5, Level3 l3 WHERE l5.consumer_id = l3.consumer_id AND l3.kpi1 >= 2.0 AND l3.kpi1 <= 52.0) dmp GROUP BY campaign_name")

pivot_query.coalesce(1).saveAsTextFile("/SparkTest/OutputScala/SQL/Level5_p")
