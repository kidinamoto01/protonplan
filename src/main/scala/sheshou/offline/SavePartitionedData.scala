package sheshou.offline

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}

/**
  * Created by suyu on 17-4-27.
  */
object SavePartitionedData {
  def main(args: Array[String]) {
  /*  if (args.length < 2) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <src_path> is a list of one or more Kafka brokers
                            |  <dest_path> is a list of one or more kafka topics to consume from
        """.stripMargin)
      System.exit(1)
    }


    val Array(windowsloginpath, outputpath) = args
    println(windowsloginpath)
    println(outputpath)*/

    /* val conf = new SparkConf().setMaster("local[*]")
      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)*/
    val warehouseLocation = "hdfs:///user/hive/warehouse"

    val spark = SparkSession.builder().appName("Spark Hive Example").config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("SET spark.sql.hive.convertMetastoreParquet=false")
    //"spark.master", "local"
    //specify spark datawarehouse
    //    "spark.sql.warehouse.dir", warehouseLocation
    //  spark.conf.set("spark.master", "local")
    import spark.sql
    val netstdType = sqlContext.read.csv("/tmp/netstds_type").toDF("id","category","catdesc","subcategory","subdesc")

   // sql("SELECT COUNT(*) FROM global_temp.net_type ").show()

    netstdType.printSchema()

    netstdType.write.mode(SaveMode.Append).format("parquet").partitionBy("category","subcategory").saveAsTable("typetable")
   // sqlContext.sql("select * from typetable").show

  }
//usr/hdp/2.5.3.0-37/spark2/bin/spark-submit --class "sheshou.offline.SavePartitionedData"  --master local[*] /usr/protonplan-spark-1.0/lib/protonplan-1.0-SNAPSHOT.jar

}
