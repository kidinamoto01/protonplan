package sheshou.offline

import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}

/**
  * Created by suyu on 17-4-27.
  */
object SavePartitionedData {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <src_path> is a list of one or more Kafka brokers
                            |  <dest_path> is a list of one or more kafka topics to consume from
        """.stripMargin)
      System.exit(1)
    }


    val Array(windowsloginpath, outputpath) = args
    println(windowsloginpath)
    println(outputpath)

    /* val conf = new SparkConf().setMaster("local[*]")
      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)*/
    val spark = SparkSession.builder().appName("Spark Hive Example").config("spark.master", "local").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = new SQLContext(sc)
    //"spark.master", "local"
    //specify spark datawarehouse
    //    "spark.sql.warehouse.dir", warehouseLocation
    //  spark.conf.set("spark.master", "local")
    import spark.sql
    val netstdType = sqlContext.read.csv("/tmp/netstds_type")
    netstdType.createGlobalTempView("net_type")
    sql("SELECT COUNT(*) FROM global_temp.net_type ").show()

    netstdType.printSchema()
    netstdType.write.mode(SaveMode.Append)
      .format("parquet")
      .partitionBy("_c0")
      .saveAsTable("typepartitioned")

  }

}
