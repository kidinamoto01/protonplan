package sheshou.offline

import java.util.{Calendar, Properties}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}

/**
  * Created by suyu on 17-4-13.
  */
object ReadParquetData {

  def main(args: Array[String]) {

    println("args is "+args)

    val Array(filepath, outputpath,user,password,url) = args
    println(filepath)
    println(outputpath)
    println(user)
    println(password)
    println(url)

    val conf = new SparkConf().setAppName("Offline Doc Application")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //read json file

    val file =sqlContext.read.parquet(filepath)//.toDF()

    val temptable = file.registerTempTable("windowslogin") //where loginresult = 537 or loginresult = 529

    val tmp = sqlContext.sql("select t.id,t.attack_time,t.destip as dst_ip, t.srcip as src_ip, t.attack_type, t.srccountrycode as src_country_code, t.srccountry as src_country, t.srccity as src_city,t.destcountrycode as dst_country_code,t.destcountry as dst_country,t.destcity as dst_city , t.srclatitude as src_latitude, t.srclongitude as src_longitude ,t.destlatitude as dst_latitude ,t.destlongitude as dst_longitude ,t.end_time,t.asset_id,t.asset_name,t.alert_level from (select \"0\" as id,loginresult , collecttime as attack_time, destip,srcip,\"forcebreak\" as attack_type ,srccountrycode,srccountry,srccity,destcountrycode,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,collectequpip,collecttime as end_time, count(*) as sum ,\"0\" as asset_id, \"name\" as asset_name,\"0\" as  alert_level from windowslogin group by loginresult,collecttime,destip,srcip,srccountrycode,srccountry,srccity,destcountrycode,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,collecttime,collectequpip)t where t.sum > 2")

    tmp.printSchema()

    tmp.rdd.foreach(println)

    val cal = Calendar.getInstance()
    val date =cal.get(Calendar.DATE )
    val Year =cal.get(Calendar.YEAR )
    val Month1 =cal.get(Calendar.MONTH )
    val Month = Month1+1
    val Hour = cal.get(Calendar.HOUR_OF_DAY)

    tmp.write.mode(SaveMode.Append).save(outputpath+"/"+Year+"/"+Month+"/"+date+"/"+Hour+"/")
    tmp.write.format("parquet").mode(SaveMode.Append).parquet("/sheshou/data/parquet/realtime/break" + "/" + Year + "/" + Month + "/" + date + "/" + Hour + "/")
    val prop = new Properties()
    prop.setProperty("user", user)
    prop.setProperty("password", password)


    val dfWriter = tmp.write.mode("append").option("driver", "com.mysql.jdbc.Driver")

    dfWriter.jdbc(url, "attack_list", prop)
  }
}
