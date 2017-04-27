package sheshou.streaming


import java.util.{Calendar, Properties}

import kafka.serializer.StringDecoder
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by fenglu on 2017/4/20.
  */
object Readkafka {

  def attack(requetspage : String): String = {
    var result = "Nothing"
    //命令执行漏洞
    val commands=List( "|id", "|cat /etc/passwd#","'|'ld", "\"|\"ld","';cat /etc/passwd;'","\";cat /etc/passwd;\""
      , "||cat /etc/passwd", "'&dir&'", "\"&dir&\"", "|ping -n 20 127.0.0.1||x", "`ping -c 20 127.0.0.1`", "&ping -n 20 127.0.0.1&","|ping -c 20 127.0.0.1||x")

    commands.map{
      x=>
        if ( requetspage.contains(x))
          result =  "Web层攻击-命令执行漏洞"
    }

    //文件包含漏洞
    val packagefile = List("\\..\\..\\..\\..\\..\\..\\..\\..","/../../../../../../","\\\\..\\\\..\\\\..\\\\..\\\\..\\\\..\\\\..\\\\..\\\\","/\\..\\..\\..\\..\\", "/????/????/????/????/????/????/????/????",
      "/..??..??..??..??..??..","/..?..?..?..?..?..?..?.","./././././././././././././././.","boot.ini","win.ini", "/windows/win.ini",
      "/etc/passwd", "/WEB-INF/web.xml", "//../....//....//WEB-INF/web.xml")

    packagefile.map{
      x=>
        if ( requetspage.contains(x))
          result = "Web层攻击-文件包含漏洞"
    }

    //路径扫描
    val pathfile = List(".mdb","cmd.exe","fuck.php", "fck_select.html", "cao.php", "inc.asp.bak","ydxzdate.asa"
      ,"1.asp.asa","a.asp;(1).jpg","360.aspx", "6qv4myup.aspx","DataShop).aspx","admin_.aspx", "CEO.jsp","admin/go.jsp"
      , "install/install.jsp","readme.txt")

    pathfile.map{
      x=>
        if ( requetspage.contains(x))
          result = "Web层攻击-路径扫描"
    }

    //SQL注入攻击
    val sqlFile = List("user()","\"", "\'", "--", "/*", "/*!", "';", "-1' or", "-1 or",
      "order by", "b/**/y", "/*!by*/","IF(SUBSTR(@@version", "and sleep(", "' and sleep(","waitfor delay '",
      "@@version", "having 1=1--", "' and", "' || '' || '","' exec master..xp_cmdshell 'vol'--")

    sqlFile.map{
      x=>
        if ( requetspage.contains(x))
          result = "Web层攻击-SQL注入攻击"
    }

    //struts2漏洞
    val strutsFile = List()
    strutsFile.map{
      x=>
        if ( requetspage.contains(x))
          result = "Web层攻击-struts2漏洞"
    }

    //模糊测试
    val vagueFile = List(")))))))))))")
    vagueFile.map{
      x=>
        if ( requetspage.contains(x))
          result = "Web层攻击-模糊测试"
    }

    //XSS跨站脚本攻击
    val xssFile = List("\">", "\"><script>", "</textArea><script>", ";//", "alert%281%29","alert(1)", "alert(", "confirm("
      , "confirm%281%29", "prompt(","prompt%281%29", "onMouseOver","document.location", "document%2elocation%3d1", "document.title="
      ,"document%2etitle%3d1","<a>","<script>alert(1)</script>", "<?xml version=\"1.0\" encoding=\"utf-8\"?>", "<xxx>"
      , "--><")

    xssFile.map{
      x=>
        if ( requetspage.contains(x))
          result = "Web层攻击-XSS跨站脚本攻击"
    }

    return result
  }

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println(s"""
                            |Usage: DirectKafkaWordCount <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
        """.stripMargin)
      System.exit(1)
    }


    val Array(brokers, url, user, passwd) = args
    println(brokers)
    println(url)
    println(user)
    println(passwd)

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("SaveKafkaData")
    //sparkConf.set("spark.hadoop.parquet.enable.summary-metadata", "true")
    //sparkConf.set("spark.hadoop.parquet.enable.summary-metadata", "true")
    //spark.hadoop.parquet.enable.summary-metadata false
    val  sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(10))


    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
      "zookeeper.connect" -> "localhost:2181")

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams,  Set("webmiddle")).map(_._2)

    //    val messages2 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    //      ssc, kafkaParams, Set("netstds")).map(_._2)

    val messages3 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set("windowslogin")).map(_._2)

    val messgesnet = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set("netstdsonline")).map(_._2)

    //Get Date
    /*val cal = Calendar.getInstance()
    val date = cal.get(Calendar.DATE)
    val Year = cal.get(Calendar.YEAR)
    val Month1 = cal.get(Calendar.MONTH)
    val Month = Month1+1
    val Hour = cal.get(Calendar.HOUR_OF_DAY)*/

    // Get the lines
    //webmiddle
    /*
    messages.foreachRDD{ x =>

      //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      val sqlContext = new  HiveContext(sc)
      // val text = sqlContext.read.json(x)
      val text = sqlContext.read.json(x)

      //get json schame
      text.toDF().printSchema()

      //save text into parquet file
      //make sure the RDD is not empty
      if(text.count()>0)
      {

        println("write")

        //text.write.format("parquet").mode(SaveMode.Append).parquet("hdfs://192.168.1.21:8020/sheshou/data/parquet/"+"webmiddle"+"/"+Year+"/"+Month+"/"+date+"/"+Hour+"/")
        sqlContext.udf.register("attack", attack _)
        //val tmp = sqlContext.sql("select t.id,t.attack_time,t.destip as dst_ip, t.srcip as src_ip, t.attack_type, t.srccountrycode as src_country_code, t.srccountry as src_country, t.srccity as src_city,t.destcountrycode as dst_country_code,t.destcountry as dst_country,t.destcity as dst_city , t.srclatitude as src_latitude, t.srclongitude as src_longitude ,t.destlatitude as dst_latitude ,t.destlongitude as dst_longitude ,t.end_time,t.asset_id,t.asset_name,t.alert_level from (select \"0\" as id,loginresult , collecttime as attack_time, destip,srcip,\"forcebreak\" as attack_type ,srccountrycode,srccountry,srccity,destcountrycode,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,collectequpip,collecttime as end_time, count(*) as sum ,\"0\" as asset_id, \"name\" as asset_name,\"0\" as  alert_level from windowslogin group by loginresult,collecttime,destip,srcip,srccountrycode,srccountry,srccity,destcountrycode,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,collecttime,collectequpip)t where (t.sum > 2 and ( t.loginresult = 539 or t.loginresult = 529 or  t.loginresult = 528 ))")
        val tmp= sqlContext.sql("select \"0\" as id,collecttime as attack_time,  \"0\" as dst_ip,srcip as src_ip, attack(requestpage) as attack_type, srccountrycode as src_country_code, srccountry as src_country, srccity as src_city,\"0\" as dst_country_code,\"0\" as dst_country,\"0\" as dst_city,srclatitude as src_latitude, srclongitude as  src_longitude, \"0\" as dst_latitude,\"0\" as dst_longitude, collecttime as  end_time, id as asset_id,\"0\" as assent_name,warnlevel as alert_level ,year,month,day,hour from webmiddle where attack(requestpage) != \"Nothing\" ")

        tmp.printSchema()
        tmp.registerTempTable("webmiddle")
        sqlContext.sql("insert into sheshou.attack_list partition(year,month,day,hour) select * from webmiddle")

        //insert into  mysql
        val prop = new Properties()
        prop.setProperty("user", user)
        prop.setProperty("password", passwd)
        val dfWriter = tmp.write.mode("append").option("driver", "com.mysql.jdbc.Driver")

        dfWriter.jdbc(url, "attack_list", prop)
      }

    }


    // Get the lines
    //windowslogin
    messages3.foreachRDD{ x =>

      val sqlContext = new  HiveContext(sc)
      // val text = sqlContext.read.json(x)
      val text = sqlContext.read.json(x)

      //get json schame
      text.toDF().printSchema()

      //save text into parquet file
      //make sure the RDD is not empty
      if(text.count()>0)
      {

        println("write3")

        val result = sqlContext.sql("select t.id,t.attack_time,t.destip as dst_ip, t.srcip as src_ip, t.attack_type, t.srccountrycode as src_country_code, t.srccountry as src_country, t.srccity as src_city,t.destcountrycode as dst_country_code,t.destcountry as dst_country,t.destcity as dst_city , t.srclatitude as src_latitude, t.srclongitude as src_longitude ,t.destlatitude as dst_latitude ,t.destlongitude as dst_longitude ,t.end_time,t.asset_id,t.asset_name,t.alert_level from (select \"0\" as id,loginresult , collecttime as attack_time, destip,srcip,\"forcebreak\" as attack_type ,srccountrycode,srccountry,srccity,destcountrycode,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,collectequpip,collecttime as end_time, count(*) as sum ,\"0\" as asset_id, \"name\" as asset_name,\"0\" as  alert_level from windowslogin group by loginresult,collecttime,destip,srcip,srccountrycode,srccountry,srccity,destcountrycode,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,collecttime,collectequpip)t where t.sum > 2")
        result.registerTempTable("attacklist")
        result.printSchema()
        sqlContext.sql("insert into sheshou.attack_list partition(year,month,day,hour) select * from attacklist")
        //insert into  mysql
        val prop = new Properties()
        prop.setProperty("user", user)
        prop.setProperty("password", passwd)
        val dfWriter = result.write.mode("append").option("driver", "com.mysql.jdbc.Driver")

        dfWriter.jdbc(url, "attack_list", prop)
      }

    }
*/
    //netstds
        messgesnet.foreachRDD { x =>
          val sqlContext = new HiveContext(sc)
          // val text = sqlContext.read.json(x)
          val text = sqlContext.read.json(x)

          //get json schame
          text.printSchema()

          //save text into parquet file
          //make sure the RDD is not empty
          if (text.count() > 0) {

            text.registerTempTable("netstds")

            val netstdType = sqlContext.read.csv("hdfs://192.168.1.21:8020/tmp/netstds_type")
            netstdType.registerTempTable("netstdtype")
            netstdType.printSchema()

            //(select _c1 as category ,_c2 as attack_type, _c3 as subcategory from netstdtype)t
            val mergedata = sqlContext.sql("select * from netstds left join  netstdtype on netstds.category = netstdtype._c1 and  netstds.subcategory = netstdtype._c3 ")
            mergedata.registerTempTable("result_table")
            mergedata.printSchema()

            val tmp= sqlContext.sql("select \"0\" as id, time as attack_time,  dstIP as dst_ip,srcip as src_ip, _c4 as attack_type, \"0\" as src_country_code, srcLocation as src_country, srcLocation as src_city,\"0\" as dst_country_code,dstLocation as dst_country,dstLocation as dst_city,srclatitude as src_latitude, srclongitude as  src_longitude, dstLatitude as dst_latitude,dstLongitude as dst_longitude, time as  end_time, \"0\" as asset_id,toolName as assent_name,\"0\" as alert_level  from result_table ")

            tmp.registerTempTable("attacklist")
            //insert into table
            sqlContext.sql("insert into sheshou.attack_list partition(year,month,day,hour) select * from attacklist")


            //insert into  mysql

         /*   val prop = new Properties()
            prop.setProperty("user", user)
            prop.setProperty("password", passwd)
            val dfWriter = tmp.write.mode("append").option("driver", "com.mysql.jdbc.Driver")
            dfWriter.jdbc(url, "attack_list", prop)*/
          }
        }
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
