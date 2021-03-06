package sheshou.streaming


import java.util.{Calendar, Properties}

import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
//import sheshou.streaming.{GetMysqlConnection, Stats}

/**
  * Created by fenglu on 2017/4/20.
  */
object Readkafka {

  val log = Logger.getLogger("Readkafka")

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
    val  sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(30))


    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,
      "zookeeper.connect" -> "localhost:2181")

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams,  Set("webmiddle")).map(_._2)

    val messages3 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set("windowslogin")).map(_._2)

    val messgesnet = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set("netstdsonline")).map(_._2)

    val messgeVirus = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set("reportvirus")).map(_._2)

    val messgeCve = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set("reportcve")).map(_._2)

    val messgeOe = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set("attachmentoe")).map(_._2)

    //webmiddle
    messages.foreachRDD{ x =>

      val hiveContext = new  HiveContext(sc)
      hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
      val text = hiveContext.read.json(x)

      //get json schame
      text.toDF().printSchema()

      //save text into parquet file
      //make sure the RDD is not empty
      if(text.count()>0)
      {
        println("write")
        hiveContext.udf.register("attack", attack _)
        text.registerTempTable("webmiddle")
        val tmp= hiveContext.sql("select \"0\" as id,collecttime as attack_time,  \"0\" as dst_ip,srcip as src_ip, attack(requestpage) as attack_type, srccountrycode as src_country_code, srccountry as src_country, srccity as src_city,\"0\" as dst_country_code,\"0\" as dst_country,\"0\" as dst_city,srclatitude as src_latitude, srclongitude as  src_longitude, \"0\" as dst_latitude,\"0\" as dst_longitude, collecttime as  end_time, \"0\" as asset_id,\"0\" as asset_name,warnlevel as alert_level ,year,month,day,hour from webmiddle where attack(requestpage) != \"Nothing\" ")
        tmp.registerTempTable("attacklist")
        tmp.printSchema()
        //统计小时，天数据
        val stat = new Stats(hiveContext,tmp,url,user,passwd)
        stat.stat()

        hiveContext.sql("insert into sheshou.attack_list partition(`year`,`month`,`day`,`hour`) select id,  attack_time, dst_ip, src_ip,  attack_type, src_country_code,  src_country, src_city,dst_country_code, dst_country, dst_city, src_latitude,  src_longitude, dst_latitude, dst_longitude,  end_time, asset_id,asset_name,alert_level,year,month,day,hour from attacklist ")
        //"0" as id,
        val mysqlDF = hiveContext.sql("select \"0\" as id, attack_time, dst_ip, src_ip,  attack_type, src_country_code,  src_country, src_city,dst_country_code, dst_country, dst_city, src_latitude,  src_longitude, dst_latitude, dst_longitude,  end_time, asset_id,asset_name,alert_level from attacklist ")
        //insert into  mysql
        val prop = new Properties()
        prop.setProperty("user", user)
        prop.setProperty("password", passwd)
        val dfWriter = mysqlDF.write.mode("append").option("driver", "com.mysql.jdbc.Driver")

        dfWriter.jdbc(url, "attack_list", prop)
      }
    }


    // Get the lines
    //windowslogin
    messages3.foreachRDD{ x =>

      val hiveContext = new  HiveContext(sc)
      // val text = sqlContext.read.json(x)
      val text = hiveContext.read.json(x)

      //get json schame
      text.toDF().printSchema()

      //save text into parquet file
      //make sure the RDD is not empty
      if(text.count()>0)
      {

        println("write3")
        hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
        text.registerTempTable("windowslogin")
        val result = hiveContext.sql("select t.id,t.attack_time,t.destip as dst_ip, t.srcip as src_ip, t.attack_type, t.srccountrycode as src_country_code, t.srccountry as src_country, t.srccity as src_city,t.destcountrycode as dst_country_code,t.destcountry as dst_country,t.destcity as dst_city , t.srclatitude as src_latitude, t.srclongitude as src_longitude ,t.destlatitude as dst_latitude ,t.destlongitude as dst_longitude ,t.end_time,t.asset_id,t.asset_name,t.alert_level,t.year,t.month,t.day,t.hour from (select \"0\" as id,loginresult , collecttime as attack_time, destip,srcip,\"暴力破解\" as attack_type ,srccountrycode,srccountry,srccity,destcountrycode,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,collectequpip,collecttime as end_time, count(*) as sum ,\"0\" as asset_id, \"name\" as asset_name,\"0\" as  alert_level ,year,month,day,hour from windowslogin group by loginresult,collecttime,destip,srcip,srccountrycode,srccountry,srccity,destcountrycode,destcountry,destcity,srclatitude,srclongitude,destlatitude,destlongitude,collecttime,collectequpip,year,month,day,hour)t where t.sum > 2")
        //分析天，小时数据
        val stat =new Stats(hiveContext,result,url,user,passwd)
        stat.stat()

        result.registerTempTable("attacklist")
        result.printSchema()
        hiveContext.sql("insert into sheshou.attack_list partition(`year`,`month`,`day`,`hour`) select  id,  attack_time, dst_ip, src_ip,  attack_type, src_country_code,  src_country, src_city,dst_country_code, dst_country, dst_city, src_latitude,  src_longitude, dst_latitude, dst_longitude,  end_time, asset_id,asset_name,alert_level,year,month,day,hour  from attacklist")
        //insert into  mysql
        val mysqlDF = hiveContext.sql("select \"0\" as id, attack_time, dst_ip, src_ip,  attack_type, src_country_code,  src_country, src_city,dst_country_code, dst_country, dst_city, cast((case when src_latitude is NULL then 0.0 else src_latitude end) as float) as src_latitude,  cast((case when src_longitude is NULL then 0.0 else src_longitude end) as float) as src_longitude, cast((case when dst_latitude is NULL then 0.0 else dst_latitude end) as float) as dst_latitude, cast((case when dst_longitude is NULL then 0.0 else dst_longitude end) as float) as dst_longitude,  end_time, asset_id,asset_name,alert_level from attacklist ")

        val prop = new Properties()
        prop.setProperty("user", user)
        prop.setProperty("password", passwd)

        val dfWriter = mysqlDF.write.mode("append").option("driver", "com.mysql.jdbc.Driver")

        dfWriter.jdbc(url, "attack_list", prop)

      }
    }

    //netstds
    messgesnet.foreachRDD { x =>
      val hiveContext = new HiveContext(sc)
      // val text = sqlContext.read.json(x)
      val text = hiveContext.read.json(x)

      //get json schame
      text.printSchema()

      //save text into parquet file
      //make sure the RDD is not empty
      if (text.count() > 0) {

        hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
        text.registerTempTable("netstds")

        val netstdType = hiveContext.read.csv("/tmp/netstds_type").toDF("ids","category_type","desc1","subcategory_type","desc2")
        netstdType.registerTempTable("netstdtype")
        netstdType.printSchema()

        //(select _c1 as category ,_c2 as attack_type, _c3 as subcategory from netstdtype)t
        val mergedata = hiveContext.sql("select * from netstds left join  netstdtype on netstds.category = netstdtype.category_type and  netstds.subCategory = netstdtype.subcategory_type")

        println("this is datafram")
        mergedata.foreach(println(_))

        mergedata.registerTempTable("result_table")
        mergedata.printSchema()

        val tmp= hiveContext.sql("insert into sheshou.attack_list partition(`year`,`month`,`day`,`hour`) select \"0\" as id,  attack_time, dst_ip, src_ip, desc2 as attack_type, src_country_code,  src_country, src_city,dst_country_code, dst_country, dst_city, src_latitude,  src_longitude, dst_latitude, dst_longitude,  end_time, \"0\" as asset_id,asset_name,\"0\" as alert_level,year,month,day,hour  from result_table ")
        tmp.printSchema()
        tmp.registerTempTable("attacklist")
        log.info("insert into hive is ")
        tmp.show()

        //分析天，小时数据
        val stat = new Stats(hiveContext,mergedata,url,user,passwd)
        stat.stat()

        //insert into  mysql
        //        val mysqlDF = hiveContext.sql("select \"0\" as id, attack_time, dst_ip, src_ip,  attack_type, src_country_code,  src_country, src_city,dst_country_code, dst_country, dst_city, src_latitude,  src_longitude, dst_latitude, dst_longitude,  end_time, asset_id,asset_name,alert_level from  attacklist")
        val mysqlDF = hiveContext.sql("select \"0\" as id,  attack_time, dst_ip, src_ip, desc2 as attack_type, src_country_code,  src_country, src_city,dst_country_code, dst_country, dst_city, src_latitude,  src_longitude, dst_latitude, dst_longitude,  end_time, \"0\" as asset_id,asset_name,\"0\" as alert_level  from result_table")
        val prop = new Properties()
        prop.setProperty("user", user)
        prop.setProperty("password", passwd)
        val dfWriter = mysqlDF.write.mode("append").option("driver", "com.mysql.jdbc.Driver")
        dfWriter.jdbc(url, "attack_list", prop)
      }
    }

    //report_virus
    messgeVirus.foreachRDD { x =>
      val hiveContext = new HiveContext(sc)
      // val text = sqlContext.read.json(x)
      val text = hiveContext.read.json(x)

      //get json schame
      text.printSchema()

      //save text into parquet file
      //make sure the RDD is not empty
      if (text.count() > 0) {

        hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
        text.registerTempTable("messgevirus")
        text.printSchema()

       hiveContext.sql("insert into sheshou.attack_list partition(`year`,`month`,`day`,`hour`) select \"0\" as id,  attack_time, dst_ip, src_ip,  attack_type, src_country_code,  src_country, src_city,dst_country_code, dst_country, dst_city, src_latitude,  src_longitude, dst_latitude, dst_longitude,  end_time, \"0\" as asset_id,asset_name,\"0\" as alert_level,year,month,day,hour  from messgevirus ")

        log.info("insert into hive is ")

        //分析天，小时数据
        val stat = new Stats(hiveContext,text,url,user,passwd)
        stat.stat()

        //insert into  attack_list@mysql
        val mysqlDF = hiveContext.sql("select \"0\" as id,  attack_time, dst_ip, src_ip,  attack_type, src_country_code,  src_country, src_city,dst_country_code, dst_country, dst_city, src_latitude,  src_longitude, dst_latitude, dst_longitude,  end_time, \"0\" as asset_id,asset_name,\"0\" as alert_level  from messgevirus")
        val prop = new Properties()
        prop.setProperty("user", user)
        prop.setProperty("password", passwd)
        val dfWriter = mysqlDF.write.mode("append").option("driver", "com.mysql.jdbc.Driver")
        dfWriter.jdbc(url, "attack_list", prop)
      }
    }

    //report_cve
    messgeCve.foreachRDD { x =>
      val hiveContext = new HiveContext(sc)
      // val text = sqlContext.read.json(x)
      val text = hiveContext.read.json(x)

      //get json schame
      text.printSchema()

      //save text into parquet file
      //make sure the RDD is not empty
      if (text.count() > 0) {

        hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
        text.registerTempTable("messagecve")
        text.printSchema()

        hiveContext.sql("insert into sheshou.attack_list partition(`year`,`month`,`day`,`hour`) select \"0\" as id,  attack_time, dst_ip, src_ip,  attack_type, src_country_code,  src_country, src_city,dst_country_code, dst_country, dst_city, src_latitude,  src_longitude, dst_latitude, dst_longitude,  end_time, \"0\" as asset_id,asset_name,\"0\" as alert_level,year,month,day,hour  from messagecve ")

        log.info("insert into hive is ")

        //分析天，小时数据
        val stat = new Stats(hiveContext,text,url,user,passwd)
        stat.stat()

        //insert into  attack_list@mysql
        val mysqlDF = hiveContext.sql("select \"0\" as id,  attack_time, dst_ip, src_ip,  attack_type, src_country_code,  src_country, src_city,dst_country_code, dst_country, dst_city, src_latitude,  src_longitude, dst_latitude, dst_longitude,  end_time, \"0\" as asset_id,asset_name,\"0\" as alert_level  from messagecve")
        val prop = new Properties()
        prop.setProperty("user", user)
        prop.setProperty("password", passwd)
        val dfWriter = mysqlDF.write.mode("append").option("driver", "com.mysql.jdbc.Driver")
        dfWriter.jdbc(url, "attack_list", prop)
      }
    }

    //attachment_oe
    messgeOe.foreachRDD { x =>
      val hiveContext = new HiveContext(sc)
      // val text = sqlContext.read.json(x)
      val text = hiveContext.read.json(x)

      //get json schame
      text.printSchema()

      //save text into parquet file
      //make sure the RDD is not empty
      if (text.count() > 0) {

        hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
        text.registerTempTable("messageoe")
        text.printSchema()

        hiveContext.sql("insert into sheshou.attack_list partition(`year`,`month`,`day`,`hour`) select \"0\" as id,  attack_time, dst_ip, src_ip,  attack_type, src_country_code,  src_country, src_city,dst_country_code, dst_country, dst_city, src_latitude,  src_longitude, dst_latitude, dst_longitude,  end_time, \"0\" as asset_id,asset_name,\"0\" as alert_level,year,month,day,hour  from messageoe")

        log.info("insert into hive is ")

        //分析天，小时数据
        val stat = new Stats(hiveContext,text,url,user,passwd)
        stat.stat()

        //insert into  attack_list@mysql
        val mysqlDF = hiveContext.sql("select \"0\" as id,  attack_time, dst_ip, src_ip,  attack_type, src_country_code,  src_country, src_city,dst_country_code, dst_country, dst_city, src_latitude,  src_longitude, dst_latitude, dst_longitude,  end_time, \"0\" as asset_id,asset_name,\"0\" as alert_level  from messageoe")
        val prop = new Properties()
        prop.setProperty("user", user)
        prop.setProperty("password", passwd)
        val dfWriter = mysqlDF.write.mode("append").option("driver", "com.mysql.jdbc.Driver")
        dfWriter.jdbc(url, "attack_list", prop)
      }
    }

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}