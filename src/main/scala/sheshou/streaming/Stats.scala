package sheshou.streaming

import java.sql.{Connection, DriverManager}

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import java.util.List;

/**
  * Created by fenglu on 2017/4/27.
  */
class Stats (sqlcontext:SQLContext,dataFrame:DataFrame,dburl:String,user:String,password:String){

  val log = Logger.getLogger("Stats")

  val hoursql = "select sum(1) as count, DATE_FORMAT(concat(year,'-',month,'-',day,' ',hour),'YYYY-MM-dd HH:mm:ss') as time_hour from statics group by year,month,day,hour"

  val daysql = "select sum(1) as count, DATE_FORMAT(concat(year,'-',month,'-',day),'YYYY-MM-dd') as time_day from statics group by year,month,day "

  val testhoursql = "select sum(1) as hourcount, DATE_FORMAT(concat(year,'-',month,'-',day,' ',hour),'YYYY-MM-dd HH:mm:ss') as time_hour,year,month,day,hour from statics group by year,month,day,hour"
  val testdaysql = "select sum(hourcount) as count, DATE_FORMAT(concat(year,'-',month,'-',day),'YYYY-MM-dd') as time_day from hourtable group by year,month,day "


  val inserthourprefix = "insert into hourly_stat (time_hour, attack) values("
  val inserthourlast = ") ON DUPLICATE KEY UPDATE attack = attack +"

  val insertdayprefix = "insert into dayly_stat (time_day, attack) values("
  val insertdaylast = ") ON DUPLICATE KEY UPDATE attack = attack +"

  //统计小时，天粒度的数据
  def stat(): Unit ={
    val con = getCon(dburl,user,password)
    log.info("stat get mysql database connection")
    con.setAutoCommit(false)
    dataFrame.printSchema()
    dataFrame.registerTempTable("statics")
    log.info("input datafram count is "+dataFrame.count())

    if(dataFrame.count()>0) {

      val hourstat = sqlcontext.sql(testhoursql)//分析小时数据

      val hourtable = hourstat.registerTempTable("hourtable")//注册临时表hourtable

      val daystat = sqlcontext.sql(testdaysql)//分析天数据

      val stat = con.createStatement()//创建连接

      val hourinsert = hourstat.collect()

      val dayinsert = daystat.collect()

      for (info <- hourinsert) {
        var count = info.get(0)
        var timedate = info.get(1)
        if (null != timedate && !"".equals(timedate)) {
          var sql = inserthourprefix + " '" + timedate + "' , " + count + " " + inserthourlast + " " + count
          log.info("hour batch sql is " + sql)
          stat.addBatch(sql)
        }
      }

      for(info <- dayinsert){
        var count = info.get(0)
        var timedate = info.get(1)
        if (null != timedate && !"".equals(timedate)){
          val sql = insertdayprefix + " '" + timedate + "' , " + count + " " + insertdaylast + " " + count
          log.info("day batch sql is " + sql)
          stat.addBatch(sql)
        }

      }
      stat.executeBatch()

      con.commit()
      stat.close()
      con.close()
      log.info("hour stat close mysql database connection")
    }
  }

  def getCon(dburl:String,user:String,password:String): Connection ={
    Class.forName("com.mysql.jdbc.Driver")
    val con = DriverManager.getConnection(dburl,user,password)
    return con
  }

}