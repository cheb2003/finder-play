package my.finder.console.service

import java.util.{Date, Calendar}
import java.text.SimpleDateFormat
import java.sql.{Connection, ResultSet, Statement}
import org.apache.commons.lang3
import org.slf4j.LoggerFactory
import org.slf4j
import scala.collection.mutable.ListBuffer
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.Imports._
import org.springframework.jdbc.support.rowset.SqlRowSet
import org.springframework.jdbc.core.JdbcTemplate


object KPIService {
  var logger: slf4j.Logger = LoggerFactory.getLogger("kpi")
  /**
   *  统计指定时间前3天的搜索订单订单号
   *  return ListBuffer
   * */
  private def searchOrder(daySrc: Calendar): ListBuffer[Int] = {
    val day1: Calendar = lang3.ObjectUtils.clone(daySrc)
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val day2: Calendar = lang3.ObjectUtils.clone(day1)
    day2.add(Calendar.DATE, -2)
    val begin = sdf.format(day2.getTime()) + " 00:00:00"
    val end = sdf.format(day1.getTime()) + " 23:59:59"

    val sql = "select k.TraceOrderNO_varchar from sea_keywordsTrace k where k.TraceOrderNO_varchar is not null and k.TraceOrderNO_varchar <> '' and" +
      " k.ProjectName_varchar = 'www.dinodirect.com' and k.InsertTime_timestamp between '" + begin + "' and '" + end + "'"
    val conn: Connection = DBMysql.ds.getConnection()
    val stem: Statement = conn.createStatement()
    val rs: ResultSet = stem.executeQuery(sql)
    var orderNoList: ListBuffer[Int] = new ListBuffer[Int]
    while (rs.next()) {
      val orderNo: Int = rs.getInt("TraceOrderNO_varchar")
      orderNoList += orderNo
    }
    DBMysql.colseConn(conn, stem, rs)
    orderNoList
  }

  /**
   * 统计指定时间当天的付款订单信息
   * 统计结果写入mongo数据库中
   * */
  def paymentOrder(daySrc: Calendar) = {
    val day: Calendar = lang3.ObjectUtils.clone(daySrc)
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val begin2: String = sdf.format(day.getTime()) + " 00:00:00"
    val end2: String = sdf.format(day.getTime()) + " 23:59:59"
    val orderNoList: ListBuffer[Int] = searchOrder(daySrc)
    var sb = new StringBuffer()
    if (!orderNoList.isEmpty) {
      for (x <- orderNoList) {
        sb = sb.append(x).append(",")
      }
    }
    val sql2 = "select t.orderId_int,t.callbackTime_datetime,o.TrackingPC_nvarchar,o.discountSum_money" +
      " from ec_transaction t,ec_order o where t.PaymentStatus_char='Completed' and" +
      " t.callbackTime_datetime between '" + begin2 + "' and '" + end2 + "' and" +
      " o.orderId_int = t.orderId_int and o.orderId_int in (" + sb.substring(0, sb.length() - 1) + ")"
    val conn: Connection = DBMssql.ds.getConnection()
    val stem: Statement = conn.createStatement()
    val rs: ResultSet = stem.executeQuery(sql2)
    var count: Int = 0
    val client = MyMongoManager()
    val col = client.getDB("ddsearch").getCollection("searchSale")
    while (rs.next()) {
      val orderId: Int = rs.getInt("orderId_int")
      val callTime: Date = rs.getTimestamp("callbackTime_datetime")
      val trackPc: String = rs.getString("TrackingPC_nvarchar")
      val sunMoney = rs.getDouble("discountSum_money")
      val obj = MongoDBObject("orderId" -> orderId, "pcId" -> trackPc, "time" -> callTime, "value" -> sunMoney)
      col.save(obj)
      count = count + 1
    }
    DBMssql.colseConn(conn, stem, rs)
    logger.info("{}的搜索订单中付款订单有：{}", begin2, count)
  }

  def deleteOrder(calend: Calendar){
    val from = lang3.ObjectUtils.clone(calend)
    from.set(Calendar.HOUR_OF_DAY,0)
    from.set(Calendar.MINUTE,0)
    from.set(Calendar.SECOND,0)
    val to = lang3.ObjectUtils.clone(calend)
    to.set(Calendar.HOUR_OF_DAY,23)
    to.set(Calendar.MINUTE,59)
    to.set(Calendar.SECOND,59)
    val client = MyMongoManager()
    val col:MongoCollection = client("ddsearch")("searchSale")
    val query = "time" $gte from.getTime $lte to.getTime
    col.remove(query)
  }
  /**
   * 每日搜索人次统计
   * */
  def searchPreson(calend: Calendar): ListBuffer[String] = {
    val day: Calendar = lang3.ObjectUtils.clone(calend)
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val from: String = sdf.format(day.getTime()) + " 00:00:00"
    val to: String = sdf.format(day.getTime()) + " 23:59:59"
    val sql = "select k.PCCookieID_varchar,k.TraceStep_varchar from sea_keywordsTrace k where k.InsertTime_timestamp" +
      " between '" + from + "' and '" + to + "' and ProjectName_varchar = 'www.dinodirect.com'"
    val conn: Connection = DBMysql.ds.getConnection()
    val stem: Statement = conn.createStatement()
    val rs: ResultSet = stem.executeQuery(sql)
    var pcIdList: ListBuffer[String] = new ListBuffer[String]
    while (rs.next()) {
      val TraceStep:String = rs.getString("TraceStep_varchar")
      if( "search".equals(TraceStep) ){
         val pcId: String = rs.getString("PCCookieID_varchar")
        if ( pcId != "" && pcId != null ){
            pcIdList += pcId
        }
      }
    }
    DBMysql.colseConn(conn, stem, rs)
    pcIdList
  }
}
