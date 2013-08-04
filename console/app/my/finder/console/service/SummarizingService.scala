package my.finder.console.service

import java.util.{Date, Calendar}
import java.text.SimpleDateFormat
import java.sql.{Connection, ResultSet, Statement}
import org.apache.commons.lang3
import org.slf4j.LoggerFactory
import org.slf4j
import com.mongodb.casbah.commons.{MongoDBList, MongoDBObject}
import com.mongodb.casbah.Imports._


object SummarizingService {
  var logger: slf4j.Logger = LoggerFactory.getLogger("kpi")
  def deleteTopKey(calend: Calendar){
    val from = lang3.ObjectUtils.clone(calend)
    from.set(Calendar.HOUR_OF_DAY,0)
    from.set(Calendar.MINUTE,0)
    from.set(Calendar.SECOND,0)
    val to = lang3.ObjectUtils.clone(calend)
    to.set(Calendar.HOUR_OF_DAY,23)
    to.set(Calendar.MINUTE,59)
    to.set(Calendar.SECOND,59)
    val client = MyMongoManager()
    val col:MongoCollection = client("ddsearch")("topKeySearchPerDay")
    val query = "time" $gte from.getTime $lte to.getTime
    col.remove(query)
  }

  //查询某一天的搜索关键字统计并写入mongo数据库中
  def paymentTopKey(daySrc: Calendar) = {
    val day: Calendar = lang3.ObjectUtils.clone(daySrc)
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val begin: String = sdf.format(day.getTime()) + " 00:00:00"
    val end: String = sdf.format(day.getTime()) + " 23:59:59"
    val sdf2: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time: Date = sdf2.parse(sdf.format(day.getTime()) + " 03:00:00")

    val conn: Connection = DBMysql.ds.getConnection()
    val conn2: Connection = DBMssql.ds.getConnection()
    val stem: Statement = conn.createStatement()

    val client = MyMongoManager()
    val col = client.getDB("ddsearch").getCollection("topKeySearchPerDay")
    val sql = "select min(k.sea_id),k.Keyword_varchar from sea_keywordsTrace k where k.InsertTime_timestamp between '" +
      begin + "' and '" + end + "' group by k.Keyword_varchar"
    val rs: ResultSet = stem.executeQuery(sql)
    while ( rs.next()){
      val keyword:String = rs.getString("Keyword_varchar")
      val sql1 = "select * from sea_keywordsTrace k where k.Keyword_varchar ='"+ keyword + "' and " +
      "k.InsertTime_timestamp between '" + begin + "' and '" + end + "'"
      val rs1: ResultSet = conn.createStatement().executeQuery(sql1)

      var count:Int = 0
      var resultCount:Int = 0
      var noResultCount:Int = 0
      var resultClickCount:Int = 0
      var totalOrder:Int = 0

      var sb = new StringBuffer()
      while ( rs1.next() ){
        val TraceStep_varchar = rs1.getString("TraceStep_varchar")
        if( "productdetail".equals(TraceStep_varchar)){
          resultClickCount = resultClickCount + 1
        }
        var orderNo: Int =  0
        orderNo = rs1.getInt("TraceOrderNO_varchar")
        if ( orderNo != 0 ) {
          totalOrder = totalOrder + 1
          sb = sb.append(orderNo).append(",")
        }
        //关键字点击返回结果数
        var num:Int  =  0
        num = rs1.getInt("SearchCount_int")
        if( num == 0 ){
          noResultCount = noResultCount + 1
        }else{
          resultCount = resultCount + num
        }
        //关键字点击次数
        count = count + 1
      }
      rs1.close()
      //搜索点击产品Id统计
      val sql2 = "select productkeyid_nvarchar,orderid_int from ec_orderdetail where orderid_int in (" +
        sb.substring(0, sb.length() - 1) + ")"
      val rs2: ResultSet = conn2.createStatement().executeQuery(sql2)
      var clickProducts = new StringBuffer()
      while ( rs2.next() ){
        val productId:String = rs2.getString("productkeyid_nvarchar")
        clickProducts = clickProducts.append(productId).append(",")
      }
      rs2.close()
      if ( clickProducts.length() - 1 > 0 ){
        clickProducts.substring(0, clickProducts.length() - 1)
      }
      //付款订单
      val sql3 = "select o.orderId_int,o.discountSum_money,o.TrackingPC_nvarchar,t.PaymentStatus_char from " +
           "ec_order o left join ec_transaction t on  o.orderId_int = t.orderId_int " +
           "and o.orderId_int in (" + sb.substring(0, sb.length() - 1) + ")"
      logger.info(sql3)
      val rs3: ResultSet = conn2.createStatement().executeQuery(sql3)
      var payOrder:Int = 0
      var unpaynum:Int = 0
      var payMoney:Float = 0.0F
      var totalMoney:Float = 0.0F
      var payOrders = MongoDBList.newBuilder
      var unpayOrder = MongoDBList.newBuilder
      while ( rs3.next() ){
        val  orderIdInt =  rs3.getInt("orderId_int")
        val  discountSum:Float =  rs3.getBigDecimal("discountSum_money") .floatValue()
        val  pcId:String = rs3.getString("TrackingPC_nvarchar")
        val  mongoDB = MongoDBObject("orderId" ->orderIdInt,"discountSum" ->discountSum,"pcId" ->pcId )
        val  PaymentStatus_char:String = rs3.getString("PaymentStatus_char")
        if ( "Completed".equals(PaymentStatus_char) ) {
          payOrders += mongoDB
          payMoney = payMoney + discountSum
          payOrder = payOrder + 1
        }else{
          unpayOrder +=  mongoDB
          unpaynum = unpaynum + 1
        }
        totalMoney = totalMoney + discountSum
      }
      rs3.close()
      val obj = MongoDBObject("keyword" -> keyword, "count" -> count, "time" -> time, "resultCount" -> resultCount,"resultClickCount" -> resultClickCount,
        "payOrder" ->payOrder, "clickProducts" ->clickProducts.toString, "totalOrder" ->totalOrder,"payMoney" ->payMoney, "totalMoney" ->totalMoney,
        "noResultCount" ->noResultCount, "unpayOrder" ->unpayOrder.result(), "payOrders" ->payOrders.result())
      logger.info(obj.toString())
      col.save(obj)
     }
     DBMysql.colseConn(conn, stem, rs)
  }
}
