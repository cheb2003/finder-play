package my.finder.console.service

import java.util.{Date, Calendar}
import java.text.SimpleDateFormat
import org.apache.commons.lang3
import org.slf4j.LoggerFactory
import org.slf4j
import com.mongodb.casbah.commons.{MongoDBList, MongoDBObject}
import com.mongodb.casbah.Imports._
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.support.rowset.SqlRowSet
import scala.util.control.Breaks._
import scala.collection.mutable.ListBuffer



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
    val jsMysql:JdbcTemplate = new JdbcTemplate(DBMysql.ds)
    val jsMssql:JdbcTemplate = new JdbcTemplate(DBMssql.ds)
    val client = MyMongoManager()
    val col = client.getDB("ddsearch").getCollection("topKeySearchPerDay")
    val sql = "select distinct k.Keyword_varchar from sea_keywordsTrace k where k.InsertTime_timestamp between '" +
      begin + "' and '" + end + "' and k.ProjectName_varchar like '%dinodirect%'"
    val rs: SqlRowSet = jsMysql.queryForRowSet(sql)
    while ( rs.next()){
      val keyword:String = rs.getString("Keyword_varchar")
      val sql1 = "select TraceStep_varchar,TraceOrderNO_varchar,SearchCount_int from sea_keywordsTrace where k.Keyword_varchar ='"+ keyword +
        "' and InsertTime_timestamp between '" + begin + "' and '" + end + "' and ProjectName_varchar like '%dinodirect%'"
      val rs1: SqlRowSet = jsMysql.queryForRowSet(sql1)
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
        var orderNo: String =  ""
        if( "search".equals(TraceStep_varchar)){
          //关键字点击次数
          count = count + 1
          orderNo = rs1.getString("TraceOrderNO_varchar")
          if ( orderNo != "" ) {
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
        }
      }
      //搜索点击产品Id统计
      var clickProducts = new StringBuffer()
      var clickProductIds:String  = ""
      if(sb.length -1 > 0){
        val sql2 = "select productkeyid_nvarchar,orderid_int from ec_orderdetail where orderid_int in (" +
          sb.substring(0, sb.length() - 1) + ")"
        val rs2: SqlRowSet = jsMssql.queryForRowSet(sql2)

        while ( rs2.next() ){
          val productId:String = rs2.getString("productkeyid_nvarchar")
          if ( productId != ""){
            clickProducts = clickProducts.append(productId).append(",")
          }
        }
        if ( clickProducts.length() - 1 > 0 ){
          clickProductIds = clickProducts.substring(0, clickProducts.length() - 1)
        }
      }

      //付款订单
      var payOrder:Int = 0
      var payMoney:Float = 0F
      var Orders = MongoDBList.newBuilder
      var payOrders = MongoDBList.newBuilder
      if(sb.length -1 > 0){
        val sql3 = "select o.orderId_int,o.discountSum_money,o.TrackingPC_nvarchar,t.PaymentStatus_char from " +
          "ec_order o left join ec_transaction t on  o.orderId_int = t.orderId_int " +
          "where o.orderId_int in (" + sb.substring(0, sb.length() - 1) + ")"
        val rs3: SqlRowSet = jsMssql.queryForRowSet(sql3)
        while ( rs3.next() ){
          val  orderIdInt:Int =  rs3.getInt("orderId_int")
          val  discountSum:Float =  rs3.getBigDecimal("discountSum_money").floatValue()
          val  pcId:String = rs3.getString("TrackingPC_nvarchar")
          val  mongoDB = MongoDBObject("orderId" ->orderIdInt,"discountSum" ->discountSum,"pcId" ->pcId )
          val  PaymentStatus_char:String = rs3.getString("PaymentStatus_char")
          if ( "Completed".equals(PaymentStatus_char) ) {
            payOrders += mongoDB
            payMoney = payMoney + discountSum
            payOrder = payOrder + 1
          }else{
            Orders += mongoDB
          }
        }
      }
      var totalMoney = payMoney
      var unpayOrder = MongoDBList.newBuilder
      if ( Orders.result() != null ){
        for(x  <-  0 until Orders.result().length ){
          val orderId:Int = Orders.result().as[DBObject](x).as[Int]("orderId")
          breakable{
             for(y  <-  0 until payOrders.result().length ){
                 val porderId:Int = payOrders.result().as[DBObject](y).as[Int]("orderId")
                 if( orderId != porderId ){
                   if( y == payOrders.result().length -1 ){
                     unpayOrder += Orders.result().as[DBObject](x)
                     totalMoney += Orders.result().as[DBObject](x).as[Float]("discountSum")
                   }
                 }else{
                     break
                 }
             }
          }
        }
      }
      var sKeyword:String = ""
      if ( KeywordUtil.normalizeKeyword(keyword) == null ){
          if( totalOrder != 0 ){
            sKeyword = keyword
          }
      } else{
        sKeyword = KeywordUtil.normalizeKeyword(keyword)
      }
      val obj = MongoDBObject("keyword" -> sKeyword, "count" -> count, "time" -> time, "resultCount" -> resultCount,"resultClickCount" -> resultClickCount,
        "payOrder" ->payOrder, "clickProducts" ->clickProductIds, "totalOrder" ->totalOrder,"payMoney" ->payMoney, "totalMoney" ->totalMoney,
        "noResultCount" ->noResultCount, "unpayOrder" ->unpayOrder.result(), "payOrders" ->payOrders.result())
      col.save(obj)
    }
  }

  def searchKeyWord(daySrc: Calendar):ListBuffer[MongoDBObject] = {
    val day: Calendar = lang3.ObjectUtils.clone(daySrc)
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val begin: String = sdf.format(day.getTime()) + " 00:00:00"
    val end: String = sdf.format(day.getTime()) + " 23:59:59"
    val jsMysql:JdbcTemplate = new JdbcTemplate(DBMysql.ds)
    val sql = "select k.Keyword_varchar from sea_keywordsTrace k where k.InsertTime_timestamp between '" +
      begin + "' and '" + end + "' and k.ProjectName_varchar like '%dinodirect%'"
    val rs: SqlRowSet = jsMysql.queryForRowSet(sql)
    var count:Int = 0
    while ( rs.next ){
        count += 1
    }
    val sql2 = "select distinct k.Keyword_varchar from sea_keywordsTrace k where k.InsertTime_timestamp between '" +
      begin + "' and '" + end + "' and k.ProjectName_varchar like '%dinodirect%'"
    val rs2: SqlRowSet = jsMysql.queryForRowSet(sql2)
    var list:ListBuffer[MongoDBObject] = new ListBuffer[MongoDBObject]
    while ( rs2.next()){
      val keyword:String = rs2.getString("Keyword_varchar")
      val sql3 = "select k.Keyword_varchar from sea_keywordsTrace k where k.Keyword_varchar ='"+ keyword + "' and " +
        "k.InsertTime_timestamp between '" + begin + "' and '" + end + "' and k.projectname_varchar like '%dinodirect%'"
      val rs3: SqlRowSet = jsMysql.queryForRowSet(sql3)
      var kCount:Int = 0
      while ( rs3.next() ){
        kCount = kCount + 1
      }
      var sKeyword:String = ""
      if ( KeywordUtil.normalizeKeyword(keyword) == null ){
          sKeyword = keyword
      } else{
        sKeyword = KeywordUtil.normalizeKeyword(keyword)
      }
      var value:Float = 0.0F
      if ( count != 0 ){
        val fval:Float = kCount.toFloat/ count.toFloat
        val str:String = String.valueOf(fval)
        value = str.substring( 0, (str.indexOf(".") + 2)).toFloat
      }
      list += MongoDBObject("keyword" -> sKeyword, "value" -> value)
    }
    list
  }


  def expatiationKeyWord(keyword: String,daySrc: Calendar):ListBuffer[MongoDBObject] = {
    val day: Calendar = lang3.ObjectUtils.clone(daySrc)
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val begin: String = sdf.format(day.getTime()) + " 00:00:00"
    val end: String = sdf.format(day.getTime()) + " 23:59:59"
    val sql = "select k.Keyword_varchar,k.TraceStep_varchar,k.SearchCount_int,k.InsertTime_timestamp from sea_keywordsTrace k " +
      "where k.Keyword_varchar ='"+ keyword + "' and k.InsertTime_timestamp between '" + begin + "' and '" + end + "' and "
      "k.ProjectName_varchar like '%dinodirect%'"
    val jsMysql:JdbcTemplate = new JdbcTemplate(DBMysql.ds)
    val rs: SqlRowSet = jsMysql.queryForRowSet(sql)
    var list: ListBuffer[MongoDBObject] = new ListBuffer[MongoDBObject]
    while ( rs.next()){
      val keyword:String = rs.getString("Keyword_varchar")
      var traceStep:String = rs.getString("TraceStep_varchar")
      if( "productdetail".equals(traceStep)){
        traceStep = "浏览"
      }
      if( "search".equals(traceStep)){
        traceStep = "搜索"
      }
      val resultCount:Int  = rs.getInt("SearchCount_int")
      val time:Date = rs.getDate("InsertTime_timestamp")
      var sKeyword:String = ""
        if ( KeywordUtil.normalizeKeyword(keyword) == null ){
          sKeyword = keyword
         } else{
          sKeyword = KeywordUtil.normalizeKeyword(keyword)
        }
      val obj = MongoDBObject("keyword" -> sKeyword, "traceStep" -> traceStep, "resultCount" -> resultCount,"time" -> time)
      list += obj
    }
    list
  }
}
