package controllers

import play.api.mvc.{Action, Controller}
import my.finder.console.service.{SummarizingService, MyMongoManager}

import com.mongodb.casbah.Imports._
import scala.collection.mutable.Queue

import java.util.{Date, Calendar}

import scala.xml._
import org.apache.commons.lang3
import com.mongodb.casbah.commons
import java.text.SimpleDateFormat

/**
 *
 */
object KeyWord extends Controller {
  //导出搜索统计数据
  def searchPerDay(year: String, month:String,day:String) = Action { implicit request =>
    val calend:Calendar = Calendar.getInstance()
    val year_int:Int = year.toInt
    val month_int:Int =  month.toInt - 1
    val day_int:Int =  day.toInt
    calend.set(year_int,month_int,day_int)
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
    val items = col.find( query )
    val list = items.toList
    val nodes = new Queue[Node]()
    for (i <- list) {
      docToXML(nodes,i)
    }
    Ok(<root>{ nodes }</root>)
  }

  private def docToXML(nodes: Queue[Node], i:DBObject) = {
    var n = <item/>
    val uPlist:MongoDBList = i.getAsOrElse[MongoDBList]("unpayOrder",null)
    var unPayOrderId = new StringBuffer()
    if(uPlist != null){
      if ( uPlist.size > 0 ){
        for(y  <-  0 until uPlist.length ){
          val upobj:Int = uPlist.as[DBObject](y).as[Int]("orderId")
          unPayOrderId.append(upobj).append(",")
        }
      }
    }
    if ( unPayOrderId.toString.equals("") ){
      n = n % Attribute(None, "unPayOrderIds", Text(""), Null)
    }else{
      n = n % Attribute(None, "unPayOrderIds", Text( unPayOrderId.substring(0, unPayOrderId.length() - 1)), Null)
    }
    val Plist = i.getAsOrElse[MongoDBList]("payOrders",null)
    var payOrderIds = new StringBuffer()
    if(Plist != null){
      if ( Plist.size > 0 ){
        for(y  <-  0 until Plist.length ){
          val pobj:Int = Plist.as[DBObject](y).as[Int]("orderId")
          unPayOrderId.append(pobj).append(",")
        }
      }
    }
    if ( payOrderIds.toString.equals("") ){
       n = n % Attribute(None, "payOrderIds", Text(""), Null)
    }else{
       n = n % Attribute(None, "payOrderIds", Text(payOrderIds.substring(0, payOrderIds.length() - 1)), Null)
    }
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    n = n % Attribute(None, "time", Text(sdf.format(i.as[Date]("time"))), Null)
    n = n % Attribute(None, "payOrder", Text(i.as[Int]("payOrder").toString), Null)
    n = n % Attribute(None, "totalOrder", Text(i.as[Int]("totalOrder").toString), Null)
    n = n % Attribute(None, "totalMoney", Text(i.as[Double]("totalMoney").toString), Null)
    n = n % Attribute(None, "resultCount", Text(i.as[Int]("resultCount").toString), Null)
    n = n % Attribute(None, "clickProducts", Text(i.as[String]("clickProducts")), Null)
    n = n % Attribute(None, "resultClickCount", Text(i.as[Int]("resultClickCount").toString), Null)
    n = n % Attribute(None, "payMoney", Text(i.as[Int]("payMoney").toString), Null)
    n = n % Attribute(None, "noResultCount", Text(i.as[Int]("noResultCount").toString), Null)
    n = n % Attribute(None, "count", Text(i.as[Int]("count").toString), Null)
    n = n % Attribute(None, "webSiteId", Text("61"), Null)
    n = n % Attribute(None, "keyword", Text(i.as[Int]("keyword").toString), Null)
    nodes += n
  }

  def createPerDay(year: String, month:String,day:String) = Action { implicit request =>
    val calend:Calendar = Calendar.getInstance()
    val year_int:Int = year.toInt
    val month_int:Int =  month.toInt - 1
    val day_int:Int =  day.toInt
    calend.set(year_int,month_int,day_int)
    SummarizingService.paymentTopKey(calend)
    Ok("success")
  }

  def deletePerDay(year: String, month:String,day:String) = Action { implicit request =>
    val calend:Calendar = Calendar.getInstance()
    val year_int:Int = year.toInt
    val month_int:Int =  month.toInt - 1
    val day_int:Int =  day.toInt
    calend.set(year_int,month_int,day_int)
    SummarizingService.deleteTopKey(calend)
    Ok("success")
  }

}
