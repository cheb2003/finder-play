package controllers

import play.api.mvc.{Action, Controller}
import my.finder.console.service.MyMongoManager

import com.mongodb.casbah.Imports._
import scala.collection.mutable.Queue

import java.util.Calendar

import scala.xml._
import org.apache.commons.lang3
import com.mongodb.casbah.commons

/**
 *
 */
object KeyWord extends Controller {
  //导出搜索统计数据
  def searchPerDay(year: String, month:String,day:String) = Action { implicit request =>
    val calend:Calendar = Calendar.getInstance()
    val year_int:Int = year.toInt
    val month_int:Int =  month.toInt - 1
    val day_int:Int =  day.toInt - 1
    calend.set(year_int,month_int,day_int)
    val from = lang3.ObjectUtils.clone(calend)
    from.set(Calendar.HOUR_OF_DAY,0)
    from.set(Calendar.MINUTE,0)
    from.set(Calendar.SECOND,0)
    val to = lang3.ObjectUtils.clone(calend)
    to.set(Calendar.HOUR_OF_DAY,23)
    to.set(Calendar.MINUTE,59)
    to.set(Calendar.SECOND,59)
    to.set(Calendar.DATE,28)
    val client = MyMongoManager()
    val col:MongoCollection = client("ddsearch")("topKeySearchPerDay")
    val query = "time" $gte from.getTime $lte to.getTime
    val items = col.find(MongoDBObject("keyword" -> "psm"))
    val list = items.toList
    val nodes = new Queue[Node]()
    for (i <- list) {
      println("======================333333333333===============")
      docToXML(nodes,i)
    }
    Ok(<root>{ nodes }</root>)
  }

  private def docToXML(nodes: Queue[Node], i:DBObject) = {
    var n = <item/>
    val Plist = i.as[MongoDBList]("unPayOrderId")
    var unPayOrderId = new StringBuffer()
    if ( Plist.size > 0 ){
      for( upobj:MongoDBObject <- Plist ){
         val orderId = upobj.get("orderId")
         unPayOrderId.append(orderId).append(",")
      }
      unPayOrderId.substring(0, unPayOrderId.length() - 1)
    }
    val uPlist = i.as[MongoDBList]("payOrderIds")
    var payOrderIds = new StringBuffer()
    if ( uPlist.size > 0 ){
       for(pobj:MongoDBObject <- Plist ){
          val orderId = pobj.get("orderId")
          payOrderIds.append(orderId).append(",")
       }
      payOrderIds.substring(0, payOrderIds.length() - 1)
    }
    n = n % Attribute(None, "unPayOrderIds", Text(unPayOrderId.toString), Null)
    n = n % Attribute(None, "payOrderIds", Text(payOrderIds.toString), Null)
    n = n % Attribute(None, "time", Text(i.as[Calendar]("time").toString), Null)
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
    n = n % Attribute(None, "keyword", Text(i.as[Int]("orderId").toString), Null)
    nodes += n
  }
}
