package controllers

import play.api.mvc.{Action, Controller}
import my.finder.console.service.{KPIService, SummarizingService, MyMongoManager}

import com.mongodb.casbah.Imports._
import scala.collection.mutable.Queue

import java.util.{Date, Calendar}

import scala.xml._
import org.apache.commons.lang3
import java.text.SimpleDateFormat

/**
 *
 */
object Mkt extends Controller {
  //导出每日销售订单
  def searchOrder(year: String, month:String,day:String) = Action { implicit request =>
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
    val col:MongoCollection = client("ddsearch")("searchSale")
    val query = "time" $gte from.getTime $lte to.getTime
    val items = col.find(query)
    val list = items.toList
    val nodes = new Queue[Node]()
    for (i <- list) {
      docToXML(nodes,i)
    }
    Ok(<root>{ nodes }</root>)
  }

  private def docToXML(nodes: Queue[Node], i:DBObject) = {
    var n = <order/>
    n = n % Attribute(None, "discountSum", Text(i.as[Double]("value").toString), Null)
    n = n % Attribute(None, "pcId", Text(i.as[String]("pcId")), Null)
    n = n % Attribute(None, "webSiteId", Text("61"), Null)
    n = n % Attribute(None, "orderId", Text(i.as[Int]("orderId").toString), Null)
    nodes += n
  }

  def createOrder(year: String, month:String,day:String) = Action { implicit request =>
    val calend:Calendar = Calendar.getInstance()
    val year_int:Int = year.toInt
    val month_int:Int =  month.toInt - 1
    val day_int:Int =  day.toInt
    calend.set(year_int,month_int,day_int)
    KPIService.paymentOrder(calend)
    Ok("success")
  }

  def deleteOrder(year: String, month:String,day:String) = Action { implicit request =>
    val calend:Calendar = Calendar.getInstance()
    val year_int:Int = year.toInt
    val month_int:Int =  month.toInt - 1
    val day_int:Int =  day.toInt
    calend.set(year_int,month_int,day_int)
    KPIService.deleteOrder(calend)
    Ok("success")
  }
}
