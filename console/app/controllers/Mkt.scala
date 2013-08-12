package controllers

import play.api.mvc.{Action, Controller}
import my.finder.console.service.{KPIService, SummarizingService, MyMongoManager}

import com.mongodb.casbah.Imports._
import scala.collection.mutable.{ListBuffer, Queue}

import java.util.{Date, Calendar}

import scala.xml._
import org.apache.commons.lang3
import java.text.SimpleDateFormat
import play.api.data.Form
import play.api.data.Forms._
import my.finder.common.util.Util

/**
 *
 */
object Mkt extends Controller {
  /**
   * 导出每日销售订单
   * */
   def searchOrder() = Action { implicit request =>
        val form = Form(
           tuple(
               "year" -> text,
               "month" -> text,
               "day" -> text )
           )
       val queryParams = form.bindFromRequest.data
       val year = Util.getParamString(queryParams, "year", "")
       val month = Util.getParamString(queryParams, "month", "")
       val day = Util.getParamString(queryParams, "day", "")
       val calend:Calendar = Calendar.getInstance()
       calend.set(year.toInt,month.toInt - 1,day.toInt)
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

    def createOrder() = Action { implicit request =>
      val form = Form(
        tuple(
         "year" -> text,
         "month" -> text,
         "day" -> text )
       )
       val queryParams = form.bindFromRequest.data
       val year = Util.getParamString(queryParams, "year", "")
       val month = Util.getParamString(queryParams, "month", "")
       val day = Util.getParamString(queryParams, "day", "")
       val calend:Calendar = Calendar.getInstance()
        calend.set(year.toInt,month.toInt - 1,day.toInt)
       KPIService.paymentOrder(calend)
       Ok("success")
    }

    def deleteOrder() = Action { implicit request =>
        val form = Form(
            tuple(
               "year" -> text,
               "month" -> text,
               "day" -> text )
         )
        val queryParams = form.bindFromRequest.data
        val year = Util.getParamString(queryParams, "year", "")
        val month = Util.getParamString(queryParams, "month", "")
        val day = Util.getParamString(queryParams, "day", "")
        val calend:Calendar = Calendar.getInstance()
         calend.set(year.toInt,month.toInt - 1,day.toInt)
        KPIService.deleteOrder(calend)
        Ok("success")
    }
    /**
     * 每日搜索人次导出
     * */
    def searchPerson() = Action { implicit request =>
       val form = Form(
          tuple(
             "year" -> text,
             "month" -> text,
             "day" -> text )
       )
       val queryParams = form.bindFromRequest.data
       val year = Util.getParamString(queryParams, "year", "")
       val month = Util.getParamString(queryParams, "month", "")
       val day = Util.getParamString(queryParams, "day", "")
       val calend:Calendar = Calendar.getInstance()
         calend.set(year.toInt,month.toInt - 1,day.toInt)
       val pcIdList:ListBuffer[String] = KPIService.searchPreson(calend)
       val nodes = new Queue[Node]()
       if ( !pcIdList.isEmpty ) {
          for (x <- pcIdList) {
               var pcId = <pcId/>
               pcId = pcId % Attribute(None, "value", Text(x), Null)
               nodes += pcId
           }
        }
      Ok(<root><pcIds count={nodes.length.toString}>{nodes}</pcIds></root>)
    }
}
