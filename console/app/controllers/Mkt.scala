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
import scala.collection.mutable

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

  /**
   * 0结果数统计图
   * */
  def searchNoResults() = Action { implicit request =>
    val form = Form(
      tuple(
        "bTime" -> text,
        "eTime" -> text)
    )
    val queryParams = form.bindFromRequest.data
    val bTime = Util.getParamString(queryParams, "bTime", "")
    val eTime = Util.getParamString(queryParams, "eTime", "")

    val barr:Array[String] = bTime.split("-")
    val earr:Array[String] = eTime.split("-")
    val calend:Calendar = Calendar.getInstance()
      calend.set(barr(0).toInt,barr(1).toInt - 1,barr(2).toInt)
    val from = lang3.ObjectUtils.clone(calend)
    from.set(Calendar.HOUR_OF_DAY,0)
    from.set(Calendar.MINUTE,0)
    from.set(Calendar.SECOND,0)
      calend.set(earr(0).toInt,earr(1).toInt - 1,earr(2).toInt)
    val to = lang3.ObjectUtils.clone(calend)
    to.set(Calendar.HOUR_OF_DAY,23)
    to.set(Calendar.MINUTE,59)
    to.set(Calendar.SECOND,59)
    val client = MyMongoManager()
    val col:MongoCollection = client("ddsearch")("noResultPerDay")
    val query = "time" $gte from.getTime $lte to.getTime
    val items = col.find(query)
    val list = items.toList
    val nodes = new Queue[Node]()
    for (i <- list) {
      var result = <Result/>
      val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      result = result % Attribute(None, "time", Text(sdf.format(i.as[Date]("time"))), Null)
      result = result % Attribute(None, "value", Text(i.as[Double]("value").toString), Null)
      nodes += result
    }
    //Ok(<root>{nodes}</root>)
    Ok(<root>
      <Result value="0.4" time="2013-08-07" />
      <Result value="0.1" time="2013-08-08" />
      <Result value="0.6" time="2013-08-09" />
      <Result value="0.2" time="2013-08-10" />
      <Result value="0.5" time="2013-08-11" />
      <Result value="0.3" time="2013-08-12" />
    </root>)
  }

  /**
   * 0结果数详细
   * */
  def searchNoResultsPerDay() = Action { implicit request =>
    val form = Form(
        "time" -> text
    )
   /* val queryParams = form.bindFromRequest.data
    val time = Util.getParamString(queryParams, "time", "")
    val times:Array[String] = time.split("-")
    val calend:Calendar = Calendar.getInstance()
    calend.set(times(0).toInt,times(1).toInt - 1,times(2).toInt)
    val resultList:ListBuffer[MongoDBObject] =  KPIService.searchNoResultsPerDay(calend)
    val list = resultList.toList
    val nodes = new Queue[Node]()
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    for (i <- list) {
        var result = <Result/>
      result = result % Attribute(None, "keyword", Text(i.as[String]("keyword")), Null)
      result = result % Attribute(None, "pName", Text(i.as[String]("pName")), Null)
      result = result % Attribute(None, "time", Text(sdf.format(i.as[Date]("time"))), Null)
        nodes += result
    }
    Ok(<root>{nodes}</root>)*/
    Ok("")
  }

}
