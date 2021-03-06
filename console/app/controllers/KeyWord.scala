package controllers

import play.api.mvc.{Action, Controller}
import my.finder.console.service.{SummarizingService, MyMongoManager}

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
object KeyWord extends Controller {
  //导出搜索统计数据
  def searchPerDay() = Action { implicit request =>
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
    calend.set( year.toInt,month.toInt - 1,day.toInt)
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
    val unPayOrderId = new StringBuffer()
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
    val payOrderIds = new StringBuffer()
    if(Plist != null){
      if ( Plist.size > 0 ){
        for(y  <-  0 until Plist.length ){
          val pobj:Int = Plist.as[DBObject](y).as[Int]("orderId")
          payOrderIds.append(pobj).append(",")
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
    n = n % Attribute(None, "payMoney", Text(i.as[Double]("payMoney").toString), Null)
    n = n % Attribute(None, "noResultCount", Text(i.as[Int]("noResultCount").toString), Null)
    n = n % Attribute(None, "count", Text(i.as[Int]("count").toString), Null)
    n = n % Attribute(None, "webSiteId", Text("61"), Null)
    n = n % Attribute(None, "keyword", Text(i.as[String]("keyword")), Null)
    nodes += n
  }

  def createPerDay() = Action { implicit request =>
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
    calend.set( year.toInt,month.toInt - 1,day.toInt)
    SummarizingService.paymentTopKey(calend)
    Ok("success")
  }

  def deletePerDay() = Action { implicit request =>
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
    calend.set( year.toInt,month.toInt - 1,day.toInt)
    SummarizingService.deleteTopKey(calend)
    Ok("success")
  }

  def searchKeyWord() = Action { implicit request =>
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
    calend.set( year.toInt,month.toInt - 1,day.toInt)
    val resultList:ListBuffer[MongoDBObject] =  SummarizingService.searchKeyWord(calend)
    val list = resultList.toList
    val nodes = new Queue[Node]()
    for (i <- list) {
      var result = <item/>
      result = result % Attribute(None, "keyword", Text(i.as[String]("keyword")), Null)
      result = result % Attribute(None, "value", Text(i.as[Int]("value").toString), Null)
      nodes += result
    }
    Ok(<root>{nodes}</root>)
  }

  def expatiationKeyWord() = Action { implicit request =>
    val form = Form(
      tuple(
        "date" -> text,
        "keyword" -> text)
    )
    val queryParams = form.bindFromRequest.data
    val time = Util.getParamString(queryParams, "date", "")
    val times:Array[String] = time.split("-")
    val calend:Calendar = Calendar.getInstance()
    calend.set(times(0).toInt,times(1).toInt - 1,times(2).toInt)
    val keyword = Util.getParamString(queryParams, "keyword", "")
    val resultList:ListBuffer[MongoDBObject] =  SummarizingService.expatiationKeyWord(keyword,calend)
    val list = resultList.toList
    val nodes = new Queue[Node]()
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    for (i <- list) {
      var result = <item/>
      result = result % Attribute(None, "keyword", Text(i.as[String]("keyword")), Null)
      result = result % Attribute(None, "traceStep", Text(i.as[String]("traceStep")), Null)
      result = result % Attribute(None, "resultCount", Text(i.as[Int]("resultCount").toString), Null)
      result = result % Attribute(None, "time", Text(sdf.format(i.as[Date]("time"))), Null)
      nodes += result
    }
    Ok(<root>{nodes}</root>)
  }
}
