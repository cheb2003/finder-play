package controllers

import org.apache.lucene.util.BytesRef
import play.api.mvc._
import play.api.libs.json.Json
import play.api.libs.json.Json._
import scala.collection.mutable.ListBuffer
import java.lang.Double
import java.lang.Long
import org.apache.lucene.search._
import my.finder.search.service.SearcherManager

import org.apache.lucene.index.Term
import my.finder.search.service.MongoManager
import play.api.Play._

import com.mongodb.casbah.Imports._
import scala.Some

import play.api.data._
import play.api.data.Forms._

object Dino extends Controller {

  def productJSON = Action { implicit request =>
    val form = Form(
      tuple(
        "id" -> number,
        "name" -> text
      )
    )
    Ok("dkjf")
  }

  def productXML = Action { implicit request =>
    val form = Form(
      tuple(
        "id" -> number,
        "name" -> text
      )
    )
    Ok("dkjf")
  }
}
