package controllers
import java.io.File
import play.api.mvc._
import play.api.libs.json.{Json, JsValue}
import my.finder.common.message.{OldIndexIncremetionalTaskMessage, IndexIncremetionalTaskMessage, CommandParseMessage}
import my.finder.common.util.{Constants, Util}
import my.finder.console.actor.MessageFacade.rootActor
import org.apache.lucene.store.{FSDirectory, Directory}
import org.apache.lucene.index.DirectoryReader
import play.api.data._
import play.api.data.Forms._
import play.api.Play._
import scala.concurrent.ExecutionContext.Implicits.global



object Application extends Controller {
  val wordDir = current.configuration.getString("workDir")
  val json: JsValue = Json.parse("""
{
  "user": {
    "name" : "toto",
    "age" : 25,
    "email" : "toto@jmail.com",
    "isAlive" : true,
    "friend" : {
  	  "name" : "tata",
  	  "age" : 20,
  	  "email" : "tata@coldmail.com"
    }
  }
}
                                 """)
  def index = Action {
    Ok(views.html.index("Your new application is ready."))
    //Ok("hello world")
  }
  def hello(name:String) = Action {implicit request =>
    rootActor ! CommandParseMessage(Constants.DD_PRODUCT)
    Ok("hello")
  }

  def inc = Action {implicit request =>
    rootActor ! IndexIncremetionalTaskMessage(Constants.DD_PRODUCT,null)
    Ok("inc")
  }

  def incDD = Action {implicit request =>
    rootActor ! OldIndexIncremetionalTaskMessage(Constants.OLD_DD_PRODUCT,null)
    Ok("incDD")
  }

  def indexInfo = Action { implicit request =>
    val form = Form(
      tuple(
        "i" -> text,
        "i1" -> text
      )
    )
    val queryParams = form.bindFromRequest.data
    val path = Util.getParamString(queryParams,"i","")
    val dir:Directory = FSDirectory.open(new File(wordDir.get + path));
    val reader = DirectoryReader.open(dir);
    Ok("" + Integer.valueOf(reader.numDocs()))
  }
}