package controllers
import java.io.File
import java.util._;
import scala.concurrent.ExecutionContext.Implicits.global

import my.finder.common.message.{OldIndexIncremetionalTaskMessage, IndexIncremetionalTaskMessage, CommandParseMessage}
import my.finder.console.actor.MessageFacade.rootActor
import my.finder.common.util._
import my.finder.util._
import my.finder.common.model.Doc

import org.apache.lucene.store.{FSDirectory, Directory}
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search._
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.util.Version
import org.apache.lucene.analysis.core._

import play.api.mvc._
import play.api.libs.json.{Json, JsValue}
import play.api.data._
import play.api.data.Forms._
import play.api.Play._

import scala.collection.mutable.Queue
import scala.xml._

object Application extends Controller {
  val xmlUtil = new XMLUtil
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

  def query = Action { implicit request =>
    val form = Form(
      tuple(
        "q" -> text,
        "i" -> text
      )
    )
    val queryParams = form.bindFromRequest.data
    val qStr = Util.getParamString(queryParams,"q","")
    val i = Util.getParamString(queryParams,"i","")

    val dir:Directory = FSDirectory.open(new File(wordDir.get + i));
    val reader = DirectoryReader.open(dir);

    val searcher  = new IndexSearcher(reader);
    //val parse = new QueryParser(Version.LUCENE_43,"pName",new MyAnalyzer())
    val parse = new QueryParser(Version.LUCENE_43,"pName",new KeywordAnalyzer())
    val q = parse.parse(qStr)
    println("q str " + qStr)
    println(q)
    val topDocs = searcher.search(q,100)
    val scoreDocs = topDocs.scoreDocs
    var nodes = new Queue[Node]()
    nodes += <total>{topDocs.totalHits}</total>
    println("-----------" + topDocs.totalHits)
    for (i <- 0 until scoreDocs.length) {
      val indexDoc = searcher.getIndexReader().document(scoreDocs(i).doc)
      docToXML(nodes,indexDoc)
    }
    reader.close()
    Ok(<root>{nodes}</root>)
  }

  private def docToXML(nodes:Queue[Node], document:org.apache.lucene.document.Document) = {
    val fields:List[IndexableField]  = document.getFields()
    val ite = fields.iterator
    var n = <doc/>
    while(ite.hasNext){
      val field = ite.next
      n = n % Attribute(None,field.name,Text(field.stringValue),Null)
    }
    nodes += n
  }
}