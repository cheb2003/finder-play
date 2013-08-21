package controllers

import play.api.data.Form
import play.api.data.Forms._
import my.finder.common.util.Util
import play.api.mvc.{Action, Controller}
import org.apache.lucene.search._
import org.apache.lucene.index.{IndexableField, DirectoryReader, Term}
import org.apache.lucene.store.{FSDirectory, Directory}
import java.io.File
import play.api.Play._
import scala.collection.mutable.Queue
import scala.xml.{Null, Text, Attribute, Node}
import java.util.List
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.util.Version
import org.apache.lucene.analysis.core.KeywordAnalyzer

/**
 * Created with IntelliJ IDEA.
 * User: Administrator
 * Date: 13-8-20
 * Time: 上午10:50
 * To change this template use File | Settings | File Templates.
 */
object SearchLiftStyle extends Controller {

  def searchLiftStyle = Action { implicit request =>
    val form = Form(
      tuple(
        "sort" -> text,
        "size" -> number,
        "page" -> number,
        "i" -> text,
        "q" -> text
      )
    )
    val queryParams = form.bindFromRequest.data
    val sort = Util.getParamString(queryParams, "sort", "").trim
    var size = Util.getParamInt(queryParams, "size", 20)
    var page = Util.getParamInt(queryParams, "page", 1)
    val q = Util.getParamString(queryParams, "q", "").trim
    val i = Util.getParamString(queryParams, "i", "").trim
    //排序
    val sortField = sort match {
      case "sortno-" => new SortField("sortno", SortField.Type.INT, true)
      case "sortno+" => new SortField("sortno", SortField.Type.INT, false)
      case _ => new SortField("sortno", SortField.Type.INT, false)
    }
    val sot: Sort = new Sort(sortField)

    if (page < 0) {
      page = 1
    }
    if (size < 1 || size > 100) {
      size = 20
    }
    val start = (page - 1) * size + 1
    var skuTerm: Term = null
    var skuPq: TermQuery = null
    var qy: Query = null
    var parse: QueryParser = null
    if (q != "") {
      //skuTerm = new Term("sku",sku)
      parse = new QueryParser(Version.LUCENE_43, "q", new KeywordAnalyzer())
      qy = parse.parse(q)
      //skuPq = new TermQuery(skuTerm)
    }
    //分页
    val tsdc: TopFieldCollector = TopFieldCollector.create(sot, start + size, false, false, false, false)
    val wordDir = current.configuration.getString("indexDir")
    val dir: Directory = FSDirectory.open(new File(wordDir.get + i));
    val reader = DirectoryReader.open(dir)
    val searcher  = new IndexSearcher(reader)
    searcher.search(qy, tsdc)
    //从0开始计算
    val topDocs: TopDocs = tsdc.topDocs(start - 1, size)
    val scoreDocs = topDocs.scoreDocs
    val total = tsdc.getTotalHits()

    var nodes = new Queue[Node]()
    nodes += <total>{ total }</total>
    for (i <- 0 until scoreDocs.length) {
      val indexDoc = reader.document(scoreDocs(i).doc)
      docToXML(nodes, indexDoc)
    }
    reader.close()
    Ok(<root>{ nodes }</root>)

  }

  def docToXML(nodes: Queue[Node], document: org.apache.lucene.document.Document) = {

    val fields: List[IndexableField] = document.getFields()
    val ite = fields.iterator
    var n = <doc/>
    while (ite.hasNext) {
      val field = ite.next
      n = n % Attribute(None, field.name, Text(field.stringValue), Null)
    }
    nodes += n

  }

}
