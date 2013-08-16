package controllers
import java.io.File
import java.util._;
import scala.concurrent.ExecutionContext.Implicits.global

import my.finder.common.message.{ OldIndexIncremetionalTaskMessage, IndexIncremetionalTaskMessage, CommandParseMessage,PartitionIndexAttributesTaskMessage}
import my.finder.console.actor.MessageFacade.rootActor
import my.finder.common.util._

import my.finder.common.model.Doc

import org.apache.lucene.store.{ FSDirectory, Directory }
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search._
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.util.Version
import org.apache.lucene.analysis.core._
import org.apache.lucene.index.Term
import org.apache.lucene.search.spell.SpellChecker
import org.apache.lucene.search.spell.PlainTextDictionary
import org.apache.lucene.index.IndexWriterConfig


import play.api.mvc._
import play.api.libs.json.{ Json, JsValue }
import play.api.data._
import play.api.data.Forms._
import play.api.Play._

import scala.collection.mutable.Queue
import scala.xml._
import java.util.regex.Pattern
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.en.EnglishAnalyzer

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

object Application extends Controller {
  val logger = LoggerFactory.getLogger("my")
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
  def hello(name: String) = Action { implicit request =>
    if(name == Constants.DD_PRODUCT) {
      rootActor ! CommandParseMessage(Constants.DD_PRODUCT)
      Ok("hello dbsearch")
    }
    if(name == Constants.DD_PRODUCT_FORDB){
      rootActor ! CommandParseMessage(Constants.DD_PRODUCT_FORDB)
      Ok("hello ddsearch")
    }
    Ok("hello")
  }
  def indexDDAttributes = Action { implicit request =>
    val form = Form(
        "i" -> text
    )
    val queryParams = form.bindFromRequest.data

    val i = Util.getParamString(queryParams, "i", "")
    val key = Util.getKey(Constants.DD_PRODUCT_ATTRIBUTE,new Date())
    rootActor ! PartitionIndexAttributesTaskMessage(key,i)
    Ok("hello attr")
  }

  def inc = Action { implicit request =>
    rootActor ! IndexIncremetionalTaskMessage(Constants.DD_PRODUCT, null)
    Ok("inc")
  }

  def incDD = Action { implicit request =>
    rootActor ! OldIndexIncremetionalTaskMessage(Constants.OLD_DD_PRODUCT, null)
    Ok("incDD")
  }

  def indexInfo = Action { implicit request =>
    val form = Form(
      tuple(
        "i" -> text,
        "i1" -> text))
    val queryParams = form.bindFromRequest.data
    val path = Util.getParamString(queryParams, "i", "")
    val dir: Directory = FSDirectory.open(new File(wordDir.get + path));
    val reader = DirectoryReader.open(dir);
    Ok("" + Integer.valueOf(reader.numDocs()))
  }

  def queryKeyword = Action { implicit request =>
    val form = Form(
      tuple(
        "pName" -> text,
        "pNameRU" -> text,
        "pNameBR" -> text,
        "pNameCN" -> text,
        "sku" -> text,
        "i" -> text,
        "pTypeNameEN" -> text,
        "pTypeNameRU" -> text,
        "pTypeNameBR" -> text,
        "pExtendItemBR" -> text,
        "pExtendItemRU" -> text,
        "pExtendItemEN" -> text,
        "match" -> text,
        "errorWord" -> text)
    )
    val queryParams = form.bindFromRequest.data

    val pNames = Util.getParamString(queryParams, "pName", "").toLowerCase.split(" ")
    val pNameRUs = Util.getParamString(queryParams, "pNameRU", "").toLowerCase.split(" ")
    val pNameCNs = Util.getParamString(queryParams, "pNameCN", "").toLowerCase.split(" ")
    val pNameBRs = Util.getParamString(queryParams, "pNameBR", "").toLowerCase.split(" ")
    val skus = Util.getParamString(queryParams, "sku", "").toLowerCase.split(" ")
    val segmentWordRus = Util.getParamString(queryParams, "segmentWordRu", "").toLowerCase.split(" ")
    val segmentWordBrs = Util.getParamString(queryParams, "segmentWordBr", "").toLowerCase.split(" ")
    val segmentWordEns = Util.getParamString(queryParams, "segmentWordEn", "").toLowerCase.split(" ")
    val sourceKeywords = Util.getParamString(queryParams, "sourceKeyword", "").toLowerCase.split(" ")
    val sourceKeywordCNs = Util.getParamString(queryParams, "sourceKeywordCN", "").toLowerCase.split(" ")
    val businessBrands = Util.getParamString(queryParams, "businessBrand", "").toLowerCase.split(" ")
    val i = Util.getParamString(queryParams, "i", "")
    val pTypeNamesEN = Util.getParamString(queryParams, "pTypeNameEN", "").toLowerCase.split(" ")
    val pTypeNamesBR = Util.getParamString(queryParams, "pTypeNameBR", "").toLowerCase.split(" ")
    val pTypeNamesRU = Util.getParamString(queryParams, "pTypeNameRU", "").toLowerCase.split(" ")
    val pExtendItemBRs = Util.getParamString(queryParams, "pExtendItemBR", "").toLowerCase.split(" ")
    val pExtendItemRUs = Util.getParamString(queryParams, "pExtendItemRU", "").toLowerCase.split(" ")
    val pExtendItemENs = Util.getParamString(queryParams, "pExtendItemEN", "").toLowerCase.split(" ")
    val matchs = Util.getParamString(queryParams, "match", "")
    val errorWords = Util.getParamString(queryParams, "errorWord", "")

    val bq: BooleanQuery = new BooleanQuery()
    val bqKeyEn: BooleanQuery = new BooleanQuery()
    val bqKeyRu: BooleanQuery = new BooleanQuery()
    val bqKeyBr: BooleanQuery = new BooleanQuery()
    val bqKeyCn: BooleanQuery = new BooleanQuery()
    if(matchs.length > 0){
      val regEx = "[`\\-~!@#$%^&*()+_=|{}':;',\\[\\].<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。，、？]"
      val p: Pattern = Pattern.compile(regEx)
      val m = p.matcher(matchs.toLowerCase())
      val kwmatchs = m.replaceAll("").trim().replaceAll("~!@#$%^&*()_+","").replaceAll("\\s+", " ").split(" ")
      logger.info("{}",kwmatchs)
      if (kwmatchs.length > 0) {
        val bqmatchs: BooleanQuery = new BooleanQuery()
        for (k <- kwmatchs) {
          if (k.trim != "") {
            val term: Term = new Term("pName", k)
            val pq: PrefixQuery = new PrefixQuery(term)
            bqmatchs.add(pq, BooleanClause.Occur.MUST)
          }
        }
        bq.add(bqmatchs, BooleanClause.Occur.SHOULD)
      }  
    }
    

    val analyzer: Analyzer  = new EnglishAnalyzer(Version.LUCENE_43)
    val qp: QueryParser = new QueryParser(Version.LUCENE_43, "pName" , analyzer)
    if(errorWords.length > 0){
      val qerrorWord: Query = qp.parse(errorWords.replace(" ", " AND "))
      bq.add(qerrorWord, BooleanClause.Occur.SHOULD)  
    }
    

    if (pNames.length > 0) {
      for (k <- pNames) {
        if (k.trim != "") {
          val term: Term = new Term("pName", k)
          val pq: PrefixQuery = new PrefixQuery(term)
          bqKeyEn.add(pq, BooleanClause.Occur.MUST)
        }
      }
      bq.add(bqKeyEn, BooleanClause.Occur.SHOULD)
    }

    if (pNameCNs.length > 0) {
      for (k <- pNameCNs) {
        if (k.trim != "") {
          val term: Term = new Term("pNameCN", k)
          val pq: PrefixQuery = new PrefixQuery(term)
          bqKeyCn.add(pq, BooleanClause.Occur.MUST)
        }
      }
      bq.add(bqKeyCn, BooleanClause.Occur.SHOULD)
    }

    if (pNameRUs.length > 0) {
      for (k <- pNameRUs) {
        if (k.trim != "") {
          val term: Term = new Term("pNameRU", k)
          val pq: PrefixQuery = new PrefixQuery(term)
          bqKeyRu.add(pq, BooleanClause.Occur.MUST)
        }

      }
      bq.add(bqKeyRu, BooleanClause.Occur.SHOULD)
    }

    if (pNameBRs.length > 0) {
      for (k <- pNameBRs) {
        if (k.trim != "") {
          val term: Term = new Term("pNameBR", k)
          val pq: PrefixQuery = new PrefixQuery(term)
          bqKeyBr.add(pq, BooleanClause.Occur.MUST)
        }
      }
      bq.add(bqKeyBr, BooleanClause.Occur.SHOULD)
    }

    if (segmentWordEns.length > 0) {
      val bqSegmentWordEns: BooleanQuery = new BooleanQuery()
      for (k <- segmentWordEns) {
        if (k.trim != "") {
          val term: Term = new Term("segmentWordEn", k)
          val pq: TermQuery = new TermQuery(term)
          bqSegmentWordEns.add(pq, BooleanClause.Occur.MUST)
        }
      }
      bq.add(bqSegmentWordEns, BooleanClause.Occur.SHOULD)
    }

    if (segmentWordRus.length > 0) {
      val bqSegmentWordRus: BooleanQuery = new BooleanQuery()
      for (k <- segmentWordRus) {
        if (k.trim != "") {
          val term: Term = new Term("segmentWordRu", k)
          val pq: TermQuery = new TermQuery(term)
          bqSegmentWordRus.add(pq, BooleanClause.Occur.MUST)
        }
      }
      bq.add(bqSegmentWordRus, BooleanClause.Occur.SHOULD)
    }

    if (segmentWordBrs.length > 0) {
      val bqSegmentWordBrs: BooleanQuery = new BooleanQuery()
      for (k <- segmentWordBrs) {
        if (k.trim != "") {
          val term: Term = new Term("segmentWordBr", k)
          val pq: TermQuery = new TermQuery(term)
          bqSegmentWordBrs.add(pq, BooleanClause.Occur.MUST)
        }
      }
      bq.add(bqSegmentWordBrs, BooleanClause.Occur.SHOULD)
    }

    if (sourceKeywords.length > 0) {
      val bqSourceKeywords: BooleanQuery = new BooleanQuery()
      for (k <- sourceKeywords) {
        if (k.trim != "") {
          val term: Term = new Term("sourceKeyword", k)
          val pq: TermQuery = new TermQuery(term)
          bqSourceKeywords.add(pq, BooleanClause.Occur.MUST)
        }
      }
      bq.add(bqSourceKeywords, BooleanClause.Occur.SHOULD)
    }

    if (sourceKeywordCNs.length > 0) {
      val bqSourceKeywordCns: BooleanQuery = new BooleanQuery()
      for (k <- sourceKeywordCNs) {
        if (k.trim != "") {
          val term: Term = new Term("sourceKeywordCN", k)
          val pq: TermQuery = new TermQuery(term)
          bqSourceKeywordCns.add(pq, BooleanClause.Occur.MUST)
        }
      }
      bq.add(bqSourceKeywordCns, BooleanClause.Occur.SHOULD)
    }

    if (businessBrands.length > 0) {
      val bqBusinessBrands: BooleanQuery = new BooleanQuery()
      for (k <- businessBrands) {
        if (k.trim != "") {
          val term: Term = new Term("businessBrand", k)
          val pq: TermQuery = new TermQuery(term)
          bqBusinessBrands.add(pq, BooleanClause.Occur.MUST)
        }
      }
      bq.add(bqBusinessBrands, BooleanClause.Occur.SHOULD)
    }

    if (pTypeNamesEN.length > 0) {
      val bqpTypeNames: BooleanQuery = new BooleanQuery()
      for (k <- pTypeNamesEN) {
        if (k.trim != "") {
          val term: Term = new Term("pTypeNameEN", k)
          val pq: TermQuery = new TermQuery(term)
          bqpTypeNames.add(pq, BooleanClause.Occur.MUST)
        }
      }
      bq.add(bqpTypeNames, BooleanClause.Occur.SHOULD)
    }

    if (pTypeNamesRU.length > 0) {
      val bqpTypeNamesRU: BooleanQuery = new BooleanQuery()
      for (k <- pTypeNamesRU) {
        if (k.trim != "") {
          val term: Term = new Term("pTypeNameRU", k)
          val pq: TermQuery = new TermQuery(term)
          bqpTypeNamesRU.add(pq, BooleanClause.Occur.MUST)
        }
      }
      bq.add(bqpTypeNamesRU, BooleanClause.Occur.SHOULD)
    }

    if (pTypeNamesBR.length > 0) {
      val bqpTypeNamesBR: BooleanQuery = new BooleanQuery()
      for (k <- pTypeNamesBR) {
        if (k.trim != "") {
          val term: Term = new Term("pTypeNameBR", k)
          val pq: TermQuery = new TermQuery(term)
          bqpTypeNamesBR.add(pq, BooleanClause.Occur.MUST)
        }
      }
      bq.add(bqpTypeNamesBR, BooleanClause.Occur.SHOULD)
    }

    if (pExtendItemBRs.length > 0) {
      val bqpExtendItemBRs: BooleanQuery = new BooleanQuery()
      for (k <- pExtendItemBRs) {
        if (k.trim != "") {
          val term: Term = new Term("pExtendItemBR", k)
          val pq: TermQuery = new TermQuery(term)
          bqpExtendItemBRs.add(pq, BooleanClause.Occur.MUST)
        }
      }
      bq.add(bqpExtendItemBRs, BooleanClause.Occur.SHOULD)
    }

    if (pExtendItemRUs.length > 0) {
      val bqpExtendItemRUs: BooleanQuery = new BooleanQuery()
      for (k <- pExtendItemRUs) {
        if (k.trim != "") {
          val term: Term = new Term("pExtendItemRU", k)
          val pq: TermQuery = new TermQuery(term)
          bqpExtendItemRUs.add(pq, BooleanClause.Occur.MUST)
        }
      }
      bq.add(bqpExtendItemRUs, BooleanClause.Occur.SHOULD)
    }

    if (pExtendItemENs.length > 0) {
      val bqpExtendItemENs: BooleanQuery = new BooleanQuery()
      for (k <- pExtendItemENs) {
        if (k.trim != "") {
          val term: Term = new Term("pExtendItemEN", k)
          val pq: TermQuery = new TermQuery(term)
          bqpExtendItemENs.add(pq, BooleanClause.Occur.MUST)
        }
      }
      bq.add(bqpExtendItemENs, BooleanClause.Occur.SHOULD)
    }
    val dir: Directory = FSDirectory.open(new File(wordDir.get + i));
    val reader = DirectoryReader.open(dir);
    val searcher = new IndexSearcher(reader);
    println(bq)
    logger.info("{}",bq)
    val topDocs = searcher.search(bq, 1)
    val scoreDocs = topDocs.scoreDocs
    var nodes = new Queue[Node]()
    nodes += <total>{ topDocs.totalHits }</total>
    /*for (i <- 0 until scoreDocs.length) {
      val indexDoc = searcher.getIndexReader().document(scoreDocs(i).doc)
      docToXML(nodes,indexDoc)
    }*/
    reader.close()
    Ok(<root>{ nodes }</root>)
  }

  def query = Action { implicit request =>
    val form = Form(
      tuple(
        "q" -> text,
        "i" -> text))
    val queryParams = form.bindFromRequest.data

    val qStr = Util.getParamString(queryParams, "q", "")
    val i = Util.getParamString(queryParams, "i", "")

    val dir: Directory = FSDirectory.open(new File(wordDir.get + i));
    val reader = DirectoryReader.open(dir);

    val searcher = new IndexSearcher(reader);
    //val parse = new QuedryParser(Version.LUCENE_43,"pName",new MyAnalyzer())
    val parse = new QueryParser(Version.LUCENE_43, "pName", new KeywordAnalyzer())
    val q = parse.parse(qStr)
    println(q)
    val topDocs = searcher.search(q, 100)
    val scoreDocs = topDocs.scoreDocs
    var nodes = new Queue[Node]()
    nodes += <total>{ topDocs.totalHits }</total>
    for (i <- 0 until scoreDocs.length) {
      val indexDoc = searcher.getIndexReader().document(scoreDocs(i).doc)
      docToXML(nodes, indexDoc)
    }
    reader.close()
    Ok(<root>{ nodes }</root>)
  }

  def generateSpellCheck = Action { implicit request =>
    val spellCheckDirStr = new File(current.configuration.getString("spellCheckDir").get)
    val dir = FSDirectory.open(spellCheckDirStr)
    val sc = new SpellChecker(dir)
    val analyzer = new KeywordAnalyzer()
    val config = new IndexWriterConfig(Version.LUCENE_43,analyzer)
    sc.indexDictionary(new PlainTextDictionary(new File(spellCheckDirStr + "/spellcheck.txt")),config,false)
    dir.close()
    Ok("success")
  }

  //监控用
  def heartbeat = Action { implicit request =>
    Ok("success")
  }

  private def docToXML(nodes: Queue[Node], document: org.apache.lucene.document.Document) = {
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