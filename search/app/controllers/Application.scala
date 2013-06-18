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
import org.apache.lucene.queryparser.classic.{QueryParserBase, QueryParser}

object Application extends Controller {

  val dinobuydb = current.configuration.getString("dinobuydb")
  val fields = MongoDBObject("productkeyid_nvarchar" -> 1 , "ec_product.venturestatus_tinyint" -> 1,"ec_product.venturelevelnew_tinyint" -> 1)
  def index = Action {
    Ok(views.html.index("Your new application is ready."))
  }

  def export = Action { request =>
    val page:Int = if(request.getQueryString("page") == Some("") || request.getQueryString("page") == None) 1 else Integer.valueOf(request.getQueryString("page").get)
    val keyword = request.getQueryString("keyword").getOrElse("")
    val term:Term = new Term("pName", keyword)
    val pq:TermQuery = new TermQuery(term)
    /*val skuTerm:Term = new Term("sku", "A")
    val skupq:PrefixQuery = new PrefixQuery(skuTerm)
    val nrq = NumericRangeQuery.newIntRange("qdwproductstatus_int",0,1,true,true);*/

    val bq = new BooleanQuery()
    bq.add(pq,BooleanClause.Occur.MUST)
    /*bq.add(nrq, BooleanClause.Occur.MUST);

    bq.add(skupq,BooleanClause.Occur.MUST)*/
    val searcher:IndexSearcher = SearcherManager.searcher
    val size = 1000
    val start = (page - 1) * size + 1;

    val sortField:SortField  = SortField.FIELD_SCORE
    val sot:Sort = new Sort(sortField);

    //分页
    val tsdc:TopFieldCollector = TopFieldCollector.create(sot, start + size, false, false, false, false);
    println(bq)
    searcher.search(bq, tsdc);

    val ids = ListBuffer[Long]()
    //从0开始计算
    val topDocs:TopDocs = tsdc.topDocs(start - 1, size);
    val scoreDocs = topDocs.scoreDocs;
    //val total = tsdc.getTotalHits()
    for (i <- 0 until scoreDocs.length) {
      val indexDoc = searcher.getIndexReader().document(scoreDocs(i).doc);
      ids += Long.valueOf(indexDoc.get("pId"))
    }
    val sb = new StringBuffer()
    if (ids.length > 0) {
      val mongo = MongoManager()
      val productColl = mongo(dinobuydb.get)("ec_productinformation")
      val items = productColl.find("productid_int" $in ids, fields, 0, size)

      for (x <- items) {
        if(x.as[DBObject]("ec_product").as[Int]("venturelevelnew_tinyint") == 0){
          sb.append(x.as[String]("productkeyid_nvarchar")).append(',').append(x.as[DBObject]("ec_product").as[Int]("venturestatus_tinyint")).append("\r\n")
        }
      }
    }
    Ok(sb.toString())
  }

  def search = Action {
    request =>
      val indexCode = request.getQueryString("indexcode").getOrElse("")
      val productTypeId = request.getQueryString("producttypeid").getOrElse("")
      val productKeyId = request.getQueryString("productkeyid").getOrElse("")
      val productBrandId = request.getQueryString("productbrandid").getOrElse("")
      /*val unitPrice = request.getQueryString("unitprice").getOrElse("")
      val createTime = request.getQueryString("createtime").getOrElse("")
      val isQualityProduct = request.getQueryString("isqualityproduct").getOrElse("")
      val ventureStatus = request.getQueryString("venturestatus").getOrElse("")
      val qdwProductStatus = request.getQueryString("qdwproductstatus").getOrElse("")*/
      val range = request.getQueryString("ranges").getOrElse("")
      val sort = request.getQueryString("sort").getOrElse("")

      var productAliasName = request.getQueryString("productaliasname").getOrElse("")
      val businessBrand = request.getQueryString("businessbrand").getOrElse("")

      val currentPage:Int = if(request.getQueryString("page") == Some("") || request.getQueryString("page") == None) 1 else Integer.valueOf(request.getQueryString("page").get)
      val isTaobaoStr:String = request.getQueryString("isTaobao").getOrElse("")
      val isQualityStr:String = request.getQueryString("isQuality").getOrElse("")
      var size:Int = if(request.getQueryString("size") == Some("")) 100 else Integer.valueOf(request.getQueryString("size").getOrElse("100"))


      if (size < 1 || size > 1000) {
        size = 100;
      }

      productAliasName = productAliasName.trim
      val bq:BooleanQuery  = new BooleanQuery()
      if(productAliasName != ""){
        val keywords = productAliasName.split(" ")
        val bqKeyEn:BooleanQuery  = new BooleanQuery()
        //search title
        for (k <- keywords) {
          val term:Term = new Term("pName", k)
          val pq:PrefixQuery = new PrefixQuery(term)
          bqKeyEn.add(pq, BooleanClause.Occur.MUST)
        }
        bq.add(bqKeyEn, BooleanClause.Occur.MUST)
      }

      //search indexCode
      if(indexCode != ""){
        val indexCodeTerm:Term = new Term("indexCode", indexCode);
        val indexCodePQ:PrefixQuery = new PrefixQuery(indexCodeTerm);
        bq.add(indexCodePQ, BooleanClause.Occur.MUST)
      }
      //search producttypeid
      if (productTypeId != "") {
        val typeIds = productTypeId.split(",")
        val bqTypeIds = new BooleanQuery()
        for (typeId <- typeIds) {
          val term:Term = new Term("pTypeId", typeId);
          val q:TermQuery = new TermQuery(term)
          bqTypeIds.add(q,BooleanClause.Occur.SHOULD)
        }
        bq.add(bqTypeIds,BooleanClause.Occur.MUST)
      }
      //sku
      if (productKeyId != "") {
        val skus = productKeyId.split(",")
        val bqSkus = new BooleanQuery()
        for (sku <- skus) {
          val term:Term = new Term("sku", sku);
          val q:TermQuery = new TermQuery(term)
          bqSkus.add(q,BooleanClause.Occur.SHOULD)
        }
        bq.add(bqSkus,BooleanClause.Occur.MUST)
      }
      //productBrandId
      if (productBrandId != "") {
        val brandIds = productBrandId.split(",")
        val bqBrandId = new BooleanQuery()
        for (brand <- brandIds) {
          val term:Term = new Term("pBrandId", brand);
          val q:TermQuery = new TermQuery(term)
          bqBrandId.add(q,BooleanClause.Occur.SHOULD)
        }
        bq.add(bqBrandId,BooleanClause.Occur.MUST)
      }
      if (isTaobaoStr != "") {
        val term:Term = new Term("isTaobao", isTaobaoStr);
        val q:TermQuery = new TermQuery(term)
        bq.add(q,BooleanClause.Occur.MUST)
      }
      if (isQualityStr != "") {
        val term:Term = new Term("isQuality", isQualityStr);
        val q:TermQuery = new TermQuery(term)
        bq.add(q,BooleanClause.Occur.MUST)
      }



      if(businessBrand != ""){
        val businessBrandTerm:Term = new Term("businessBrand", businessBrand);
        val businessBrandPQ:PrefixQuery = new PrefixQuery(businessBrandTerm);
        bq.add(businessBrandPQ, BooleanClause.Occur.MUST)
      }
      ranges(range.split(","),bq)
      val sot:Sort  = sorts(sort);
      val ids = ListBuffer[Long]()


      val searcher:IndexSearcher = SearcherManager.searcher

      val start = (currentPage - 1) * size + 1;
      //分页
      val tsdc:TopFieldCollector = TopFieldCollector.create(sot, start + size, false, false, false, false);
      println(bq)
      searcher.search(bq, tsdc);


      //从0开始计算
      val topDocs:TopDocs = tsdc.topDocs(start - 1, size);
      val scoreDocs = topDocs.scoreDocs;
      val total = tsdc.getTotalHits()
      for (i <- 0 until scoreDocs.length) {
        val indexDoc = searcher.getIndexReader().document(scoreDocs(i).doc);
        ids += Long.valueOf(indexDoc.get("pId"))
      }

      val sb = new StringBuffer()
      for (x <- ids){
        sb.append(x).append(',')
      }

      //Ok(Json.toJson(jsonObject).toString())
      if (sb.length() > 0) {
        Ok(Json.obj("productIds" -> sb.substring(0,sb.length() - 1), "totalHits" -> total))
      } else {
        Ok(Json.obj("productIds" -> "", "totalHits" -> total))
      }
  }


  def sorts(sort:String):Sort = {
    //排序
    var sortField:SortField  = null
    if ("price-".equals(sort)) {
      sortField = new SortField("unitPrice", SortField.Type.DOUBLE, true);
    } else if ("price+".equals(sort)) {
      sortField = new SortField("unitPrice", SortField.Type.DOUBLE, false);
    } else if ("date-".equals(sort)) {
      sortField = new SortField("createTime", SortField.Type.DOUBLE, true);
    } else if ("date+".equals(sort)) {
      sortField = new SortField("createTime", SortField.Type.DOUBLE, false);
    }
    var sot:Sort = null;
    if (sortField == null) {
      //默认按相关度排序
      sortField = SortField.FIELD_SCORE;
      sot = new Sort(sortField);
    } else {
      sot = new Sort(sortField);
    }
    sot;
  }
  def ranges(ranges:Array[String], bq:BooleanQuery) {
    //范围查询

    for (range <- ranges) {
      val parts = range.split(":");
      if (parts.length == 3) {
        if (parts(0).equals("unitprice")) {
          val nrq = NumericRangeQuery.newDoubleRange("unitPrice",Double.valueOf(parts(1)),Double.valueOf(parts(2)),true,true);
          bq.add(nrq, BooleanClause.Occur.MUST);
        }
        if (parts(0).equals("createtime")) {
          val query:TermRangeQuery = new TermRangeQuery("createTime", new BytesRef(parts(1)), new BytesRef(parts(2)), true, true);
          bq.add(query, BooleanClause.Occur.MUST);
        }
        if (parts(0).equals("isqualityproduct")) {
          val query:TermRangeQuery = new TermRangeQuery("isQuality", new BytesRef(parts(1)), new BytesRef(parts(2)), true, true);
          bq.add(query, BooleanClause.Occur.MUST);
        }
        if (parts(0).equals("venturestatus")) {
          val query:TermRangeQuery = new TermRangeQuery("ventureStatus", new BytesRef(parts(1)), new BytesRef(parts(2)), true, true);
          bq.add(query, BooleanClause.Occur.MUST);
        }
      }
    }
  }

  def test = Action { implicit request =>
    val form = Form(
      tuple(
        "keyword" -> text,
        "country" -> text,
        "indexCode" -> text,
        "range" -> text,
        "currentPage" -> number,
        "size" -> number,
        "sort" -> text
      )
    )
    /*val anyData = Map("id" -> "111", "name" -> "secret")
    val (id, name) = form.bind(anyData).get*/
    val queryParams = form.bindFromRequest.data
    //Ok("Got: " + id + name)
    var keyword = getParam[String](queryParams,"keyword","")
    val country = getParam[String](queryParams,"country","")
    val indexCode = getParam[String](queryParams,"indexCode","")
    val range = getParam[String](queryParams,"range","")
    var currentPage = getParam[Int](queryParams,"currentPage",1)
    var size = getParam[Int](queryParams,"size",100)
    val sort = getParam[String](queryParams,"sort","")
    if(currentPage < 1){
      currentPage = 1
    }
    if(size < 1 || size > 100){
      size = 20
    }

    keyword = keyword.trim()
    keyword = QueryParserBase.escape(keyword);

    //Ok("Got: " + id + name)
    Ok("Got: ")
  }
  def getParam[T](v:Map[String,Any],key:String,default:T):T = {
    if(v.contains("name")) v("name").asInstanceOf[T]  else default
  }
}
