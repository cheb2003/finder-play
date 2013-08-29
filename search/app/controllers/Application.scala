package controllers

import play.api.Play._
import play.api.mvc._
import play.api.libs.json.Json
import play.api.libs.json.JsValue
import play.api.libs.json.Json._
import play.api.data._
import play.api.data.Forms._

import scala.collection.mutable.ListBuffer
import scala.Some

import java.lang.Double
import java.lang.Long

import org.apache.lucene.search._
import org.apache.lucene.search.grouping._
import org.apache.lucene.util.BytesRef
import org.apache.lucene.index.Term

import my.finder.search.service.{ Helper, SearcherManager, MongoManager }
import my.finder.common.util.{ Util }
import my.finder.common.util.MyAnalyzer

//import com.mongodb.casbah.Imports._

import org.apache.lucene.queryparser.classic.{ QueryParserBase, QueryParser }
import org.apache.lucene.document.FieldType
import java.util
import org.apache.commons.lang.ArrayUtils
import org.apache.http.params.HttpProtocolParams
import org.apache.lucene.util.Version

object Application extends Controller {

  //val dinobuydb = current.configuration.getString("dinobuydb")
  //val fields = MongoDBObject("productkeyid_nvarchar" -> 1, "ec_product.venturestatus_tinyint" -> 1, "ec_product.venturelevelnew_tinyint" -> 1)
  def index = Action {
    Ok(views.html.index("Your new application is ready."))
  }

  def export = Action { request =>
    val page: Int = if (request.getQueryString("page") == Some("") || request.getQueryString("page") == None) 1 else Integer.valueOf(request.getQueryString("page").get)
    val keyword = request.getQueryString("keyword").getOrElse("")
    val parse = new QueryParser(Version.LUCENE_43, "pName", new MyAnalyzer())

    val bq: BooleanQuery = new BooleanQuery()
    val bqWord: BooleanQuery = new BooleanQuery()

    val r = NumericRangeQuery.newIntRange("ventureLevelNew", 0, 0, true, true)

    val q = parse.parse("\"" + keyword.toLowerCase() + "\"")
    val qBrandName = parse.parse("pBrandName:\"" + keyword.toLowerCase() + "\"")

    bq.add(r, BooleanClause.Occur.MUST)
    bqWord.add(q, BooleanClause.Occur.SHOULD)
    bqWord.add(qBrandName, BooleanClause.Occur.SHOULD)
    bq.add(bqWord, BooleanClause.Occur.MUST)
    val searcher: IndexSearcher = SearcherManager.dbSearcher
    val size = 1000
    val start = (page - 1) * size + 1;

    val sortField: SortField = SortField.FIELD_SCORE
    val sot: Sort = new Sort(sortField);

    //分页
    val tsdc: TopFieldCollector = TopFieldCollector.create(sot, start + size, false, false, false, false);
    println(bq)
    searcher.search(bq, tsdc);

    //从0开始计算
    val topDocs: TopDocs = tsdc.topDocs(start - 1, size);
    println(topDocs.totalHits)
    val scoreDocs = topDocs.scoreDocs;
    val sb = new StringBuffer()
    //val total = tsdc.getTotalHits()
    var t1 = System.currentTimeMillis()
    var t2 = System.currentTimeMillis()
    for (i <- 0 until scoreDocs.length) {
      t1 = System.currentTimeMillis()
      val indexDoc = searcher.getIndexReader().document(scoreDocs(i).doc);
      sb.append(indexDoc.get("sku")).append(',')
      if (Integer.valueOf(indexDoc.get("ventureStatus")) == 0) {
        sb.append("绿")
      }
      if (Integer.valueOf(indexDoc.get("ventureStatus")) == 1) {
        sb.append("黄")
      }
      if (Integer.valueOf(indexDoc.get("ventureStatus")) == 2) {
        sb.append("红")
      }
      if (Integer.valueOf(indexDoc.get("ventureStatus")) == 3) {
        sb.append("黑")
      }
      if (Integer.valueOf(indexDoc.get("ventureStatus")) == 4) {
        sb.append("灰")
      }
      if (Integer.valueOf(indexDoc.get("ventureStatus")) == 5) {
        sb.append("棕")
      }
      sb.append("\r\n")
      t2 = System.currentTimeMillis()
      println("load data " + (t2 - t1))
    }
    Ok(sb.toString())
  }

  def search = Action {
    implicit request =>
      val form = Form(
        tuple(
          "keyword" -> text,
          "producttypeid" -> text,
          "productkeyid" -> text,
          "productbrandid" -> text,
          "ranges" -> text,
          "size" -> number,
          "sort" -> text,
          "productaliasname" -> text,
          "businessbrand" -> text,
          "page" -> number,
          "istaobao" -> text,
          "isquality" -> text))
      /*val indexCode = request.getQueryString("indexcode").getOrElse("")
      val productTypeId = request.getQueryString("producttypeid").getOrElse("")
      val productKeyId = request.getQueryString("productkeyid").getOrElse("")
      val productBrandId = request.getQueryString("productbrandid").getOrElse("")

      val range = request.getQueryString("ranges").getOrElse("")
      val sort = request.getQueryString("sort").getOrElse("")

      var productAliasName = request.getQueryString("productaliasname").getOrElse("")
      val businessBrand = request.getQueryString("businessbrand").getOrElse("")

      val currentPage:Int = if(request.getQueryString("page") == Some("") || request.getQueryString("page") == None) 1 else Integer.valueOf(request.getQueryString("page").get)
      val isTaobaoStr:String = request.getQueryString("isTaobao").getOrElse("")
      val isQualityStr:String = request.getQueryString("isQuality").getOrElse("")
      var size:Int = if(request.getQueryString("size") == Some("")) 100 else Integer.valueOf(request.getQueryString("size").getOrElse("100"))*/

      val queryParams = form.bindFromRequest.data
      //Ok("Got: " + id + name)
      val indexCode = Util.getParamString(queryParams, "indexcode", "")
      val productTypeId = Util.getParamString(queryParams, "producttypeid", "")
      val productKeyId = Util.getParamString(queryParams, "productkeyid", "")
      val range = Util.getParamString(queryParams, "ranges", "")
      val page = Util.getParamInt(queryParams, "page", 1)
      var size = Util.getParamInt(queryParams, "size", 20)
      val sort = Util.getParamString(queryParams, "sort", "")
      val productAliasName = Util.getParamString(queryParams, "productaliasname", "").trim
      val businessBrand = Util.getParamString(queryParams, "businessbrand", "")
      val isTaobaoStr = Util.getParamString(queryParams, "istaobao", "")
      val isQualityStr = Util.getParamString(queryParams, "isquality", "")
      val productBrandId = Util.getParamString(queryParams, "productbrandid", "")

      if (size < 1 || size > 1000) {
        size = 100;
      }

      val bq: BooleanQuery = new BooleanQuery()
      val bqKeyword: BooleanQuery = new BooleanQuery()
      if (productAliasName != "") {
        val keywords = productAliasName.toLowerCase().split(" ")
        val bqKeyEn: BooleanQuery = new BooleanQuery()
        val bqTypeName: BooleanQuery = new BooleanQuery
        val bqBrandName: BooleanQuery = new BooleanQuery
        val bqSegmentWord: BooleanQuery = new BooleanQuery
        val bqSourceKeyword: BooleanQuery = new BooleanQuery
        bqSegmentWord.setBoost(10f)
        bqSourceKeyword.setBoost(20f)
        //search title
        for (k <- keywords) {
          val term: Term = new Term("pName", k)
          val pq: PrefixQuery = new PrefixQuery(term)
          bqKeyEn.add(pq, BooleanClause.Occur.MUST)

          val typeNameTerm: Term = new Term("pTypeNameEN", k)
          val qTypeName: TermQuery = new TermQuery(typeNameTerm)
          bqTypeName.add(qTypeName, BooleanClause.Occur.MUST)

          val brandNameTerm: Term = new Term("pBrandName", k)
          val qBrandName: TermQuery = new TermQuery(brandNameTerm)
          bqBrandName.add(qBrandName, BooleanClause.Occur.MUST)

          val segmentWordEnTerm: Term = new Term("segmentWordEn", k)
          val qSegmentWordEn: TermQuery = new TermQuery(segmentWordEnTerm)
          bqSegmentWord.add(qSegmentWordEn, BooleanClause.Occur.MUST)

          val sourceKeywordTerm: Term = new Term("sourceKeyword", k)
          val qSourceKeyword: TermQuery = new TermQuery(sourceKeywordTerm)
          bqSourceKeyword.add(qSourceKeyword, BooleanClause.Occur.MUST)
        }
        bqKeyword.add(bqKeyEn, BooleanClause.Occur.SHOULD)
        bqKeyword.add(bqTypeName, BooleanClause.Occur.SHOULD)
        bqKeyword.add(bqBrandName, BooleanClause.Occur.SHOULD)
        bqKeyword.add(bqSegmentWord, BooleanClause.Occur.SHOULD)
        bqKeyword.add(bqSourceKeyword, BooleanClause.Occur.SHOULD)
        bq.add(bqKeyword, BooleanClause.Occur.MUST)
      }
      
      //search indexCode
      if (indexCode != "") {
        val indexCodeTerm: Term = new Term("indexCode", indexCode);
        val indexCodePQ: PrefixQuery = new PrefixQuery(indexCodeTerm);
        bq.add(indexCodePQ, BooleanClause.Occur.MUST)
      }
      //search producttypeid
      if (productTypeId != "") {
        val typeIds = productTypeId.split(",")
        val bqTypeIds = new BooleanQuery()
        for (typeId <- typeIds) {
          //val term:Term = new Term("pTypeId", typeId);

          //val q:TermQuery = new TermQuery(term)
          val q = NumericRangeQuery.newIntRange("pTypeId", Integer.valueOf(typeId), Integer.valueOf(typeId), true, true)
          bqTypeIds.add(q, BooleanClause.Occur.SHOULD)
        }
        bq.add(bqTypeIds, BooleanClause.Occur.MUST)
      }
      //sku
      if (productKeyId != "") {
        val skus = productKeyId.split(",")
        val bqSkus = new BooleanQuery()
        for (sku <- skus) {
          val term: Term = new Term("sku", sku);
          val q: TermQuery = new TermQuery(term)
          bqSkus.add(q, BooleanClause.Occur.SHOULD)
        }
        bq.add(bqSkus, BooleanClause.Occur.MUST)
      }
      //productBrandId
      if (productBrandId != "") {
        val brandIds = productBrandId.split(",")
        val bqBrandId = new BooleanQuery()
        for (brand <- brandIds) {
          //val term:Term = new Term("pBrandId", brand);
          //val q:TermQuery = new TermQuery(term)
          val q = NumericRangeQuery.newIntRange("pBrandId", Integer.valueOf(brand), Integer.valueOf(brand), true, true)
          bqBrandId.add(q, BooleanClause.Occur.SHOULD)
        }
        bq.add(bqBrandId, BooleanClause.Occur.MUST)
      }
      if (isTaobaoStr != "") {
        /*val term:Term = new Term("isTaobao", isTaobaoStr);
        val q:TermQuery = new TermQuery(term)*/
        val q = NumericRangeQuery.newIntRange("isTaobao", Integer.valueOf(isTaobaoStr), Integer.valueOf(isTaobaoStr), true, true)
        bq.add(q, BooleanClause.Occur.MUST)
      }
      if (isQualityStr != "") {
        /*val term:Term = new Term("isQuality", isQualityStr);
        val q:TermQuery = new TermQuery(term)*/
        val q = NumericRangeQuery.newIntRange("isQuality", Integer.valueOf(isQualityStr), Integer.valueOf(isQualityStr), true, true)
        bq.add(q, BooleanClause.Occur.MUST)
      }

      if (businessBrand != "") {
        val businessBrandTerm: Term = new Term("businessBrand", businessBrand.toLowerCase());
        val businessBrandPQ: PrefixQuery = new PrefixQuery(businessBrandTerm);
        bq.add(businessBrandPQ, BooleanClause.Occur.MUST)
      }
      ranges(range.split(","), bq)
      val sot: Sort = sorts(sort);
      val ids = ListBuffer[Long]()

      val searcher: IndexSearcher = SearcherManager.dbSearcher

      val start = (page - 1) * size + 1;
      //分页
      val tsdc: TopFieldCollector = TopFieldCollector.create(sot, start + size, false, false, false, false);
      println(bq)
      searcher.search(bq, tsdc);

      //从0开始计算
      val topDocs: TopDocs = tsdc.topDocs(start - 1, size);
      val scoreDocs = topDocs.scoreDocs;
      val total = tsdc.getTotalHits()
      for (i <- 0 until scoreDocs.length) {
        val indexDoc = searcher.getIndexReader().document(scoreDocs(i).doc);
        ids += Long.valueOf(indexDoc.get("pId"))
      }

      val sb = new StringBuffer()
      for (x <- ids) {
        sb.append(x).append(',')
      }

      //Ok(Json.toJson(jsonObject).toString())
      if (sb.length() > 0) {
        Ok(Json.obj("productIds" -> sb.substring(0, sb.length() - 1), "totalHits" -> total))
      } else {
        Ok(Json.obj("productIds" -> "", "totalHits" -> total))
      }
  }

  def searchOldDDInc = Action {
    implicit request =>
      val form = Form(
        tuple(
          "keyword" -> text,
          "page" -> text,
          "size" -> text))

      val queryParams = form.bindFromRequest.data
      val page = Util.getParamInt(queryParams, "page", 1)
      var size = Util.getParamInt(queryParams, "size", 20)
      val keyword = Util.getParamString(queryParams, "keyword", "").trim

      if (size < 1 || size > 1000) {
        size = 100;
      }

      val bq: BooleanQuery = new BooleanQuery()
      if (keyword != "") {
        val keywords = keyword.toLowerCase().split(" ")
        val bqKeyEn: BooleanQuery = new BooleanQuery()
        //search title
        for (k <- keywords) {
          val term: Term = new Term("pName", k)
          val pq: PrefixQuery = new PrefixQuery(term)
          bqKeyEn.add(pq, BooleanClause.Occur.MUST)
        }
        bq.add(bqKeyEn, BooleanClause.Occur.SHOULD)
      }

      if (keyword != "") {
        val keywords = keyword.toLowerCase().split(" ")
        val bqKeyEn: BooleanQuery = new BooleanQuery()
        for (k <- keywords) {
          val term: Term = new Term("sourceKeyword", k)
          val pq: TermQuery = new TermQuery(term)
          bqKeyEn.add(pq, BooleanClause.Occur.MUST)
        }
        bq.add(bqKeyEn, BooleanClause.Occur.SHOULD)
      }

      val sot: Sort = sorts(null);
      val ids = ListBuffer[Long]()

      val searcher: IndexSearcher = SearcherManager.oldIncSearcher

      val start = (page - 1) * size + 1;
      //分页
      val tsdc: TopFieldCollector = TopFieldCollector.create(sot, start + size, false, false, false, false);
      println(bq)
      searcher.search(bq, tsdc);

      //从0开始计算
      val topDocs: TopDocs = tsdc.topDocs(start - 1, size);
      val scoreDocs = topDocs.scoreDocs;
      val total = tsdc.getTotalHits()
      for (i <- 0 until scoreDocs.length) {
        val indexDoc = searcher.getIndexReader().document(scoreDocs(i).doc);
        ids += Long.valueOf(indexDoc.get("pId"))
      }

      val sb = new StringBuffer()
      for (x <- ids) {
        sb.append(x).append(',')
      }

      //Ok(Json.toJson(jsonObject).toString())
      if (sb.length() > 0) {
        Ok(Json.obj("productIds" -> sb.substring(0, sb.length() - 1), "totalHits" -> total))
      } else {
        Ok(Json.obj("productIds" -> "", "totalHits" -> total))
      }
  }

  def sorts(sort: String): Sort = {
    //排序
    val sortStrs: Array[String] = sort.split(",")
    val lst = ListBuffer[SortField]()
    for (s <- sortStrs) {
      var sortField: SortField = null
      if ("price-".equals(s)) {
        sortField = new SortField("unitPrice", SortField.Type.DOUBLE, true);
      } else if ("price+".equals(s)) {
        sortField = new SortField("unitPrice", SortField.Type.DOUBLE, false);
      } else if ("date-".equals(s)) {
        sortField = new SortField("createTime", SortField.Type.DOUBLE, true);
      } else if ("date+".equals(s)) {
        sortField = new SortField("createTime", SortField.Type.DOUBLE, false);
      } else if ("isqualityproduct-".equals(s)) {
        sortField = new SortField("isQuality", SortField.Type.INT, true);
      } else if ("isqualityproduct+".equals(s)) {
        sortField = new SortField("isQuality", SortField.Type.INT, false);
      }
      if (sortField != null) {
        lst += sortField
      }
    }

    var sot: Sort = null;
    if (lst.length == 0) {
      //默认按相关度排序
      //sortField = ;
      sot = new Sort(SortField.FIELD_SCORE);
    } else {
      val list = new util.ArrayList[SortField]();
      for (x <- lst) {
        list.add(x)
      }
      sot = Helper.addSortField(list)
    }
    sot;
  }
  def ranges(ranges: Array[String], bq: BooleanQuery) {
    //范围查询

    for (range <- ranges) {
      val parts = range.split(":");
      if (parts.length == 3) {
        if (parts(0).equals("unitprice")) {
          val nrq = NumericRangeQuery.newDoubleRange("unitPrice", Double.valueOf(parts(1)), Double.valueOf(parts(2)), true, true);
          bq.add(nrq, BooleanClause.Occur.MUST);
        }
        if (parts(0).equals("createtime")) {
          val query: TermRangeQuery = new TermRangeQuery("createTime", new BytesRef(parts(1)), new BytesRef(parts(2)), true, true);
          bq.add(query, BooleanClause.Occur.MUST);
        }
        if (parts(0).equals("isqualityproduct")) {
          val query = NumericRangeQuery.newIntRange("isQuality", Integer.valueOf(parts(1)), Integer.valueOf(parts(2)), true, true);
          bq.add(query, BooleanClause.Occur.MUST);
        }
        if (parts(0).equals("venturestatus")) {
          /*val query:TermRangeQuery = new TermRangeQuery("ventureStatus", new BytesRef(parts(1)), new BytesRef(parts(2)), true, true);*/
          val query = NumericRangeQuery.newIntRange("ventureStatus", Integer.valueOf(parts(1)), Integer.valueOf(parts(2)), true, true);
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
        "sort" -> text))
    /*val anyData = Map("id" -> "111", "name" -> "secret")
    val (id, name) = form.bind(anyData).get*/
    val queryParams = form.bindFromRequest.data
    //Ok("Got: " + id + name)
    var keyword = Util.getParamString(queryParams, "keyword", "")
    val country = Util.getParamString(queryParams, "country", "")
    val indexCode = Util.getParamString(queryParams, "indexCode", "")
    val range = Util.getParamString(queryParams, "range", "")
    var currentPage = Util.getParamInt(queryParams, "currentPage", 1)
    var size = Util.getParamInt(queryParams, "size", 100)
    val sort = Util.getParamString(queryParams, "sort", "")
    if (currentPage < 1) {
      currentPage = 1
    }
    if (size < 1 || size > 100) {
      size = 20
    }

    keyword = keyword.trim()
    keyword = QueryParserBase.escape(keyword);

    //Ok("Got: " + id + name)
    Ok("Got: ")
  }

  def searchOldDDInctest = Action {
    implicit request =>
      val form = Form(
        tuple(
          "keyword" -> text,
          "page" -> text,
          "size" -> text))

      val queryParams = form.bindFromRequest.data
      val page = Util.getParamInt(queryParams, "page", 1)
      var size = Util.getParamInt(queryParams, "size", 20)
      val keyword = Util.getParamString(queryParams, "keyword", "").trim

      if (size < 1 || size > 1000) {
        size = 100;
      }

      val bq: BooleanQuery = new BooleanQuery()
      if (keyword != "") {
        val keywords = keyword.toLowerCase().split(" ")
        val bqKeyEn: BooleanQuery = new BooleanQuery()
        //search title
        for (k <- keywords) {
          val term: Term = new Term("pName", k)
          val pq: PrefixQuery = new PrefixQuery(term)
          bqKeyEn.add(pq, BooleanClause.Occur.MUST)
        }
        bq.add(bqKeyEn, BooleanClause.Occur.SHOULD)
      }

      if (keyword != "") {
        val keywords = keyword.toLowerCase().split(" ")
        val bqKeyEn: BooleanQuery = new BooleanQuery()
        for (k <- keywords) {
          val term: Term = new Term("sourceKeyword", k)
          val pq: TermQuery = new TermQuery(term)
          bqKeyEn.add(pq, BooleanClause.Occur.MUST)
        }
        bq.add(bqKeyEn, BooleanClause.Occur.SHOULD)
      }

      val sot: Sort = sorts(null);
      val ids = ListBuffer[String]()

      val searcher: IndexSearcher = SearcherManager.oldIncSearcher

      val start = (page - 1) * size + 1;
      //分页
      val tsdc: TopFieldCollector = TopFieldCollector.create(sot, start + size, false, false, false, false);
      println(bq)
      searcher.search(bq, tsdc);

      //从0开始计算
      val topDocs: TopDocs = tsdc.topDocs(start - 1, size);
      val scoreDocs = topDocs.scoreDocs;
      val total = tsdc.getTotalHits()
      for (i <- 0 until scoreDocs.length) {
        val indexDoc = searcher.getIndexReader().document(scoreDocs(i).doc);
        ids += indexDoc.get("pName") + "####" + indexDoc.get("sourceKeyword") + "\r\n"
      }

      val sb = new StringBuffer()
      for (x <- ids) {
        sb.append(x).append(',')
      }

      //Ok(Json.toJson(jsonObject).toString())
      if (sb.length() > 0) {
        Ok(Json.obj("productIds" -> sb.substring(0, sb.length() - 1), "totalHits" -> total))
      } else {
        Ok(Json.obj("productIds" -> "", "totalHits" -> total))
      }
  }

  def searchKeywordGroupByCategory = Action { implicit request =>
    val form = Form(
      tuple(
        "keyword" -> text,
        "sort" -> text,
        "indexcode" -> text,
        "istaobao" -> text,
        "size" -> number,
        "isquality" -> text,
        "brandid" -> text,
        "page" -> number))

    val queryParams = form.bindFromRequest.data
    var page = Util.getParamInt(queryParams, "page", 1)
    var size = Util.getParamInt(queryParams, "size", 20)
    val keyword = Util.getParamString(queryParams, "keyword", "").trim.toLowerCase
    val indexCode = Util.getParamString(queryParams, "indexcode", "").trim.toLowerCase
    val isTaobaoStr = Util.getParamString(queryParams, "istaobao", "")
    val isQualityStr = Util.getParamString(queryParams, "isquality", "")
    val brandIdStr = Util.getParamString(queryParams, "brandid", "")

    val sort = Util.getParamString(queryParams, "sort", "").trim

    if (page < 0) {
      page = 1
    }

    if (size < 1 || size > 100) {
      size = 20;
    }

    val bq: BooleanQuery = new BooleanQuery()
    val bqKeyword: BooleanQuery = new BooleanQuery()
    val bqBrand: BooleanQuery = new BooleanQuery()
    val bqSearch: BooleanQuery = new BooleanQuery()
    
    if (keyword != "") {
      val keywordSplit = keyword.toLowerCase().split(" ")
      val bqKeyEn: BooleanQuery = new BooleanQuery()
      val bqTypeName: BooleanQuery = new BooleanQuery
      val bqBrandName: BooleanQuery = new BooleanQuery
      val bqSegmentWord: BooleanQuery = new BooleanQuery
      val bqSourceKeyword: BooleanQuery = new BooleanQuery
      bqSegmentWord.setBoost(10f)
      bqSourceKeyword.setBoost(20f)
      for (k <- keywordSplit) {
        val term: Term = new Term("pName", k)
        val pq: PrefixQuery = new PrefixQuery(term)
        bqKeyEn.add(pq, BooleanClause.Occur.MUST)

        val typeNameTerm: Term = new Term("pTypeNameEN", k)
        val qTypeName: TermQuery = new TermQuery(typeNameTerm)
        bqTypeName.add(qTypeName, BooleanClause.Occur.MUST)

        val brandNameTerm: Term = new Term("pBrandName", k)
        val qBrandName: TermQuery = new TermQuery(brandNameTerm)
        bqBrandName.add(qBrandName, BooleanClause.Occur.MUST)

        val segmentWordEnTerm: Term = new Term("segmentWordEn", k)
        val qSegmentWordEn: TermQuery = new TermQuery(segmentWordEnTerm)
        bqSegmentWord.add(qSegmentWordEn, BooleanClause.Occur.MUST)

        val sourceKeywordTerm: Term = new Term("sourceKeyword", k)
        val qSourceKeyword: TermQuery = new TermQuery(sourceKeywordTerm)
        bqSourceKeyword.add(qSourceKeyword, BooleanClause.Occur.MUST)
      }
      bqSearch.add(bqKeyEn, BooleanClause.Occur.SHOULD)
      bqSearch.add(bqTypeName, BooleanClause.Occur.SHOULD)
      bqSearch.add(bqBrandName, BooleanClause.Occur.SHOULD)
      bqSearch.add(bqSegmentWord, BooleanClause.Occur.SHOULD)
      bqSearch.add(bqSourceKeyword, BooleanClause.Occur.SHOULD)
    }
    bq.add(bqSearch, BooleanClause.Occur.MUST)
    if (indexCode != "") {
      val term: Term = new Term("indexCode", indexCode)
      val q: TermQuery = new TermQuery(term)
      bq.add(q, BooleanClause.Occur.MUST)
    }

    if (brandIdStr != "") {
      val q = NumericRangeQuery.newIntRange("pBrandId", Integer.valueOf(brandIdStr), Integer.valueOf(brandIdStr), true, true)
      bq.add(q, BooleanClause.Occur.MUST)

    }

    if (isTaobaoStr != "") {
      /*val term:Term = new Term("isTaobao", isTaobaoStr);
      val q:TermQuery = new TermQuery(term)*/
      val q = NumericRangeQuery.newIntRange("isTaobao", Integer.valueOf(isTaobaoStr), Integer.valueOf(isTaobaoStr), true, true)
      bq.add(q, BooleanClause.Occur.MUST)
    }
    if (isQualityStr != "") {
      /*val term:Term = new Term("isQuality", isQualityStr);
      val q:TermQuery = new TermQuery(term)*/
      val q = NumericRangeQuery.newIntRange("isQuality", Integer.valueOf(isQualityStr), Integer.valueOf(isQualityStr), true, true)
      bq.add(q, BooleanClause.Occur.MUST)
    }

    /*bqSearch.add(bqKeyword, BooleanClause.Occur.SHOULD)
    bqSearch.add(bqBrand, BooleanClause.Occur.SHOULD)*/

    
    val searcher: IndexSearcher = SearcherManager.dbSearcher
    val start = (page - 1) * size + 1;
    val sot: Sort = sorts(sort);
    val tsdc: TopFieldCollector = TopFieldCollector.create(sot, start + size, false, false, false, false);

    println(bq)
    searcher.search(bq, tsdc);

    val topDocs: TopDocs = tsdc.topDocs(start - 1, size);
    val scoreDocs = topDocs.scoreDocs;
    val total = tsdc.getTotalHits()
    val ids = ListBuffer[Int]()
    for (i <- 0 until scoreDocs.length) {
      val indexDoc = searcher.getIndexReader().document(scoreDocs(i).doc);
      ids += Integer.valueOf(indexDoc.get("pId"))
    }

    val sb = new StringBuffer()
    for (x <- ids) {
      sb.append(x).append(',')
    }

    val groupingSearch = new GroupingSearch("indexCode");
    /*groupingSearch.setGroupSort(groupSort);
    groupingSearch.setFillSortFields(fillFields);*/
    groupingSearch.setAllGroups(true);
    val lst: ListBuffer[JsValue] = ListBuffer[JsValue]()
    val result: TopGroups[BytesRef] = groupingSearch.search(searcher, bq, 0, 10000);
    for (x <- result.groups) {
      lst += toJson(
        Map(
          "indexCode" -> toJson(x.groupValue.utf8ToString()),
          "count" -> toJson(x.totalHits)))
    }
    Ok(
      toJson(
        Map(
          "total" -> toJson(total),
          "productIds" -> toJson(ids.toArray),
          "category" -> toJson(lst.toArray))))
  }

}