package controllers

import org.apache.lucene.util.BytesRef
import play.api.mvc._
import play.api.libs.json.{JsArray, Writes, JsValue, Json}
import play.api.libs.json.Json._
import scala.collection.mutable.{HashMap, Queue, ListBuffer}
import java.lang.Long
import org.apache.lucene.search._
import my.finder.search.service.{DDSearchService, Helper, SearcherManager}
import org.apache.lucene.index.{IndexableField, Term}
import play.api.data._
import play.api.data.Forms._
import my.finder.common.util.Util

import java.util.{List, Calendar, Date}
import java.text.{DateFormat, SimpleDateFormat}
import scala.xml.{Null, Text, Attribute, Node}
import org.apache.lucene.search.grouping.{TopGroups, GroupingSearch}
import scala.Predef._
import org.apache.commons.lang.StringUtils
import my.finder.common.model.{ErrorResult, IdsPageResult}
import org.apache.lucene.search.BooleanClause.Occur

object Dino extends Controller {
  implicit val ids1PageResultWrites = new Writes[IdsPageResult] {
    def writes(c: IdsPageResult): JsValue = {
      Json.obj(
        "page" -> c.page,
        "size" -> c.size,
        "total" -> c.total,
        "ids" -> c.data,
        "query" -> c.query
      )
    }
  }
  def shop = Action { implicit request =>
    val form = Form(
      tuple(
        "sort" -> text,
        "size" -> number,
        "page" -> number,
        "country" -> text,
        "indexcode" -> text,
        "shopId" -> text,
        "keyword" -> text
      )
    )
    val queryParams = form.bindFromRequest.data
    val result = DDSearchService.shop(queryParams)
    val json = if (result.isInstanceOf[IdsPageResult]) {
      toJson(result.asInstanceOf[IdsPageResult])
    } else if (result.isInstanceOf[ErrorResult]){
      //toJson(result.asInstanceOf[ErrorResult])
      Json.parse("{}")
    } else {
      Json.parse("{}")
    }


    /*if (result.isInstanceOf[ErrorResult]) {
      Ok(toJson(result.asInstanceOf[ErrorResult]))
    }*/
    Ok(json)
  }
  def category = Action { implicit request =>
    val form = Form(
      tuple(
        "sort" -> text,
        "size" -> number,
        "page" -> number,
        "country" -> text,
        "indexcode" -> text,
        "attributes" -> text
      )
    )
    val queryParams = form.bindFromRequest.data
    val sort = Util.getParamString(queryParams, "sort", "")
    var size = Util.getParamInt(queryParams, "size", 20)
    var page = Util.getParamInt(queryParams, "page", 1)
    val country = Util.getParamString(queryParams, "country", "")
    val indexCode = Util.getParamString(queryParams, "indexcode", "")
    val attributes = Util.getParamString(queryParams, "attributes", "")
    if(StringUtils.isBlank(indexCode)){
      Ok(empty)
    }
    
    if (page < 0) {
      page = 1
    }

    if (size < 1 || size > 100) {
      size = 20;
    }

    val bqAll = new BooleanQuery
    val tIndexCode = new Term("indexCode",indexCode)
    val qIndexCode = new PrefixQuery(tIndexCode)
    bqAll.add(qIndexCode,Occur.MUST)

    val attrSplit = attributes.trim.toLowerCase.split(",")
    /*for(attr <- attrSplit if(attr.trim != "")){
      val tAttribute = new Term("attribute",attribute)
      val qAttribute = new TermQuery(tAttribute)
      bqAll.add(qAttribute,BooleanClause.Occur.MUST)
    }

    val searcher: IndexSearcher = SearcherManager.ddSearcher

    val start = (page - 1) * size + 1;
    //分页
    val tsdc: TopFieldCollector = TopFieldCollector.create(sot, start + size, false, false, false, false);
    println(bqAll)
    searcher.search(bqAll, tsdc);

    //从0开始计算
    val topDocs: TopDocs = tsdc.topDocs(start - 1, size);
    val scoreDocs = topDocs.scoreDocs;
    val total = tsdc.getTotalHits()
    val ids = ListBuffer[Long]()
    for (i <- 0 until scoreDocs.length) {
      val indexDoc = searcher.getIndexReader().document(scoreDocs(i).doc);
      ids += Long.valueOf(indexDoc.get("pId"))
    }

    Ok(
      toJson(
        Map(
          "total" -> toJson(total),
          "productIds" -> toJson(ids.toArray)
          )
      )
    )*/
    Ok("")
  }
  def empty: String = {
    "{\"totalHits\":0,\"productIds\":[]}"
  }
  def productJSON = Action { implicit request =>
    val form = Form(
      tuple(
        "keyword" -> text,
        "sort" -> text,
        "size" -> number,
        "page" -> number,
        "range" -> text,
        "country" -> text,
        "indexcode" -> text,
        "tag"-> text
      )
    )
    val queryParams = form.bindFromRequest.data
    searchProduct(queryParams,"json")
    Ok("productJSON")
  }

  def productXML = Action { implicit request =>
    val form = Form(
      tuple(
        "page" -> number,
        "size" -> number,
        "keyword" -> text,
        "range" -> text,
        "tag"-> text,
        "indexcode" -> text,
        "attribute" -> text,
        "country" -> text,
        "eliminateid" -> text,
        "ecProduct001" -> text,
        "sort" -> text
      )
    )
    val queryParams = form.bindFromRequest.data
    searchProduct(queryParams,"xml")

    Ok("productXML")
  }

  private def searchProduct(queryParams:Map[String,String],format:String) = {

    //设置参数
    val page = Util.getPage(queryParams, 1)
    val size = Util.getSize(queryParams, 20)
    val keyword = Util.getParamString(queryParams, "keyword", "")
    val sort = Util.getParamString(queryParams, "sort", "").trim
    val tag = Util.getParamString(queryParams, "tag", "").trim
    val indexcode = Util.getParamString(queryParams, "indexcode", "").trim
    val attribute = Util.getParamString(queryParams, "attribute", "").trim
    val country = Util.getParamString(queryParams, "country", "").trim
    val excludeId = Util.getParamString(queryParams, "excludeid", "").trim
    val ecProduct001 = Util.getParamString(queryParams, "ecProduct001", "").trim

    val bqAll: BooleanQuery = new BooleanQuery()
    
    bqAll.add(DDSearchService.getKeyWordQuery(keyword), Occur.MUST)
    bqAll.add(getTag(tag), Occur.MUST)
    bqAll.add(getIndexCode(indexcode), Occur.MUST)
    bqAll.add(getAttributeQuery(attribute), Occur.MUST)
    bqAll.add(getExcludeIdQuery(excludeId), Occur.MUST_NOT)
    bqAll.add(getExcludeAreaQuery(ecProduct001), Occur.MUST)

    //查询产品
    val sot: Sort = DDSearchService.sorts(sort);
    val ids = ListBuffer[Long]()
    val searcher: IndexSearcher = SearcherManager.searcher

    val start = (page - 1) * size + 1;
    //分页
    val tsdc: TopFieldCollector = TopFieldCollector.create(sot, start + size, false, false, false, false);
    searcher.search(bqAll, tsdc);
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

    if (sb.length() > 0) {
      Ok(Json.obj("productIds" -> sb.substring(0, sb.length() - 1), "totalHits" -> total))
    } else {
      Ok(Json.obj("productIds" -> "", "totalHits" -> total))
    }

    //返回品类
    val groupingSearch = new GroupingSearch("indexCode");
    groupingSearch.setAllGroups(true)
    val lst: ListBuffer[JsValue] = ListBuffer[JsValue]()
    val result: TopGroups[BytesRef] = groupingSearch.search(searcher, bqAll, 0, 10000)
    var f2 = new StringBuffer()
    var f3 = new StringBuffer()

    for (x <- result.groups) {
      val ic = x.groupValue.utf8ToString()

      if(ic.length == 8) {
        f2.append(x).append(",")
      }else if(ic.length == 12) {
        f3.append(x).append(",")
      }
     /* val p = new HashMap[Int,String]()
      val indexmp: HashMap[Int,String] = getIndexCode(x.groupValue.utf8ToString(),p)
      lst += toJson(
        for(y <- indexmp.iterator) {
          Map(
            "level" -> toJson(y._1),
            "indexCode" -> toJson(y._2)
          )
        }
      )*/
    }
    val s2 = f2.substring(0,f2.length()-1)
    val s3 = f3.substring(0,f3.length()-1)
    var n2 = 0
    var n3 = 0
    var min = 0
    var max = 0

    for (x <- result.groups) {
      for(c2 <- s2.split(",")) {
        var count = 0
        for(c3 <- s3.split(",")) {
         count = count + 1
         if(c2 == c3.substring(0,8)){
           n3 = n3 + x.totalHits
         }
        }
      }
    }

    //返回格式xml
    var nodes = new Queue[Node]()
    nodes += <total>{ topDocs.totalHits }</total>
    for (i <- 0 until scoreDocs.length) {
      val indexDoc = searcher.getIndexReader().document(scoreDocs(i).doc)
      docToXML(nodes, indexDoc)
    }
    Ok(<root>{ nodes }</root>)

  }

  private def getIndexCode(indexcode: String,p: HashMap[Int,String]) = {

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

  
  //tag: all new event clear
  //+(isLifeStyle:true isDailyDeal:true isEventProduct:true)
  //+isClearanceTinyint:1
  //-ecProduct001.productCountryInfoForCreatorNvarchar:344_0
  def getTag(tag: String):BooleanQuery = {

    val bqTagSub: BooleanQuery = new BooleanQuery()
    val bqTag: BooleanQuery = new BooleanQuery()
    if(tag != null) {
      if("new".equals(tag)) {
        val c: Calendar = Calendar.getInstance()
        val date = new Date()
        c.setTime(date)
        c.add(Calendar.DAY_OF_YEAR,-7)
        val df: DateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val before7day = df.format(c.getTime)
        val today = df.format(date)

        val createTimePq: TermRangeQuery = new TermRangeQuery("createTime", new BytesRef(before7day), new BytesRef(today), true, true);
        bqTag.add(createTimePq, BooleanClause.Occur.MUST)

      }else if("event".equals(tag)) {
        val isLifeStyleTerm: Term = new Term("isLifeStyle","true")
        val isLifeStylePq: TermQuery = new TermQuery(isLifeStyleTerm)
        bqTagSub.add(isLifeStylePq, BooleanClause.Occur.SHOULD)

        val isDailyDealTerm: Term = new Term("isDailyDeal","true")
        val isDailyDealPq: TermQuery = new TermQuery(isDailyDealTerm)
        bqTagSub.add(isDailyDealPq, BooleanClause.Occur.SHOULD)

        val isEventProductTerm: Term = new Term("isEventProduct","true")
        val isEventProductPq: TermQuery = new TermQuery(isEventProductTerm)
        bqTagSub.add(isEventProductPq, BooleanClause.Occur.SHOULD)

        bqTag.add(bqTagSub, BooleanClause.Occur.MUST)
      }else if("clear".equals(tag)) {
        val isClearanceTinyintTerm: Term = new Term("isClearanceTinyint","1")
        val isClearanceTinyintPq: TermQuery = new TermQuery(isClearanceTinyintTerm)
        bqTag.add(isClearanceTinyintPq, BooleanClause.Occur.MUST)
      }
      println(bqTag)
    }
    bqTag
  }

  //查类别
  def getIndexCode(indexcode: String):Query = {
    val q = if (StringUtils.isNotBlank(indexcode)) {
      val tIndexCode: Term = new Term("indexcode",indexcode)
      val tqIndexCode: TermQuery = new TermQuery(tIndexCode)
      tqIndexCode
    } else {
      null
    }
    q
  }

  //查产品属性 ###a###b###,###c###d###
  def getAttributeQuery(attribute: String):Query = {
    val bqAttr = if(StringUtils.isNotBlank(attribute)) {
      val bqAttr: BooleanQuery = new BooleanQuery()
      val attributeSplit = attribute.split(",")
      for(att <- attributeSplit) {
          val tAttr: Term = new Term("attribute","\""+att+"\"")
          val tqAttr: TermQuery = new TermQuery(tAttr)
          bqAttr.add(tqAttr, BooleanClause.Occur.MUST)
      }
    } else {
      null
    }
    bqAttr
  }

  def getExcludeIdQuery(excludeId: String):BooleanQuery = {
    val q = if (StringUtils.isNotBlank(excludeId)) {
      val bqExcludeId: BooleanQuery = new BooleanQuery()
      val excludeIdSplit = excludeId.split(",")
      for(eli <- excludeIdSplit) {
        val tExcludeId: Term = new Term("id",eli)
        val tqExcludeId: TermQuery = new TermQuery(tExcludeId)
        bqExcludeId.add(tqExcludeId, Occur.MUST)
      }
    } else {
      null
    }
    q
  }

  def getExcludeAreaQuery(ecProduct001: String):BooleanQuery = {
    val bqEcProduct001: BooleanQuery = new BooleanQuery()
    if(ecProduct001 != null) {
      val ecProduct001Term: Term = new Term("ecProduct001",ecProduct001)
      val ecProduct001Pq: PrefixQuery = new PrefixQuery(ecProduct001Term)
      bqEcProduct001.add(ecProduct001Pq, BooleanClause.Occur.MUST)
      println(bqEcProduct001)
    }
    bqEcProduct001
  }
  

  //返回属性
  private def searchProductAttribute(bq: BooleanQuery) = {

    val attributesSearcher: IndexSearcher = SearcherManager.searcher
    val tsdc: TopFieldCollector = TopFieldCollector.create(null, 100, false, false, false, false)
    attributesSearcher.search(bq, tsdc)
    val topDocs: TopDocs = tsdc.topDocs(0, 100);
    val scoreDocs = topDocs.scoreDocs;

    val attributeVuale = new StringBuffer()
    var categoryId: Long = 0L
    var categoryName: String = ""
    var preCategoryId = 0L
    val mp = new HashMap[HashMap[Long,String],StringBuffer]()
    for (i <- 0 until scoreDocs.length) {
      val indexDoc = attributesSearcher.getIndexReader().document(scoreDocs(i).doc);
      categoryId = Long.valueOf(indexDoc.get("categoryId"))
      categoryName = String.valueOf(indexDoc.get("categoryName"))
      val mpinner = new HashMap[Long,String]()
      mpinner.put(categoryId,categoryName)

      if(preCategoryId != categoryId) {
        attributeVuale.delete(0,attributeVuale.length())
      }
      attributeVuale.append(indexDoc.get("attributeVuale"))

      mp.put(mpinner,attributeVuale)
      preCategoryId =  categoryId
    }

    if(!mp.isEmpty) {
      for (x <- mp.iterator) {
        var cid = 0L
        var cname = ""
        var sb = new StringBuffer()
        var mp1 = new HashMap[Long,String]()
        mp1 = x._1
        sb = x._2
        for(y <- mp1.iterator) {
          cid  = y._1
          cname = y._2
          Ok(Json.obj("categoryId" ->cid ,"categoryName" ->cname , "attributeVuale" -> sb.toString))
        }
      }
    }
  }
}
