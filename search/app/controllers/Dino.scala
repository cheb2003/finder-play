package controllers

import org.apache.lucene.util.BytesRef
import play.api.mvc._
import play.api.libs.json.{JsValue, Json}
import play.api.libs.json.Json._
import scala.collection.mutable.{HashMap, Queue, ListBuffer}
import java.lang.Long
import org.apache.lucene.search._
import my.finder.search.service.{Helper, SearcherManager}
import org.apache.lucene.index.{IndexableField, Term}
import play.api.data._
import play.api.data.Forms._
import my.finder.common.util.Util
import java.util
import java.util.{List, Calendar, Date}
import java.text.{DateFormat, SimpleDateFormat}
import scala.xml.{Null, Text, Attribute, Node}
import org.apache.lucene.search.grouping.{TopGroups, GroupingSearch}
import scala.Predef._
import org.apache.commons.lang.StringUtils

object Dino extends Controller {


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
    bqAll.add(qIndexCode,BooleanClause.Occur.MUST)

    val attrSplit = attributes.trim.toLowerCase.split(",")
    for(attr <- attrSplit if(attr.trim != "")){
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
    )
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
    var page = Util.getParamInt(queryParams, "page", 1)
    var size = Util.getParamInt(queryParams, "size", 20)
    val keyword = Util.getParamString(queryParams, "keyword", "").trim.toLowerCase
    val sort = Util.getParamString(queryParams, "sort", "").trim
    val tag = Util.getParamString(queryParams, "tag", "").trim
    val indexcode = Util.getParamString(queryParams, "indexcode", "").trim
    val attribute = Util.getParamString(queryParams, "attribute", "").trim
    val country = Util.getParamString(queryParams, "country", "").trim
    val eliminateid = Util.getParamString(queryParams, "eliminateid", "").trim
    val ecProduct001 = Util.getParamString(queryParams, "ecProduct001", "").trim

    val bqAll: BooleanQuery = new BooleanQuery()
    if (page < 0) {
      page = 1
    }

    if (size < 1 || size > 100) {
      size = 20;
    }
    bqAll.add(getKeyWord(keyword), BooleanClause.Occur.MUST)
    bqAll.add(getTag(tag), BooleanClause.Occur.MUST)
    bqAll.add(getIndexcode(indexcode), BooleanClause.Occur.MUST)
    bqAll.add(getAttribute(attribute), BooleanClause.Occur.MUST)
    bqAll.add(getEliminateid(eliminateid), BooleanClause.Occur.MUST)
    bqAll.add(getEcProduct001(ecProduct001), BooleanClause.Occur.MUST)
    println(bqAll)

    //查询产品
    val sot: Sort = sorts(sort);
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

  def getKeyWord(keyword: String):BooleanQuery = {

    val bq: BooleanQuery = new BooleanQuery()
    //查关键字
    if (keyword != "") {
      val keywordSplit = keyword.split(" ")

      val bqKeyEn: BooleanQuery = new BooleanQuery()
      val bqKeywordBoundCategory: BooleanQuery = new BooleanQuery()
      val bqseokeyword: BooleanQuery = new BooleanQuery()
      val bqtype: BooleanQuery = new BooleanQuery()
      val bqshortdes: BooleanQuery = new BooleanQuery()
      val bqbrand: BooleanQuery = new BooleanQuery()

      for (k <- keywordSplit) {
        //名称 40
        val nameTerm: Term = new Term("name", k)
        val namePq: PrefixQuery = new PrefixQuery(nameTerm)
        bqKeyEn.add(namePq, BooleanClause.Occur.MUST)

        //绑定品类 90
        val keywordBoundCategoryTerm: Term = new Term("keywordBoundCategory", k)
        val keywordBoundCategoryPq: TermQuery = new TermQuery(keywordBoundCategoryTerm)
        bqKeywordBoundCategory.add(keywordBoundCategoryPq, BooleanClause.Occur.MUST)

        //seokeyword 7
        val seokeywordTerm: Term = new Term("seokeyword", k)
        val seokeywordPq: TermQuery = new TermQuery(seokeywordTerm)
        bqseokeyword.add(seokeywordPq, BooleanClause.Occur.MUST)

        //品类 3
        val typeTerm: Term = new Term("type", k)
        val typePq: TermQuery = new TermQuery(typeTerm)
        bqtype.add(typePq, BooleanClause.Occur.MUST)

        //短描述2
        val shortdesTerm: Term = new Term("shortdes", k)
        val shortdesPq: TermQuery = new TermQuery(shortdesTerm)
        bqshortdes.add(shortdesPq, BooleanClause.Occur.MUST)

        //品牌 18
        val brandTerm: Term = new Term("brand", k)
        val brandPq: TermQuery = new TermQuery(brandTerm)
        bqbrand.add(brandPq, BooleanClause.Occur.MUST)
      }
      bqKeyEn.setBoost(40f)
      bqKeywordBoundCategory.setBoost(90f)
      bqseokeyword.setBoost(7f)
      bqtype.setBoost(3f)
      bqshortdes.setBoost(2f)
      bqbrand.setBoost(18f)

      bq.add(bqKeyEn, BooleanClause.Occur.SHOULD)
      bq.add(bqKeywordBoundCategory, BooleanClause.Occur.SHOULD)
      bq.add(bqseokeyword, BooleanClause.Occur.SHOULD)
      bq.add(bqtype, BooleanClause.Occur.SHOULD)
      bq.add(bqshortdes, BooleanClause.Occur.SHOULD)
      bq.add(bqbrand, BooleanClause.Occur.SHOULD)
      println(bq)
    }
    bq
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
  def getIndexcode(indexcode: String):BooleanQuery = {

    val bqIndex: BooleanQuery = new BooleanQuery()
    if (indexcode != "") {
      val indexcodeTerm: Term = new Term("indexcode",indexcode)
      val indexcodePq: TermQuery = new TermQuery(indexcodeTerm)
      bqIndex.add(indexcodePq, BooleanClause.Occur.MUST)
      println(bqIndex)
    }
    bqIndex
  }

  //查产品属性 ###a###b### ###c###d###
  def getAttribute(attribute: String):BooleanQuery = {

    val bqAttribute: BooleanQuery = new BooleanQuery()
    if (attribute != "") {
      val attributeSplit = attribute.split(" ")
      for(att <- attributeSplit) {
          val attTerm: Term = new Term("attribute","\""+att+"\"")
          val attPq: TermQuery = new TermQuery(attTerm)
          bqAttribute.add(attPq, BooleanClause.Occur.MUST)
      }
      println(bqAttribute)
    }
    bqAttribute
  }

  def getEliminateid(eliminateid: String):BooleanQuery = {

    val bqEliminateid: BooleanQuery = new BooleanQuery()
    if (eliminateid != "") {
      val eliminateids = eliminateid.split(" ")
      for(eli <- eliminateids) {
        val eliminateidTerm: Term = new Term("eliminateid",eli)
        val eliminateidPq: PrefixQuery = new PrefixQuery(eliminateidTerm)
        bqEliminateid.add(eliminateidPq, BooleanClause.Occur.MUST)
      }
      println(bqEliminateid)
    }
    bqEliminateid
  }

  def getEcProduct001(ecProduct001: String):BooleanQuery = {

    val bqEcProduct001: BooleanQuery = new BooleanQuery()
    if(ecProduct001 != null) {
      val ecProduct001Term: Term = new Term("ecProduct001",ecProduct001)
      val ecProduct001Pq: PrefixQuery = new PrefixQuery(ecProduct001Term)
      bqEcProduct001.add(ecProduct001Pq, BooleanClause.Occur.MUST)
      println(bqEcProduct001)
    }
    bqEcProduct001
  }
  //排序
  def sorts(sort: String): Sort = {
    val lst = ListBuffer[SortField]()
    
    val sortField = sort match {
      case "price-" => new SortField("unitPrice", SortField.Type.DOUBLE, true)
      case "price+" => new SortField("unitPrice", SortField.Type.DOUBLE, false)
      case "releasedate-" => new SortField("createTime", SortField.Type.DOUBLE, true)
      case "releasedate+" => new SortField("createTime", SortField.Type.DOUBLE, false)
      case "reviews-" => new SortField("reviews", SortField.Type.INT, true)
      case "reviews+" => new SortField("reviews", SortField.Type.INT, false)
      case "diggs-" => new SortField("diggs", SortField.Type.INT, true)
      case "diggs+" => new SortField("diggs", SortField.Type.INT, false)
      case "videos-" => new SortField("videos", SortField.Type.INT, true)
      case "videos+" => new SortField("videos", SortField.Type.INT, false)
      case _ => null
    }
    var sot: Sort = new Sort(sortField)
    sot
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
