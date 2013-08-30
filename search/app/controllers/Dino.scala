package controllers

import org.apache.lucene.util.{Version, BytesRef}
import play.api.mvc._
import play.api.libs.json.{JsArray, Writes, JsValue, Json}
import play.api.libs.json.Json._
import scala.collection.mutable.{HashMap, Queue, ListBuffer}
import org.apache.lucene.search._
import my.finder.search.service.{DDSearchService, SearcherManager}
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
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.analysis.core.WhitespaceAnalyzer
import java.util
import scala.collection.mutable
import scala.util.control.Breaks._

case class AttrGroup(name:String,values:mutable.HashSet[String])
case class CategoryGroup(indexCode:String,var count:Int,parent:CategoryGroup,children:ListBuffer[CategoryGroup])
class ProductSearchPageResult(data:ListBuffer[Int],val attrGroups:ListBuffer[AttrGroup],val categoryGroups:ListBuffer[CategoryGroup],page:Int,size:Int,total:Int,query:String) extends IdsPageResult(data,page,size,total,query)

object Dino extends Controller {

  implicit val attrGroupWrites = new Writes[AttrGroup] {
    def writes(c: AttrGroup): JsValue = {
      Json.obj(
        "name" -> c.name,
        "values" -> c.values
      )
    }
  }
  implicit val categoryGroupWrites = new Writes[CategoryGroup] {
    def writes(c: CategoryGroup): JsValue = {
      Json.obj(
        "indexCode" -> c.indexCode,
        "count" -> c.count,
        "children" -> buildChildren(c.children)
      )
    }
  }
  private def buildChildren(children:ListBuffer[CategoryGroup]):ListBuffer[JsValue] = {
    val lst: ListBuffer[JsValue] = ListBuffer[JsValue]()
    for(c <- children){
      lst += toJson(
        Map(
          "indexCode" -> toJson(c.indexCode),
          "count" -> toJson(c.count),
          "children" -> toJson(buildChildren(c.children))
        )
      )
    }
    lst
  }
  /*implicit val productSearchPageResultWrites = new Writes[ProductSearchPageResult] {
    def writes(c: ProductSearchPageResult): JsValue = {
      Json.obj(
        "page" -> c.page,
        "size" -> c.size,
        "total" -> c.total,
        "ids" -> c.data,
        "query" -> c.query,
        "attributes" -> JsArray(c.attrGroups.map(toJson(_))),
        "categories" -> JsArray(c.categoryGroups.map(toJson(_)))
      )
    }
  }*/

  implicit val idsPageResultWrites = new Writes[IdsPageResult] {
    def writes(c: IdsPageResult): JsValue = {
      val v = if(c.isInstanceOf[ProductSearchPageResult]){
        val t = c.asInstanceOf[ProductSearchPageResult]
        val jsAttrGroup = if(t.attrGroups != null){
          JsArray(t.attrGroups.map(toJson(_)))
        } else {
          JsArray()
        }
        val jsCategoryGroup = if(t.categoryGroups != null){
          JsArray(t.categoryGroups.map(toJson(_)))
        } else {
          JsArray()
        }
        Json.obj(
          "page" -> t.page,
          "size" -> t.size,
          "total" -> t.total,
          "ids" -> t.data,
          "query" -> t.query,
          "attributes" -> jsAttrGroup,
          "categories" -> jsCategoryGroup
        )
      } else {
        Json.obj(
          "page" -> c.page,
          "size" -> c.size,
          "total" -> c.total,
          "ids" -> c.data,
          "query" -> c.query
        )
      }
      v
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

  def brand = Action { implicit request =>
    val form = Form(
      tuple(
        "sort" -> text,
        "size" -> number,
        "page" -> number,
        "country" -> text,
        "indexcode" -> text,
        "brandId" -> text,
        "keyword" -> text
      )
    )
    val queryParams = form.bindFromRequest.data
    val result = DDSearchService.brand(queryParams)
    val json = if (result.isInstanceOf[IdsPageResult]) {
      toJson(result.asInstanceOf[IdsPageResult])
    } else if (result.isInstanceOf[ErrorResult]){
      Json.parse("{}")
    } else {
      Json.parse("{}")
    }

    Ok(json)
  }

  def newarrival = Action { implicit request =>
    val form = Form(
      tuple(
        "sort" -> text,
        "size" -> number,
        "page" -> number,
        "country" -> text,
        "indexcode" -> text,
        "price" -> text,
        "keyword" -> text
      )
    )
    val queryParams = form.bindFromRequest.data
    val result = DDSearchService.newarrival(queryParams)
    val json = if (result.isInstanceOf[IdsPageResult]) {
      toJson(result.asInstanceOf[IdsPageResult])
    } else if (result.isInstanceOf[ErrorResult]){
      Json.parse("{}")
    } else {
      Json.parse("{}")
    }

    Ok(json)
  }

  def under999 = Action { implicit request =>
    val form = Form(
      tuple(
        "sort" -> text,
        "size" -> number,
        "page" -> number,
        "country" -> text,
        "indexcode" -> text,
        "keyword" -> text
      )
    )
    val queryParams = form.bindFromRequest.data
    val result = DDSearchService.under999(queryParams)
    val json = if (result.isInstanceOf[IdsPageResult]) {
      toJson(result.asInstanceOf[IdsPageResult])
    } else if (result.isInstanceOf[ErrorResult]){
      Json.parse("{}")
    } else {
      Json.parse("{}")
    }

    Ok(json)
  }

  def clearance = Action { implicit request =>
    val form = Form(
      tuple(
        "sort" -> text,
        "size" -> number,
        "page" -> number,
        "country" -> text,
        "indexcode" -> text,
        "price" -> text,
        "keyword" -> text
      )
    )
    val queryParams = form.bindFromRequest.data
    val result = DDSearchService.clearance(queryParams)
    val json = if (result.isInstanceOf[IdsPageResult]) {
      toJson(result.asInstanceOf[IdsPageResult])
    } else if (result.isInstanceOf[ErrorResult]){
      Json.parse("{}")
    } else {
      Json.parse("{}")
    }

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
        "ranges" -> text,
        "country" -> text,
        "indexcode" -> text,
        "tag"-> text,
        "excludeids"-> text,
        "excludeAreas"-> text
      )
    )
    val queryParams = form.bindFromRequest.data
    searchProduct(queryParams,"json")
    
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
    val sort = Util.getParamString(queryParams, "sort", "")
    val tag = Util.getParamString(queryParams, "tag", "")
    val indexcode = Util.getParamString(queryParams, "indexcode", "")
    val attribute = Util.getParamString(queryParams, "attribute", "")
    val country = Util.getParamString(queryParams, "country", "")
    val excludeIds = Util.getParamString(queryParams, "excludeids", "")
    val excludeAreas = Util.getParamString(queryParams, "excludeAreas", "")
    val ranges = Util.getParamString(queryParams, "ranges", "")

    val AXTPage = Util.getPage(queryParams, 1)
    val AXTSize = Util.getSize(queryParams, 15)
    val ddSearcher: IndexSearcher = SearcherManager.ddSearcher
    val bqAll: BooleanQuery = new BooleanQuery()
    val qKeyword = DDSearchService.getKeyWordQuery(keyword)
    if(qKeyword != null){
      bqAll.add(qKeyword, Occur.MUST)  
    }
    
    val qTag = getTag(tag)
    if(qTag != null) {
      bqAll.add(qTag, Occur.MUST)
    }
    
    val qIndexCode = getIndexCode(indexcode)
    if(qIndexCode != null){
      bqAll.add(qIndexCode, Occur.MUST)
    }
    
    val qAttr = getAttributeQuery(attribute)
    if(qAttr != null) {
      bqAll.add(qAttr, Occur.MUST)  
    }
    
    val qExcludeIds = getExcludeIdQuery(excludeIds)
    if(qExcludeIds != null){
      bqAll.add(qExcludeIds, Occur.MUST_NOT)  
    }
    
    val qExcludeAreas = getExcludeAreaQuery(excludeAreas)
    if(qExcludeAreas != null){
      bqAll.add(qExcludeAreas, Occur.MUST)  
    }
    
    val qRanges = getRanges(ranges)
    if(qRanges != null){
      bqAll.add(qRanges, Occur.MUST)  
    }

    //search attributes query
    val bqAttr = bqAll.clone()

    val tHasAttr = new Term("attribute","hasattributes")
    val tqHasAttr = new TermQuery(tHasAttr)
    bqAttr.add(tqHasAttr, Occur.MUST)

    //search attributes 
    val attrData = getAttributes(bqAttr)
    //AXT
    val bqA = bqAll.clone()
    val tA = new Term("skuOrder","0")
    val tqA = new TermQuery(tA)
    bqA.add(tqA, Occur.MUST)

    val bqX = bqAll.clone()
    val tX = new Term("skuOrder","1")
    val tqX = new TermQuery(tX)
    bqX.add(tqX, Occur.MUST)

    val bqT = bqAll.clone()
    val tT = new Term("skuOrder","2")
    val tqT = new TermQuery(tT)
    bqT.add(tqT, Occur.MUST)

    val AXTData = getAXT(ddSearcher,bqA,bqX,bqT,AXTPage,AXTSize)

    //查询产品
    val sot: Sort = DDSearchService.sorts(sort);
    val ids = ListBuffer[Int]()

    val start = (page - 1) * size + 1

    val tsdc: TopFieldCollector = TopFieldCollector.create(sot, start + size, false, false, false, false)
    ddSearcher.search(bqAll, tsdc);

    val topDocs: TopDocs = tsdc.topDocs(start - 1, size)
    val scoreDocs = topDocs.scoreDocs;
    val total = tsdc.getTotalHits
    scoreDocs.map { s =>
      ids += java.lang.Integer.valueOf(ddSearcher.getIndexReader.document(s.doc).get("id"))
    }

    //返回品类
    val groupingSearch = new GroupingSearch("indexCode");
    groupingSearch.setAllGroups(true)
    val lst: ListBuffer[JsValue] = ListBuffer[JsValue]()
    val topGroups: TopGroups[BytesRef] = groupingSearch.search(ddSearcher, bqAll, 0, 10000)
    val categoryGroups = ListBuffer[CategoryGroup]()
    for(topGroup <- topGroups.groups){
      val indexCode = topGroup.groupValue.utf8ToString()
      if(indexCode.length > 8){
        buildCategoryGroupTree(categoryGroups,indexCode,topGroup.totalHits,1,null)
      }
    }
    val result = new ProductSearchPageResult(ids,attrData,categoryGroups,page,size,total,bqAll.toString)

    Ok(toJson(result))
  }

  private def buildCategoryGroupTree(cgs:ListBuffer[CategoryGroup], indexCode:String,count:Int,level:Int,parent:CategoryGroup) {
    val codeLength = (level + 1) * 4
    if(indexCode.length >= codeLength){
      val tCode = indexCode.substring(0,codeLength)
      for(icg <- cgs){
        if(icg.indexCode == tCode && indexCode.length > codeLength){
          buildCategoryGroupTree(icg.children,indexCode,count,level + 1,icg)
        }
      }
      val cg = if(tCode == indexCode) CategoryGroup(tCode,count,parent,ListBuffer[CategoryGroup]())
      else CategoryGroup(tCode,0,parent,ListBuffer[CategoryGroup]())
      if(parent != null){
        parent.children += cg
      }
      if(indexCode.length > codeLength){
        buildCategoryGroupTree(cg.children,indexCode,count,level + 1,cg)
      }
      if(tCode == indexCode){
        addParentCount(parent,count)
      }

      if(level == 1){
        var b = false
        for(icg <- cgs if (icg.indexCode == cg.indexCode)){
          b = true
        }
        if(!b){
          cgs += cg
        }
      }
    }
  }

  private def addParentCount(parent:CategoryGroup,count:Int) {
    if(parent != null){
      parent.count += count
      addParentCount(parent.parent,count)
    }
  }
  def getRanges(ranges:String):Query = {
    //范围查询
    val q = if(StringUtils.isNotBlank(ranges)){
      val bqRanges = new BooleanQuery
      val rangesSplit = ranges.split(",")
      for (range <- rangesSplit) {  
        val parts = range.split(":")
        if (parts.length == 3) {
          if (parts(0).equals("price")) {
            val nrq = NumericRangeQuery.newDoubleRange("price", java.lang.Double.valueOf(parts(1)), java.lang.Double.valueOf(parts(2)), true, true);
            bqRanges.add(nrq, BooleanClause.Occur.MUST);
          }
        }
      }
      bqRanges
    } else {
      null
    }
    q
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
  def getTag(tag: String):Query = {

    
    val q = if(StringUtils.isNotBlank(tag)){
      val bqTag: BooleanQuery = new BooleanQuery()
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
        bqTag
      }else if("event".equals(tag)) {
        val bqTagSub: BooleanQuery = new BooleanQuery()
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
        bqTag
      }else if("clear".equals(tag)) {
        val isClearanceTinyintTerm: Term = new Term("isClearanceTinyint","1")
        val isClearanceTinyintPq: TermQuery = new TermQuery(isClearanceTinyintTerm)
        bqTag.add(isClearanceTinyintPq, BooleanClause.Occur.MUST)
        bqTag
      } else null
    } else null
    q
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
    val q:Query = if(StringUtils.isNotBlank(attribute)) {
      val bqAttr: BooleanQuery = new BooleanQuery()
      val attributeSplit = attribute.split(",")
      for(att <- attributeSplit) {
          val tAttr: Term = new Term("attribute","\""+att+"\"")
          val tqAttr: TermQuery = new TermQuery(tAttr)
          bqAttr.add(tqAttr, BooleanClause.Occur.MUST)
      }
      bqAttr
    } else {
      null
    }
    q
  }

  def getExcludeIdQuery(excludeId: String):Query = {
    val q = if (StringUtils.isNotBlank(excludeId)) {
      val bqExcludeId: BooleanQuery = new BooleanQuery()
      val excludeIdSplit = excludeId.split(",")
      for(eli <- excludeIdSplit) {
        val tExcludeId: Term = new Term("id",eli)
        val tqExcludeId: TermQuery = new TermQuery(tExcludeId)
        bqExcludeId.add(tqExcludeId, Occur.MUST)
      }
      bqExcludeId
    } else {
      null
    }
    q
  }

  def getExcludeAreaQuery(excludeIds: String):BooleanQuery = {
    val q = if(StringUtils.isNotBlank(excludeIds)) {
      val bqExcludeIds: BooleanQuery = new BooleanQuery()
      val tExcludeId: Term = new Term("id",excludeIds)
      val tqExcludeId: TermQuery = new TermQuery(tExcludeId)
      bqExcludeIds.add(tqExcludeId, BooleanClause.Occur.MUST)
      bqExcludeIds  
    } else {
      null
    }
    q
  }
  

  //返回属性
  private def getAttributes(bqAttr:BooleanQuery):ListBuffer[AttrGroup] = {
    val ddSearcher =  SearcherManager.ddSearcher
    
    val ddTopDocs = ddSearcher.search(bqAttr,20000)
    val ddScoreDocs = ddTopDocs.scoreDocs
    val result = if(ddScoreDocs.length > 0){
      val sbDocIds = new StringBuffer
      val parser = new QueryParser(Version.LUCENE_43,"docIds",new WhitespaceAnalyzer(Version.LUCENE_43))
      BooleanQuery.setMaxClauseCount(Integer.MAX_VALUE)
      ddScoreDocs.map{ s =>
        sbDocIds.append(s.doc).append(" ")
      }
      val q = parser.parse(sbDocIds.toString)
      val attrSearcher =  SearcherManager.attrSearcher
      val attrTopDocs = attrSearcher.search(q,20000)
      val attrScoreDocs = attrTopDocs.scoreDocs
      val map = if(attrScoreDocs.length > 0){
        val data = new HashMap[String,AttrGroup]
        val set = new util.HashSet[String]()
        set.add("name")
        set.add("value")
        attrScoreDocs.map{ s =>
          val doc = attrSearcher.getIndexReader.document(s.doc,set)
          val name = doc.get("name")
          if (data.getOrElse(name,null) == null) {
            data += (name -> AttrGroup(name,mutable.HashSet[String]()))
          } 
          data(name).values += doc.get("value")
        }
        data
      } else null
      map
    } else null
    
    val lst = if(result != null) {
      val ite = result.iterator
      val l = ListBuffer[AttrGroup]()
      ite.foreach{ x=>
        l += x._2
      }
      l
    } else null
    lst 
  }

  private def getAXT(searcher: IndexSearcher,bqA: BooleanQuery,bqX: BooleanQuery,bqT: BooleanQuery,pageNow: Int,pageSize: Int):ListBuffer[ListBuffer[Int]] = {

    val ids = new ListBuffer[ListBuffer[Int]]
    val a = searcher.search(bqA, Integer.MAX_VALUE)
    val aTotal = a.totalHits
    val b = searcher.search(bqX, Integer.MAX_VALUE)
    val xTotal = b.totalHits
    val c = searcher.search(bqT, Integer.MAX_VALUE)
    val tTotal = c.totalHits
    if(aTotal == 0 && xTotal == 0 && tTotal == 0) {
      ids
    }
    //各产品总数计数
    var aCount = 0
    var xCount = 0
    var tCount = 0
    //各产品每页显示的个数计数
    var aPageRatioCount = 0
    var xPageRatioCount = 0
    var tPageRatioCount = 0
    //每页占比
    if(pageSize == 30) {
      aPageRatioCount = 15
      xPageRatioCount = 9
      tPageRatioCount = 6
    }else if(pageSize == 60) {
      aPageRatioCount = 30
      xPageRatioCount = 18
      tPageRatioCount = 12
    }else {
      aPageRatioCount = 7
      xPageRatioCount = 5
      tPageRatioCount = 3
    }
    var perPageCount = 0 //每页数计数
    var pageCount = 0 //页码数计数
    var exitCount = 0L //循环次数
    val exit = 3000000L //最多循环次数
    //各产品每页实际条数计数
    var aCurPageCount = 0
    var xCurPageCount = 0
    var tCurPageCount = 0
    while(pageCount < pageNow) {
      pageCount += 1

      aCurPageCount = 0
      xCurPageCount = 0
      tCurPageCount = 0
      perPageCount = 0
      //防止死循环
      breakable {

      while(true) {
        exitCount += 1
        if(exitCount >= exit) {
          ids
        }

        var i = 0
        while(i < aPageRatioCount && perPageCount < pageSize && aCount < aTotal ) {
          aCurPageCount += 1
          aCount += 1
          perPageCount += 1
          i += 1
        }
        i = 0
        while(i < xPageRatioCount && perPageCount < pageSize && xCount < xTotal) {
          xCurPageCount += 1
          xCount += 1
          perPageCount += 1
          i += 1
        }
        i = 0
        while(i < tPageRatioCount && perPageCount < pageSize && tCount < tTotal) {
          tCurPageCount += 1
          tCount += 1
          perPageCount += 1
          i += 1
        }

        if(perPageCount >= pageSize || (aCount >= aTotal && xCount >= xTotal && tCount >= tTotal)) {
          break
        }

      }
      }
    }
    val indexReader = searcher.getIndexReader()
    if(aCurPageCount != 0) {
      val tsdc: TopFieldCollector = TopFieldCollector.create(null, aTotal, false, false, false, false);
      searcher.search(bqA, tsdc);
      val topDocs: TopDocs = tsdc.topDocs(aCount - aCurPageCount, aCurPageCount);
      val scoreDocs = topDocs.scoreDocs
      var aIds = new ListBuffer[Int]
      for (i <- 0 until scoreDocs.length) {
        val indexDoc = indexReader.document(scoreDocs(i).doc)
        aIds += Integer.valueOf(indexDoc.get("id"))
      }
      ids += aIds
    }
    if(xCurPageCount != 0) {
      val tsdc: TopFieldCollector = TopFieldCollector.create(null, aTotal, false, false, false, false);
      searcher.search(bqA, tsdc);
      val topDocs: TopDocs = tsdc.topDocs(aCount - aCurPageCount, aCurPageCount);
      val scoreDocs = topDocs.scoreDocs
      val xIds = new ListBuffer[Int]
      for (i <- 0 until scoreDocs.length) {
        val indexDoc = indexReader.document(scoreDocs(i).doc)
        xIds += Integer.valueOf(indexDoc.get("id"))
      }
      ids += xIds
    }
    if(tCurPageCount != 0) {
      val tsdc: TopFieldCollector = TopFieldCollector.create(null, aTotal, false, false, false, false);
      searcher.search(bqA, tsdc);
      val topDocs: TopDocs = tsdc.topDocs(aCount - aCurPageCount, aCurPageCount);
      val scoreDocs = topDocs.scoreDocs
      val tIds = new ListBuffer[Int]
      for (i <- 0 until scoreDocs.length) {
        val indexDoc = indexReader.document(scoreDocs(i).doc)
        tIds += Integer.valueOf(indexDoc.get("id"))
      }
      ids += tIds
    }
    indexReader.close()
    ids
  }

}
