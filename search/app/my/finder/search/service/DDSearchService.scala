package my.finder.search.service

import my.finder.common.util.Util
import org.apache.lucene.search._
import org.apache.lucene.index.Term
import org.apache.commons.lang.StringUtils
import my.finder.common.model.{IdsPageResult, PageResult, ErrorResult}
import org.apache.lucene.search.BooleanClause.Occur
import scala.collection.mutable.ListBuffer
import my.finder.search.service
import java.util
import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.lucene.util.BytesRef
import java.lang.Double


/**
 * dd前台搜索接口服务
 */

object DDSearchService{
  /**
   * 店铺查询接口.
   * 必须传入shopid参数
   * @param queryParams
   */
  def shop(queryParams: Map[String, String]):PageResult = {
    val sort = Util.getParamString(queryParams, "sort", "")
    val size = Util.getSize(queryParams, 20)
    val page = Util.getPage(queryParams, 1)
    val countrycode = Util.getParamString(queryParams, "country", "")
    val indexCode = Util.getParamString(queryParams, "indexcode", "")
    val shopId = Util.getParamString(queryParams, "shopid", "")
    val keyword = Util.getParamString(queryParams, "keyword", "")
    val isFreeShipping = Util.getParamString(queryParams, "isfreeshipping", "")
  	def searchShop:IdsPageResult = {
      val bq = new BooleanQuery

      if(StringUtils.isNotBlank(isFreeShipping)){
        val tIsFreeShipping:Term = new Term("isFreeShipping",isFreeShipping)
        val tqIsFreeShipping = new TermQuery(tIsFreeShipping)
        bq.add(tqIsFreeShipping,Occur.MUST)
      }

      val tShopId:Term = new Term("shopIds",shopId)
      val tqShopId = new TermQuery(tShopId)
      bq.add(tqShopId,Occur.MUST)

      val bqKeyword = getKeyWordQuery(keyword)
      if (bqKeyword != null) {
        bq.add(bqKeyword,Occur.MUST)
      }

      if(StringUtils.isNotBlank(indexCode)){
      	val tIndexCode = new Term("shopCategorys",indexCode)
	      val tqIndexCode = new TermQuery(tIndexCode)
	      bq.add(tqIndexCode,Occur.MUST)
      }

      val sot: Sort = sorts(sort,"country" + countrycode.toUpperCase);
      val searcher: IndexSearcher = service.SearcherManager.ddSearcher
      val start = (page - 1) * size + 1;
      val tsdc: TopFieldCollector = TopFieldCollector.create(sot, start + size, false, false, false, false);

      searcher.search(bq, tsdc);


      val topDocs: TopDocs = tsdc.topDocs(start - 1, size);
      val ids = readIds(searcher,topDocs.scoreDocs)
      val total = tsdc.getTotalHits()

      new IdsPageResult(ids,page,size,total,bq.toString)
    }



    if(StringUtils.isNotBlank(shopId)){
      searchShop
    } else {
      new ErrorResult("require shopid")
    }

    
  }

  /**
   * 品牌搜索接口
   * 必须传入brandid参数
   * @param queryParams
   */
  def brand(queryParams: Map[String, String]):PageResult = {
    val sort = Util.getParamString(queryParams, "sort", "")
    val size = Util.getSize(queryParams, 20)
    val page = Util.getPage(queryParams, 1)
    val countrycode = Util.getParamString(queryParams, "country", "")
    val indexCode = Util.getParamString(queryParams, "indexcode", "")
    val brandId = Util.getParamString(queryParams, "brandid", "")
    val keyword = Util.getParamString(queryParams, "keyword", "")
    def searchBrand:IdsPageResult = {
      val bq = new BooleanQuery

      val tBrandId:Term = new Term("brandId",brandId)
      val tQBrandId = new TermQuery(tBrandId)
      bq.add(tQBrandId,Occur.MUST)

      val bqKeyword = getKeyWordQuery(keyword)
      if (bqKeyword != null) {
        bq.add(bqKeyword,Occur.MUST)
      }

      if(StringUtils.isNotBlank(indexCode)){
        val tIndexCode = new Term("indexCode",indexCode)
        val tqIndexCode = new TermQuery(tIndexCode)
        bq.add(tqIndexCode,Occur.MUST)
      }

      val sot: Sort = sorts(sort,"country" + countrycode.toUpperCase);
      val searcher: IndexSearcher = service.SearcherManager.ddSearcher
      val start = (page - 1) * size + 1;
      val tsdc: TopFieldCollector = TopFieldCollector.create(sot, start + size, false, false, false, false);

      searcher.search(bq, tsdc);


      val topDocs: TopDocs = tsdc.topDocs(start - 1, size);
      val ids = readIds(searcher,topDocs.scoreDocs)
      val total = tsdc.getTotalHits()

      new IdsPageResult(ids,page,size,total,bq.toString)
    }

    if(StringUtils.isNotBlank(brandId)){
      searchBrand
    } else {
      new ErrorResult("require brandId")
    }

  }

  /**
   * NewArrival搜索接口
   * @param queryParams
   */
  def newarrival(queryParams: Map[String, String]):PageResult = {
    val sort = Util.getParamString(queryParams, "sort", "")
    val size = Util.getSize(queryParams, 20)
    val page = Util.getPage(queryParams, 1)
    val countrycode = Util.getParamString(queryParams, "country", "")
    val indexCode = Util.getParamString(queryParams, "indexcode", "")
    val price = Util.getParamString(queryParams, "price", "")
    val keyword = Util.getParamString(queryParams, "keyword", "")

    val s = new SimpleDateFormat("yyyyMMddHHmm")
    val idsPageResult = {
      val bq = new BooleanQuery

      val now = Calendar.getInstance()
      val snow = s.format(now.getTime)

      val calbefore7 = now
      calbefore7.add(Calendar.DATE, -7)
      val scalbefore7 = s.format(calbefore7.getTime)
      val trq = new TermRangeQuery("createTime",new BytesRef(scalbefore7),new BytesRef(snow),true,true);
      bq.add(trq,Occur.MUST)

      val bqKeyword = getKeyWordQuery(keyword)
      if (bqKeyword != null) {
        bq.add(bqKeyword,Occur.MUST)
      }

      if(StringUtils.isNotBlank(price)){
        bq.add(ranges(price),Occur.SHOULD)
      }

      if(StringUtils.isNotBlank(indexCode)){
        val tIndexCode = new Term("indexCode",indexCode)
        val tqIndexCode = new TermQuery(tIndexCode)
        bq.add(tqIndexCode,Occur.MUST)
      }

      val sot: Sort = sorts(sort,"country" + countrycode.toUpperCase);
      val searcher: IndexSearcher = service.SearcherManager.ddSearcher
      val start = (page - 1) * size + 1;
      val tsdc: TopFieldCollector = TopFieldCollector.create(sot, start + size, false, false, false, false);

      searcher.search(bq, tsdc);

      val topDocs: TopDocs = tsdc.topDocs(start - 1, size);
      val ids = readIds(searcher,topDocs.scoreDocs)
      val total = tsdc.getTotalHits()

      new IdsPageResult(ids,page,size,total,bq.toString)
    }
    idsPageResult

  }

  /**
   * under999搜索接口
   * @param queryParams
   */
  def under999(queryParams: Map[String, String]):PageResult = {
    val sort = Util.getParamString(queryParams, "sort", "")
    val size = Util.getSize(queryParams, 20)
    val page = Util.getPage(queryParams, 1)
    val countrycode = Util.getParamString(queryParams, "country", "")
    val indexCode = Util.getParamString(queryParams, "indexcode", "")
    val keyword = Util.getParamString(queryParams, "keyword", "")
    val idsPageResult = {
      val bq = new BooleanQuery

      val nrq = NumericRangeQuery.newDoubleRange("price", 0, 9.99d, true, true);
      bq.add(nrq,Occur.MUST)

      val bqKeyword = getKeyWordQuery(keyword)
      if (bqKeyword != null) {
        bq.add(bqKeyword,Occur.MUST)
      }

      if(StringUtils.isNotBlank(indexCode)){
        val tIndexCode = new Term("indexCode",indexCode)
        val tqIndexCode = new TermQuery(tIndexCode)
        bq.add(tqIndexCode,Occur.MUST)
      }

      val sot: Sort = sorts(sort,"country" + countrycode.toUpperCase);
      val searcher: IndexSearcher = service.SearcherManager.ddSearcher
      val start = (page - 1) * size + 1;
      val tsdc: TopFieldCollector = TopFieldCollector.create(sot, start + size, false, false, false, false);

      searcher.search(bq, tsdc);


      val topDocs: TopDocs = tsdc.topDocs(start - 1, size);
      val ids = readIds(searcher,topDocs.scoreDocs)
      val total = tsdc.getTotalHits()

      new IdsPageResult(ids,page,size,total,bq.toString)
    }
    idsPageResult
  }

  /**
   * 清仓搜索接口
   * @param queryParams
   */
  def clearance(queryParams: Map[String, String]):PageResult = {
    val sort = Util.getParamString(queryParams, "sort", "")
    val size = Util.getSize(queryParams, 20)
    val page = Util.getPage(queryParams, 1)
    val countrycode = Util.getParamString(queryParams, "country", "")
    val indexCode = Util.getParamString(queryParams, "indexcode", "")
    val price = Util.getParamString(queryParams, "price", "")
    val keyword = Util.getParamString(queryParams, "keyword", "")
    def searchClearance:IdsPageResult = {
      val bq = new BooleanQuery

      val tIsClearance:Term = new Term("isClearance","1")
      val tQisClearance = new TermQuery(tIsClearance)
      bq.add(tQisClearance,Occur.MUST)

      val bqKeyword = getKeyWordQuery(keyword)
      if (bqKeyword != null) {
        bq.add(bqKeyword,Occur.MUST)
      }

      if(StringUtils.isNotBlank(price)){
        bq.add(ranges(price),Occur.SHOULD)
      }

      if(StringUtils.isNotBlank(indexCode)){
        val tIndexCode = new Term("indexCode",indexCode)
        val tqIndexCode = new TermQuery(tIndexCode)
        bq.add(tqIndexCode,Occur.MUST)
      }

      val sot: Sort = sorts(sort,"country" + countrycode.toUpperCase);
      val searcher: IndexSearcher = service.SearcherManager.ddSearcher
      val start = (page - 1) * size + 1;
      val tsdc: TopFieldCollector = TopFieldCollector.create(sot, start + size, false, false, false, false);
      searcher.search(bq, tsdc);

      val topDocs: TopDocs = tsdc.topDocs(start - 1, size);
      val ids = readIds(searcher,topDocs.scoreDocs)
      val total = tsdc.getTotalHits()

      new IdsPageResult(ids,page,size,total,bq.toString)
    }
    searchClearance

  }

  private def readIds(searcher:IndexSearcher,scoreDocs:Array[ScoreDoc]):ListBuffer[Int] = {
    val ids = ListBuffer[Int]()
    val set = new util.HashSet[String]()
    set.add("id")
    for (i <- 0 until scoreDocs.length) {
      val indexDoc = searcher.getIndexReader().document(scoreDocs(i).doc,set)
      ids += Integer.valueOf(indexDoc.get("id"))
    }
    ids
  }

  /**
   *
   * @param sort
   * @return
   */
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
      case _ => {
        //默认排序,先排相关度,在排国家分数
      }
    }
    var sot: Sort = new Sort(new SortField("unitPrice", SortField.Type.DOUBLE, true))
    sot

  }

  /**
   * 传入国家代码排序
   * @param sort
   * @return
   */
  def sorts(sort: String,country: String): Sort = {
    val sot = sort match {
      case "price-" => new Sort(new SortField("price", SortField.Type.DOUBLE, true))
      case "price+" => new Sort(new SortField("price", SortField.Type.DOUBLE, false))
      case "releasedate-" => new Sort(new SortField("createTime", SortField.Type.DOUBLE, true))
      case "releasedate+" => new Sort(new SortField("createTime", SortField.Type.DOUBLE, false))
      case "reviews-" => new Sort(new SortField("reviews", SortField.Type.INT, true))
      case "reviews+" => new Sort(new SortField("reviews", SortField.Type.INT, false))
      case "diggs-" => new Sort(new SortField("diggs", SortField.Type.INT, true))
      case "diggs+" => new Sort(new SortField("diggs", SortField.Type.INT, false))
      case "videos-" => new Sort(new SortField("videos", SortField.Type.INT, true))
      case "videos+" => new Sort(new SortField("videos", SortField.Type.INT, false))
      case _ => {
        if(country != "")
        //默认排序,先排相关度,在排国家分数
          new Sort(SortField.FIELD_SCORE,new SortField(country, SortField.Type.STRING, true));
        else
          new Sort(SortField.FIELD_SCORE);
      }
    }
    sot
    //var sot: Sort = new Sort(new SortField("unitPrice", SortField.Type.DOUBLE, true))
  }

  /**
   * 获取关键字查询条件.
   * @param pKeyword 关键字
   * @return
   */
  def getKeyWordQuery(pKeyword: String): BooleanQuery = {
    if(StringUtils.isNotBlank(pKeyword)){
      val keyword = pKeyword.trim.toLowerCase
      val bq: BooleanQuery = new BooleanQuery()
      val keywordSplit = keyword.split(" ")

      val bqKeyEn: BooleanQuery = new BooleanQuery()
      val bqKeywordBoundCategory: BooleanQuery = new BooleanQuery()
      val bqSeokeyword: BooleanQuery = new BooleanQuery()
      val bqType: BooleanQuery = new BooleanQuery()
      val bqShortDes: BooleanQuery = new BooleanQuery()
      val bqBrand: BooleanQuery = new BooleanQuery()

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
        bqSeokeyword.add(seokeywordPq, BooleanClause.Occur.MUST)

        //品类 3
        val typeTerm: Term = new Term("indexCode", k)
        val typePq: TermQuery = new TermQuery(typeTerm)
        bqType.add(typePq, BooleanClause.Occur.MUST)

        //短描述2
        /*val shortdesTerm: Term = new Term("shortdes", k)
        val shortdesPq: TermQuery = new TermQuery(shortdesTerm)
        bqShortDes.add(shortdesPq, BooleanClause.Occur.MUST)*/

        //品牌 18
        val brandTerm: Term = new Term("brandName", k)
        val brandPq: TermQuery = new TermQuery(brandTerm)
        bqBrand.add(brandPq, BooleanClause.Occur.MUST)
      }
      bqKeyEn.setBoost(40f)
      bqKeywordBoundCategory.setBoost(90f)
      bqSeokeyword.setBoost(7f)
      bqType.setBoost(3f)
      bqShortDes.setBoost(2f)
      bqBrand.setBoost(18f)

      bq.add(bqKeyEn, BooleanClause.Occur.SHOULD)
      bq.add(bqKeywordBoundCategory, BooleanClause.Occur.SHOULD)
      bq.add(bqSeokeyword, BooleanClause.Occur.SHOULD)
      bq.add(bqType, BooleanClause.Occur.SHOULD)
      bq.add(bqShortDes, BooleanClause.Occur.SHOULD)
      bq.add(bqBrand, BooleanClause.Occur.SHOULD)
      bq
    } else {
      null
    }
  }


  def ranges(range: String):BooleanQuery = {
    //范围查询
    val bqRanges: BooleanQuery = new BooleanQuery()
    val parts = range.split(":");
    if (parts.length == 3) {
      if (parts(0).equals("price")) {
        val nrq = NumericRangeQuery.newDoubleRange("price", Double.valueOf(parts(1)), Double.valueOf(parts(2)), true, true);
        bqRanges.add(nrq, BooleanClause.Occur.MUST);
      }
      if (parts(0).equals("createtime")) {
        val query: TermRangeQuery = new TermRangeQuery("createTime", new BytesRef(parts(1)), new BytesRef(parts(2)), true, true);
        bqRanges.add(query, BooleanClause.Occur.MUST);
      }
      bqRanges
    } else null
  }
}

