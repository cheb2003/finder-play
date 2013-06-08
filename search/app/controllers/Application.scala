package controllers

import org.apache.lucene.util.{BytesRef}
import play.api.mvc._
import play.api.libs.json.{Json}
import play.api.libs.json.Json._
import scala.collection.mutable.ListBuffer
import java.lang.Double
import java.lang.Long
import org.apache.lucene.search._
import my.finder.search.service.SearcherManager

import org.apache.lucene.index.Term

object Application extends Controller {

  def index = Action {
    Ok(views.html.index("Your new application is ready."))
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

      var currentPage = Integer.valueOf(request.getQueryString("page").getOrElse("1"))
      var size = Integer.valueOf(request.getQueryString("size").getOrElse("100"))
      if (currentPage < 1) {
        currentPage = 1
      }
      if (size < 1 || size > 1000) {
        size = 100;
      }

      productAliasName = productAliasName.trim
      val keywords = productAliasName.split(" ");

      val bq:BooleanQuery  = new BooleanQuery();
      val bqKeyEn:BooleanQuery  = new BooleanQuery();
      //search title
      for (k <- keywords) {
        val term:Term = new Term("pName", k);
        val pq:PrefixQuery = new PrefixQuery(term);
        bqKeyEn.add(pq, BooleanClause.Occur.MUST);
      }
      bq.add(bqKeyEn, BooleanClause.Occur.SHOULD);
      //search indexCode
      if(indexCode != ""){
        val indexCodeTerm:Term = new Term("indexCode", indexCode);
        val indexCodePQ:PrefixQuery = new PrefixQuery(indexCodeTerm);
        bq.add(indexCodePQ, BooleanClause.Occur.SHOULD)
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

      if(businessBrand != ""){
        val indexCodeTerm:Term = new Term("indexCode", indexCode);
        val indexCodePQ:PrefixQuery = new PrefixQuery(indexCodeTerm);
        bq.add(indexCodePQ, BooleanClause.Occur.SHOULD)
      }
      ranges(range.split(","),bq)
      val sot:Sort  = sorts(sort);
      val ids = ListBuffer[Long]()


      val searcher:IndexSearcher = SearcherManager.searcher

      val start = (currentPage - 1) * size + 1;
      //分页
      val tsdc:TopFieldCollector = TopFieldCollector.create(sot, start + size, false, false, false, false);

      searcher.search(bq, tsdc);


      //从0开始计算
      val topDocs:TopDocs = tsdc.topDocs(start - 1, size);
      val scoreDocs = topDocs.scoreDocs;
      val total = tsdc.getTotalHits()
      for (i <- 0 until scoreDocs.length) {
        val indexDoc = searcher.getIndexReader().document(scoreDocs(i).doc);
        ids += Long.valueOf(indexDoc.get("pId"))
      }


      val jsonObject = toJson(
        Map(
          "productIds" -> toJson(ids),
          "totalHits" -> toJson(total)
        )
      )
      Ok(Json.toJson(jsonObject).toString())
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
}