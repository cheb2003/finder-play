package my.finder.index.actor

import akka.actor.{ActorLogging, Actor}
import my.finder.common.util.Util
import org.apache.lucene.document._
import my.finder.common.message.{CompleteSubTask, IndexTaskMessageDD}
import my.finder.index.service.{DBService, IndexWriteManager}
import java.sql.{ResultSet, Statement, Connection}
import org.apache.lucene.index.IndexWriter
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap
import scala.util.control.Breaks._
import java.util.Date
import scala.Predef.String
import org.apache.commons.lang.StringUtils
import scala.collection.mutable

class Product(val id:String,
  val name:String,
  var sku:String,
  var indexCode:String,
  var isOneSale:String,
  var isAliExpress:String,
  var businessName:String,
  var createTime:String,
  var typeId:String,
  var isQuality:String,
  var ventureStatus:String,
  var ventureLevelNew:String,
  var isTaobao:String,
  var brandId:String,
  var brandName:String,
  var attribute:String,
  var isEvent:String,
  var searchKeyword:String,
  var areaScore:String,
  var isDailyDeal:String,
  var isLifeStyle:String,
  var wwwScore:Float,
  var shopIds:String,
  var shopCategorys:String,
  var fitType:String,
  var indexCodeOfTypeShow:String,
  var excludeAreas:String,
  val price:Double,
  var reviews:Int,
  val isClearance:String)

class IndexUnitActorDD extends Actor with ActorLogging {

  private val pIdField = new StringField("id", "", Field.Store.YES)
  private val pNameField = new TextField("name", "", Field.Store.YES)
  private val pSkuField = new StringField("sku", "", Field.Store.YES)
  private val pIndexCodeField = new StringField("indexCode", "", Field.Store.YES)
  private val pIsOneSaleField = new StringField("isOneSale", "", Field.Store.YES)
  private val pIsAliExpressField = new StringField("isAliExpress", "", Field.Store.YES)
  private val pBusinessNameField = new TextField("businessName", "", Field.Store.YES)
  private val pCreateTimeField = new StringField("createTime", "", Field.Store.YES)
  //osell
  private val pProductTypeIdField = new StringField("typeId", "", Field.Store.YES)
  private val pIsQualityProductField = new StringField("isQuality", "", Field.Store.YES)
  private val pVentureStatusField = new StringField("ventureStatus", "", Field.Store.YES)
  private val pVentureLevelNewField = new StringField("ventureLevelNew", "", Field.Store.YES)
  private val pIsTaobaoField = new StringField("isTaobao", "", Field.Store.YES)
  private val pProductBrandIdField = new StringField("brandId", "", Field.Store.YES)
  private val pProductBrandNameField = new TextField("brandName", "", Field.Store.YES)
  //private val pProductTypeNameField = new TextField("typeName", "", Field.Store.YES)
  private val skuOrderField = new StringField("skuOrder", "", Field.Store.YES)

  private val pAttributeValueField =  new TextField("attribute", "", Field.Store.YES)
  private val pIsEventField =  new StringField("isEvent", "", Field.Store.YES)
  private val pSearchKeywordField =  new TextField("searchKeyword", "", Field.Store.YES)
  //www score
  private val wwwScoreField = new DoubleField("wwwScore", 0d, Field.Store.YES)

  private val priceField = new DoubleField("price", 0d, Field.Store.YES)

  private val pIsDailyDealField =  new StringField("isDailyDeal", "", Field.Store.YES)
  private val pIsLifeStyleField =  new StringField("isLifeStyle", "", Field.Store.YES)
  private val shopIdsField =  new TextField("shopIds", "", Field.Store.YES)
  private val shopCategorysField =  new TextField("shopCategorys", "", Field.Store.YES)
  private val fitTypeField = new TextField("fitType","",Field.Store.YES)
  private val indexCodeTypeShowField =  new TextField("indexCodeTypeShow", "", Field.Store.YES)
  //private val showPositionTypeShowField =  new StringField("showPositionTypeShow", "", Field.Store.YES)
  private val excludeAreasField = new TextField("excludeAreas","",Field.Store.YES)
  private val reviewsField = new IntField("reviews",0,Field.Store.YES)
  private val isClearanceField = new StringField("isClearance","",Field.Store.YES)

  private var doc: Document = null

  override def preStart() {
  }

  def receive = {
    
    case msg: IndexTaskMessageDD => {
      var conn: Connection = null
      var stmt: Statement = null
      var rs: ResultSet = null

      val buffer2:StringBuffer = new StringBuffer()
      val buffer3:StringBuffer = new StringBuffer()

      val lst = new ListBuffer[Product]

      try {

        val time1 = System.currentTimeMillis()
        var bSql:Long = 0
        var eSql:Long = 0
        val writer = IndexWriteManager.getIndexWriter(msg.name, msg.date)
        var successCount: Int = 0
        var failCount: Int = 0
        var skipCount: Int = 0
        val ids = msg.ids.mkString(",")

        conn = DBService.dataSource.getConnection()
        stmt = conn.createStatement()

        //读取ec_product
        var sql = s"""select ProductID_int,productaliasname_nvarchar,ProductKeyID_nvarchar,
                IndexCode_nvarchar,IsOneSale_tinyint,IsAliExpress_tinyint,BusinessName_nvarchar,CreateTime_datetime,
                ProductTypeID_int,IsQualityProduct_tinyint,VentureStatus_tinyint,VentureLevelNew_tinyint,
                IsTaoBao_tinyint,ProductBrandID_int,productbrand_nvarchar,BusinessBrand_nvarchar,
                ProductPrice_money,QDWProductStatus_int,isClearance_tinyint from ec_product with(nolock) where ProductID_int in ($ids)"""
        stmt.setFetchSize(msg.batchSize)
        bSql = System.currentTimeMillis()
        rs = stmt.executeQuery(sql)

        var product:Product  = null
        while (rs.next()) {
          product = new Product(
            rs.getString("ProductID_int"),rs.getString("productaliasname_nvarchar"),rs.getString("ProductKeyID_nvarchar"),rs.getString("IndexCode_nvarchar"),rs.getString("IsOneSale_tinyint"),rs.getString("IsAliExpress_tinyint"),
            rs.getString("BusinessName_nvarchar"),rs.getString("CreateTime_datetime"),rs.getString("ProductTypeID_int"),rs.getString("IsQualityProduct_tinyint"),rs.getString("VentureStatus_tinyint"),rs.getString("VentureLevelNew_tinyint"),
            rs.getString("IsTaoBao_tinyint"),rs.getString("ProductBrandID_int"),rs.getString("productbrand_nvarchar"),"","","",
            "","","",Float.NaN,"","","","","",rs.getDouble("ProductPrice_money"),0,rs.getString("isClearance_tinyint")
          )
          lst += product
          //buffer1.append(rs.getInt("ProductID_int")).append(',')
          buffer2.append("'").append(rs.getString("ProductKeyID_nvarchar")).append("'").append(',')
          buffer3.append("'").append(rs.getString("ProductTypeID_int")).append("'").append(',')
        }
        eSql = System.currentTimeMillis()
        if(eSql - bSql > 1000){
          log.info("sql:{};time:{}",sql,eSql - bSql)
        }

        val skus = buffer2.substring(0,buffer2.length() - 1)
        val stypeid = buffer3.substring(0,buffer3.length() - 1)

        //读取EC_ProductExtendItem
        //sql = String.format("select productid_int,itemvalueeng_nvarchar,itemnameeng_nvarchar from EC_ProductExtendItem with(nolock) where ProductID_int in(%s) and WebSiteID_smallint = 61 and AttributeInputType_int = 1 ",ids)
        sql = s"""select ProductId_int as id,AttributeValue_nvarchar as attr from EC_ExtendsForProductId with(nolock)
                where ProductId_int in ($ids)"""
        bSql = System.currentTimeMillis()
        rs = stmt.executeQuery(sql)

        //属性
        while(rs.next()){
          breakable {
            for (p <- lst) {
              if(p.id == rs.getString("id")){
                p.attribute = rs.getString("attr")
                break
              }
            }
          }
        }
        val map = new HashMap[String,String]()
        /*while (rs.next()) {
          var s = map.getOrElse(rs.getString("productid_int"),"")
          val v = rs.getString("itemvalueeng_nvarchar")
          val vv = v.split(",")
          val sb = new StringBuffer()
          var i = 0
          while(i < vv.length){
            sb.append("###").append(rs.getString("itemnameeng_nvarchar")).append("###").append(vv(i)).append("###").append(' ')
            i += 1
          }
          s = s + sb.toString
          map.put(rs.getString("productid_int"),s)
        }
        var ite = map.iterator //元组
        for (i <- ite;p <- lst;if i._1 == p.id) {
          p.attribute = i._2
        }
        map.clear()*/
        eSql = System.currentTimeMillis()
        if(eSql - bSql > 1000){
          log.info("sql:{};time:{}",sql,eSql - bSql)
        }
        //读取EC_SearchKeywordConfig
        sql = s"select ProductTypeID_int,SearchKeyword_nvarchar from EC_SearchKeywordConfig with(nolock) where ProductTypeID_int in ($stypeid)"

        bSql = System.currentTimeMillis()
        rs = stmt.executeQuery(sql)


        while (rs.next()) {
          var s = map.getOrElse(rs.getString("ProductTypeID_int"),"")
          var searchkey = rs.getString("SearchKeyword_nvarchar")
          var sb = new StringBuffer()
          sb.append(searchkey).append(' ')
          s = s + sb.toString
          map.put(rs.getString("ProductTypeID_int"),s)
        }

        var ite1 = map.iterator
        for (i1 <- ite1) {
          for (p1 <- lst) {
            if(i1._1 == p1.typeId){
              val sr = i1._2.substring(0,i1._2.length-1)
              p1.searchKeyword = sr
            }
          }
        }
        map.clear()
        eSql = System.currentTimeMillis()
        if(eSql - bSql > 1000){
          log.info("sql:{};time:{}",sql,eSql - bSql)
        }

        //读取rs_dd_prod_score_area
        sql = s"select ProductKeyID_nvarchar,countryid_int,score_float from rs_dd_prod_score_area with(nolock) where ProductKeyID_nvarchar in ($skus)"

        bSql = System.currentTimeMillis()
        rs = stmt.executeQuery(sql)

        val map2 = new HashMap[String,String]()
        while (rs.next()) {
          val area = rs.getString("countryid_int")
          val skuid = rs.getString("ProductKeyID_nvarchar")
          val score = rs.getDouble("score_float")
          val sb = new StringBuffer()
          var s = map2.getOrElse(skuid,"")
          sb.append(area).append(":").append(score).append("|")
          s = s + sb.toString

          map2.put(skuid,s)
        }
        var ite2 = map2.iterator
        for (i1 <- ite2) {
          for(p <- lst) {
            if(i1._1.equals(p.sku)) {
              val is = i1._2.substring(0,i1._2.length-1)
              p.areaScore = is
            }
          }
        }
        eSql = System.currentTimeMillis()
        if(eSql - bSql > 1000){
          log.info("sql:{};time:{}",sql,eSql - bSql)
        }
        //read www product score
        sql = s"select ProductKeyID_nvarchar,score_float from RS_DD_PROD_SCORE with(nolock) where ProductKeyID_nvarchar in ($skus)"
        bSql = System.currentTimeMillis()
        rs = stmt.executeQuery(sql)

        
        while(rs.next()){
          breakable {
            for (p <- lst) {
              if(p.sku == rs.getString("ProductKeyID_nvarchar")){
                p.wwwScore = rs.getFloat("score_float")
                break
              }
            }
          }
        }
        eSql = System.currentTimeMillis()
        if(eSql - bSql > 1000){
          log.info("sql:{};time:{}",sql,eSql - bSql)
        }
        //读取EC_eventProduct
        sql = s"""select ProductKeyID_nvarchar,COUNT(ProductKeyID_nvarchar) as count from EC_eventProduct with(nolock)
                 where ProductKeyID_nvarchar in ($skus) group by ProductKeyID_nvarchar"""

        bSql = System.currentTimeMillis()
        rs = stmt.executeQuery(sql)

        while(rs.next()) {
          if(rs.getInt("count") > 0) {
            for(p <- lst) {
              if(rs.getString("ProductKeyID_nvarchar").equals(p.sku)) {
                p.isEvent = "true"
              }
            }
          }
        }
        eSql = System.currentTimeMillis()
        if(eSql - bSql > 1000){
          log.info("sql:{};time:{}",sql,eSql - bSql)
        }
        //读取ec_indexproduct
        sql = s"select productid_int,starttime_datetime,endtime_datetime from ec_indexproduct with(nolock) where productid_int in ($ids)"

        bSql = System.currentTimeMillis()
        rs = stmt.executeQuery(sql)


        val now:Date = new Date()
        while(rs.next()) {
          if(rs.getString("starttime_datetime") != null && rs.getString("endtime_datetime") != null && (now.after(rs.getTimestamp("starttime_datetime"))) && (now.before(rs.getTimestamp("endtime_datetime"))) ) {
            for(p <- lst) {
              if(rs.getInt("productid_int").equals(p.id)) {
                p.isDailyDeal = "true"
              }
            }
          }
        }
        eSql = System.currentTimeMillis()
        if(eSql - bSql > 1000){
          log.info("sql:{};time:{}",sql,eSql - bSql)
        }
        //读取ec_indexlifeproduct
        sql = s"select productid_int,count(productid_int) count from ec_indexlifeproduct with(nolock) where productid_int in($ids) group by productid_int"

        bSql = System.currentTimeMillis()
        rs = stmt.executeQuery(sql)


        while(rs.next()) {
          if(rs.getInt("count") > 0) {
            for(p <- lst) {
              if(rs.getString("productid_int").equals(p.sku)) {
                p.isLifeStyle = "true"
              }
            }
          }
        }
        eSql = System.currentTimeMillis()
        if(eSql - bSql > 1000){
          log.info("sql:{};time:{}",sql,eSql - bSql)
        }
        //读取店铺信息
        sql = s"select ShopID_bigint,ProductKeyID_nvarchar from SRM_Plat_ShopAndProductRelation with(nolock) where ProductKeyID_nvarchar in ($skus)"
        bSql = System.currentTimeMillis()
        rs = stmt.executeQuery(sql)

        while(rs.next){
          for(p <- lst if(p.sku == rs.getString("ProductKeyID_nvarchar"))) {
            p.shopIds += (" " + rs.getString("ShopID_bigint"))
          }
        }
        eSql = System.currentTimeMillis()
        if(eSql - bSql > 1000){
          log.info("sql:{};time:{}",sql,eSql - bSql)
        }

        //读取店铺品类信息
        sql = s"select CategoryID_int,ProductKeyID_nvarchar from SRM_Plat_ShopCategoryProductRelations with(nolock) where ProductKeyID_nvarchar in ($skus)"
        bSql = System.currentTimeMillis()
        rs = stmt.executeQuery(sql)

        while(rs.next){
          for(p <- lst if(p.sku == rs.getString("ProductKeyID_nvarchar"))) {
            p.shopCategorys += (" " + rs.getString("CategoryID_int"))
          }
        }
        eSql = System.currentTimeMillis()
        if(eSql - bSql > 1000){
          log.info("sql:{};time:{}",sql,eSql - bSql)
        }
        //read fitproducttype
        sql = s"select productid_int,IndexCode_nvarchar from ec_fitproducttype with(nolock) where productid_int in ($ids)"
        bSql = System.currentTimeMillis()
        rs = stmt.executeQuery(sql)

        while(rs.next){
          for(p <- lst if(p.id == rs.getString("productid_int"))) {
            p.fitType += (" " + rs.getString("IndexCode_nvarchar"))
          }
        }
        eSql = System.currentTimeMillis()
        if(eSql - bSql > 1000){
          log.info("sql:{};time:{}",sql,eSql - bSql)
        }

        //typeshow
        sql = s"select ProductKeyID_nvarchar,IndexCode_nvarchar from EC_TypeShow with(nolock) where ProductKeyID_nvarchar in ($skus)"
        bSql = System.currentTimeMillis()
        rs = stmt.executeQuery(sql)

        while(rs.next){
          for(p <- lst if(p.sku == rs.getString("ProductKeyID_nvarchar"))) {
            p.indexCodeOfTypeShow += (" " + rs.getString("IndexCode_nvarchar"))
          }
        }
        eSql = System.currentTimeMillis()
        if(eSql - bSql > 1000){
          log.info("sql:{};time:{}",sql,eSql - bSql)
        }



        //ec_product001
        sql = s"select productid_int as id,ProductCountryInfoForCreator_nvarchar as area from EC_product001 with(nolock) where productid_int in ($ids)"
        bSql = System.currentTimeMillis()
        rs = stmt.executeQuery(sql)
        while(rs.next){
          for(p <- lst if(p.id == rs.getString("id"))) {
            val area = rs.getString("area")
            if(StringUtils.isNotBlank(area)){
              val areaSplite = area.split(",")
              val sb = new mutable.StringBuilder()
              val len = areaSplite.length
              var i = 0
              while(i < len){
                sb.append(areaSplite(i)).append(" ")
                i += 1
              }
              p.excludeAreas = sb.toString
            }
            
          }
        }
        eSql = System.currentTimeMillis()
        if(eSql - bSql > 1000){
          log.info("sql:{};time:{}",sql,eSql - bSql)
        }


        //reviews
        sql = s"""SELECT ProductID_int as id,COUNT(ProductID_int) as count FROM EC_ProductComment_QDW with(nolock) where productid_int in ($ids) GROUP BY ProductID_int"""
        bSql = System.currentTimeMillis()
        rs = stmt.executeQuery(sql)

        while(rs.next){
          for(p <- lst if(p.id == rs.getString("id"))) {
            p.reviews = rs.getInt("count")
          }
        }
        eSql = System.currentTimeMillis()
        if(eSql - bSql > 1000){
          log.info("sql:{};time:{}",sql,eSql - bSql)
        }

        sql = s"""SELECT ProductID_int as id,COUNT(ProductID_int) as count FROM EC_ProductComment with(nolock) where productid_int in ($ids) GROUP BY ProductID_int"""
        bSql = System.currentTimeMillis()
        rs = stmt.executeQuery(sql)

        while(rs.next){
          for(p <- lst if(p.id == rs.getString("id"))) {
            p.reviews += rs.getInt("count")
          }
        }
        eSql = System.currentTimeMillis()
        if(eSql - bSql > 1000){
          log.info("sql:{};time:{}",sql,eSql - bSql)
        }

        bSql = System.currentTimeMillis()
        for (pro <- lst) {
          try {
            if(writeDocNew(pro,writer)) successCount += 1 else skipCount += 1
          } catch {
            case e: Exception => failCount + 1
          }
        }
        eSql = System.currentTimeMillis()
        log.info("index data time {}",eSql - bSql)


        val consoleRoot = context.actorFor(Util.getConsoleRootAkkaURLFromMyConfig)
        consoleRoot ! CompleteSubTask(msg.name, msg.date, msg.seq, successCount, failCount, skipCount)
        val time2 = System.currentTimeMillis()
        val arr = new Array[Long](5)
        arr(0) = time2 - time1
        arr(1) = successCount
        arr(2) = failCount
        arr(3) = skipCount
        arr(4) = lst.size
        log.info("index total time {} success {} fail {} skip {} total {}", arr)
      } catch {
        case e: Exception => log.error("{}", e); e.printStackTrace()
      } finally {
        if (rs != null) rs.close()
        if (stmt != null) stmt.close()
        if (conn != null) conn.close()
      }
    }
  }

  def writeDocNew(p:Product,writer: IndexWriter): Boolean = {
    try {
      //先清空上一个结果集的数据
      pIdField.setStringValue("")
      pNameField.setStringValue("")
      pSkuField.setStringValue("")
      pIndexCodeField.setStringValue("")
      pIsOneSaleField.setStringValue("")
      pIsAliExpressField.setStringValue("")
      pBusinessNameField.setStringValue("")
      pCreateTimeField.setStringValue("")
      pProductTypeIdField.setStringValue("")
      pIsQualityProductField.setStringValue("")
      pVentureStatusField.setStringValue("")
      pVentureLevelNewField.setStringValue("")
      pIsTaobaoField.setStringValue("")
      pProductBrandIdField.setStringValue("")
      pProductBrandNameField.setStringValue("")
      wwwScoreField.setDoubleValue(0d)
      priceField.setDoubleValue(0d)

      pAttributeValueField.setStringValue("")
      pIsEventField.setStringValue("")
      pSearchKeywordField.setStringValue("")
      pIsDailyDealField.setStringValue("")
      pIsLifeStyleField.setStringValue("")
      shopIdsField.setStringValue("")
      shopCategorysField.setStringValue("")
      fitTypeField.setStringValue("")
      skuOrderField.setStringValue("")
      indexCodeTypeShowField.setStringValue("")
      excludeAreasField.setStringValue("")
      reviewsField.setIntValue(0)
      isClearanceField.setStringValue("")



      //设置本次结果集的值
      doc = new Document()

      reviewsField.setIntValue(p.reviews)
      doc.add(reviewsField)

      if(StringUtils.isNotBlank(p.isClearance)){
        isClearanceField.setStringValue(p.isClearance)
        doc.add(isClearanceField)
      }

      if(StringUtils.isNotBlank(p.excludeAreas)){
        excludeAreasField.setStringValue(p.excludeAreas)
        doc.add(excludeAreasField)
      }

      if (StringUtils.isNotBlank(p.indexCodeOfTypeShow)){
        indexCodeTypeShowField.setStringValue(p.indexCodeOfTypeShow)
        doc.add(indexCodeTypeShowField)
      }

      if (StringUtils.isNotBlank(p.fitType)){
        fitTypeField.setStringValue(p.fitType)
        doc.add(fitTypeField)
      }

      if (p.sku.charAt(0) == 'A') {
        skuOrderField.setStringValue("0")
      } else if (p.sku.charAt(0) == 'X') {
        skuOrderField.setStringValue("1")
      } else if (p.sku.charAt(0) == 'T') {
        skuOrderField.setStringValue("2")
      } else {
        skuOrderField.setStringValue("")
      }
      doc.add(skuOrderField)

      if (StringUtils.isNotBlank(p.shopIds)){
        shopIdsField.setStringValue(p.shopIds)
        doc.add(shopIdsField)
      }

      if(p.price != Double.NaN){
        priceField.setDoubleValue(p.price)
        doc.add(priceField)
      }

      if(p.wwwScore != Float.NaN){
        wwwScoreField.setDoubleValue(p.wwwScore)
        doc.add(wwwScoreField)
      }

      if(StringUtils.isNotBlank(p.id)) {
        pIdField.setStringValue(p.id)
        doc.add(pIdField)
      }

      if(StringUtils.isNotBlank(p.name)) {
        pNameField.setStringValue(p.name)
        doc.add(pNameField)
      }

      if(StringUtils.isNotBlank(p.sku)) {
        pSkuField.setStringValue(p.sku)
        doc.add(pSkuField)
      }

      if(StringUtils.isNotBlank(p.indexCode)) {
        pIndexCodeField.setStringValue(p.indexCode)
        doc.add(pIndexCodeField)
      }

      if(StringUtils.isNotBlank(p.isOneSale)) {
        pIsOneSaleField.setStringValue(p.isOneSale)
        doc.add(pIsOneSaleField)
      }

      if(StringUtils.isNotBlank(p.isAliExpress)) {
        pIsAliExpressField.setStringValue(p.isAliExpress)
        doc.add(pIsAliExpressField)
      }

      if(StringUtils.isNotBlank(p.businessName)) {
        pBusinessNameField.setStringValue(p.businessName)
        doc.add(pBusinessNameField)
      }

      if(StringUtils.isNotBlank(p.createTime)) {
        //将数据库中存的时间戳转换成索引格式yyyyMMddHHmm,注意用[]确定范围
        val lastCreateTime = p.createTime.replaceAll("[[ ][-][:][.]]","").substring(0,12)
        pCreateTimeField.setStringValue(lastCreateTime)
        doc.add(pCreateTimeField)
      }

      if(StringUtils.isNotBlank(p.typeId)) {
        pProductTypeIdField.setStringValue(p.typeId)
        doc.add(pProductTypeIdField)
      }

      if(StringUtils.isNotBlank(p.isQuality)) {
        pIsQualityProductField.setStringValue(p.isQuality)
        doc.add(pIsQualityProductField)
      }

      if(StringUtils.isNotBlank(p.ventureStatus)) {
        pVentureStatusField.setStringValue(p.ventureStatus)
        doc.add(pVentureStatusField)
      }

      if(StringUtils.isNotBlank(p.ventureLevelNew)) {
        pVentureLevelNewField.setStringValue(p.ventureLevelNew)
        doc.add(pVentureLevelNewField)
      }

      if(StringUtils.isNotBlank(p.isTaobao)) {
        pIsTaobaoField.setStringValue(p.isTaobao)
        doc.add(pIsTaobaoField)
      }

      if(StringUtils.isNotBlank(p.brandId)) {
        pProductBrandIdField.setStringValue(p.brandId)
        doc.add(pProductBrandIdField)
      }

      if(StringUtils.isNotBlank(p.brandName)) {
        pProductBrandNameField.setStringValue(p.brandName)
        doc.add(pProductBrandNameField)
      }

      if(StringUtils.isNotBlank(p.attribute)) {
        pAttributeValueField.setStringValue(p.attribute.toLowerCase)
        doc.add(pAttributeValueField)
      }

      if(StringUtils.isNotBlank(p.areaScore)) {
        val len:Int = p.areaScore.split("\\|").length
        val areaScoreSplit = p.areaScore.split("\\|") 
        var i:Int = 0
        while(i < len) {
          val areascores = areaScoreSplit(i)
          val areascore:Array[String] = areascores.split(":")
          val countryid = Integer.valueOf(areascore(0))
          val score = areascore(1)
          var prefix = "country"
          if(countryid == 1)
            prefix += "AU"
          else if(countryid == 25)
            prefix += "BR"
          else if(countryid == 30)
            prefix += "CA"
          else if(countryid == 108)
            prefix += "US"
          else if(countryid == 197)
            prefix += "RU"
          else if(countryid == 217)
            prefix += "NZ"
          else if(countryid == 339)
            prefix += "GB"
          else if(countryid == 7)
            prefix += "AR"
          else if(countryid == 198)
            prefix += "IL"
          else if(countryid == 203)
            prefix += "NL"
          else if(countryid == 207)
            prefix += "UA"

          if(prefix != "country") {
            val pAreaScore =  new DoubleField(prefix,0d, Field.Store.YES)
            pAreaScore.setDoubleValue(java.lang.Double.valueOf(score).doubleValue())
            doc.add(pAreaScore)
          }
          i += 1
        }
      }
      if(StringUtils.isNotBlank(p.isEvent)) {
        pIsEventField.setStringValue(p.isEvent)
        doc.add(pIsEventField)
      }

      if(StringUtils.isNotBlank(p.searchKeyword)) {
        pSearchKeywordField.setStringValue(p.searchKeyword)
        doc.add(pSearchKeywordField)
      }

      if(StringUtils.isNotBlank(p.isDailyDeal)) {
        pIsDailyDealField.setStringValue(p.isDailyDeal)
        doc.add(pIsDailyDealField)
      }

      if(StringUtils.isNotBlank(p.isLifeStyle)) {
        pIsLifeStyleField.setStringValue(p.isLifeStyle)
        doc.add(pIsLifeStyleField)
      }
      writer.addDocument(doc)
      true
    } catch {
      case e: Exception => log.error("index item fail,productId"); e.printStackTrace(); throw e
      false
    }
  }
}
