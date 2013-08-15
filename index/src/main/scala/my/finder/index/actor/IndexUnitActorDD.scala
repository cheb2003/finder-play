package my.finder.index.actor

import akka.actor.{ActorLogging, Actor}
import my.finder.common.util.{Util, Config}
import org.apache.lucene.document._
import my.finder.common.message.{CompleteSubTask, IndexTaskMessageDD}
import my.finder.index.service.{DBService, IndexWriteManager}
import java.sql.{ResultSet, Statement, Connection}
import org.apache.lucene.index.IndexWriter
import scala.collection.mutable.{ListBuffer}
import scala.collection.mutable.HashMap
import scala.util.control.Breaks._
import java.util.Date
import java.lang.String
import scala.Predef.String
import org.apache.commons.lang.StringUtils

class Product(val id:String,val name:String,var sku:String,var indexCode:String,var isOneSale:String,
                  var isAliExpress:String,var businessName:String,var createTime:String,var typeId:String,
                  var isQuality:String,var ventureStatus:String,var ventureLevelNew:String,
                   var isTaobao:String,var brandId:String,var brandName:String,var attribute:String,
                   var isEvent:String,var searchKeyword:String, var areaScore:String,var isDailyDeal:String,
                   var isLifeStyle:String,var wwwScore:Float,
                   var shopIds:String,var shopCategorys:String,var fitType:String)

class IndexUnitActorDD extends Actor with ActorLogging {
  val workDir = Config.get("workDir")
  //val indexBatchSize = Integer.valueOf(Config.get("indexBatchSize"))

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
  //private val skuOrderField = new IntField("skuOrder", 50, Field.Store.YES)


  private val pAttributeValueField =  new StringField("attribute", "", Field.Store.YES)
  private val pIsEventField =  new StringField("isEvent", "", Field.Store.YES)
  private val pSearchKeywordField =  new StringField("searchKeyword", "", Field.Store.YES)
  //www score
  private val wwwScoreField = new DoubleField("wwwScore", 0d, Field.Store.YES)

  private val pIsDailyDealField =  new StringField("isDailyDeal", "", Field.Store.YES)
  private val pIsLifeStyle =  new StringField("isLifeStyle", "", Field.Store.YES)
  private val shopIdsField =  new TextField("shopIds", "", Field.Store.YES)
  private val shopCategorysField =  new TextField("shopCategorys", "", Field.Store.YES)
  private val fitTypeField = new TextField("fitType","",Field.Store.YES)

  private var doc: Document = null

  override def preStart() {
  }

  def receive = {
    
    case msg: IndexTaskMessageDD => {
      var conn: Connection = null
      var stmt: Statement = null
      var rs: ResultSet = null

      val buffer1:StringBuffer = new StringBuffer()
      val buffer2:StringBuffer = new StringBuffer()
      val buffer3:StringBuffer = new StringBuffer()

      val lst = new ListBuffer[Product]

      try {
        log.info("执行到读取数据建立索引")
        val time1 = System.currentTimeMillis()
        val writer = IndexWriteManager.getIndexWriter(msg.name, msg.date)
        var successCount: Int = 0
        var failCount: Int = 0
        var skipCount: Int = 0
        var total:Int = 0

        conn = DBService.dataSource.getConnection()
        stmt = conn.createStatement()

        //读取ec_product
        var sql = "select ProductID_int,productaliasname_nvarchar,ProductKeyID_nvarchar,IndexCode_nvarchar,IsOneSale_tinyint,IsAliExpress_tinyint, "+
          "BusinessName_nvarchar,CreateTime_datetime,ProductTypeID_int,IsQualityProduct_tinyint,VentureStatus_tinyint,VentureLevelNew_tinyint, "+
          "IsTaoBao_tinyint,ProductBrandID_int,productbrand_nvarchar, " +
          "BusinessBrand_nvarchar,ProductPrice_money,QDWProductStatus_int " +
          "from ec_product with(nolock) where VentureStatus_tinyint <> 3 and ProductPrice_money > 0 "+
          "and isnull(VentureLevelNew_tinyint,0) = 0 and QDWProductStatus_int = 0 and VentureStatus_tinyint <> 4 "+
          "and ProductID_int between "+msg.minId+" and "+msg.maxId
        log.info("ec_product:"+sql)
        rs = stmt.executeQuery(sql)
        var product:Product  = null
        while (rs.next()) {
          product = new Product(
            rs.getString("ProductID_int"),rs.getString("productaliasname_nvarchar"),rs.getString("ProductKeyID_nvarchar"),rs.getString("IndexCode_nvarchar"),rs.getString("IsOneSale_tinyint"),rs.getString("IsAliExpress_tinyint"),
            rs.getString("BusinessName_nvarchar"),rs.getString("CreateTime_datetime"),rs.getString("ProductTypeID_int"),rs.getString("IsQualityProduct_tinyint"),rs.getString("VentureStatus_tinyint"),rs.getString("VentureLevelNew_tinyint"),
            rs.getString("IsTaoBao_tinyint"),rs.getString("ProductBrandID_int"),rs.getString("productbrand_nvarchar"),null,null,null,
            null,null,null,Float.NaN,null,null,null
          )
          lst += product
          buffer1.append(rs.getInt("ProductID_int")).append(',')
          buffer2.append("'").append(rs.getString("ProductKeyID_nvarchar")).append("'").append(',')
          buffer3.append("'").append(rs.getString("ProductTypeID_int")).append("'").append(',')
        }
        val ids = buffer1.substring(0,buffer1.length() - 1)
        val skus = buffer2.substring(0,buffer2.length() - 1)
        val stypeid = buffer3.substring(0,buffer3.length() - 1)

        //读取EC_ProductExtendItem
        sql = String.format("select productid_int,itemvalueeng_nvarchar,itemnameeng_nvarchar from EC_ProductExtendItem with(nolock) where ProductID_int in(%s) and WebSiteID_smallint = 61 and AttributeInputType_int = 1 ",ids)
        log.info("EC_ProductExtendItem:"+sql)
        rs = stmt.executeQuery(sql)

        //属性
        val map = new HashMap[Int,String]()
        while (rs.next()) {
          var s = map.getOrElse(rs.getInt("productid_int"),"")
          val v = rs.getString("itemvalueeng_nvarchar")
          val vv = v.split(",")
          val sb = new StringBuffer()
          var i = 0
          while(i < vv.length){
            sb.append("###").append(rs.getString("itemnameeng_nvarchar")).append("###").append(vv(i)).append("###").append(' ')
            i += 1
          }
          s = s + sb.toString
          map.put(rs.getInt("productid_int"),s)
        }
        var ite = map.iterator //元组
        for (i <- ite) {
          for (p <- lst) {
            if(i._1 == p.id){
              p.attribute = i._2
            }
          }
        }
        map.clear()

        //读取EC_SearchKeywordConfig
        sql = String.format("select ProductTypeID_int,SearchKeyword_nvarchar from EC_SearchKeywordConfig with(nolock) where ProductTypeID_int in(%s)",stypeid)
        log.info("EC_SearchKeywordConfig:"+sql)
        rs = stmt.executeQuery(sql)

        while (rs.next()) {
          var s = map.getOrElse(rs.getInt("ProductTypeID_int"),"")
          var searchkey = rs.getString("SearchKeyword_nvarchar")
          var sb = new StringBuffer()
          sb.append(searchkey).append(' ')
          s = s + sb.toString
          map.put(rs.getInt("ProductTypeID_int"),s)
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

        //读取rs_dd_prod_score_area
        sql = String.format("select ProductKeyID_nvarchar,countryid_int,score_float from rs_dd_prod_score_area with(nolock) where ProductKeyID_nvarchar in(%s) ",skus)
        log.info("rs_dd_prod_score_area:"+sql)
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
          //log.info("============"+s)
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
        //read www product score
        sql = String.format("select ProductKeyID_nvarchar,score_float from RS_DD_PROD_SCORE with(nolock) where ProductKeyID_nvarchar in(%s) ",skus)
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
        //读取EC_eventProduct
        sql = String.format("select ProductKeyID_nvarchar,COUNT(ProductKeyID_nvarchar) as count from EC_eventProduct with(nolock) " +
          "where ProductKeyID_nvarchar in(%s) group by ProductKeyID_nvarchar ",skus)
        log.info("EC_eventProduct:"+sql)
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

        //读取ec_indexproduct
        sql = String.format("select productid_int,starttime_datetime,endtime_datetime from ec_indexproduct with(nolock) where productid_int in(%s) ",ids)
        log.info("ec_indexproduct:"+sql)
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

        //读取ec_indexlifeproduct
        sql = String.format("select productid_int,count(productid_int) count from ec_indexlifeproduct with(nolock) where productid_int in(%s) group by productid_int ",ids)
        log.info("ec_indexlifeproduct:"+sql)
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

        //读取店铺信息
        sql = String.format("select ShopID_bigint,ProductKeyID_nvarchar from SRM_Plat_ShopAndProductRelation where ProductKeyID_nvarchar in (%s)",skus)
        rs = stmt.executeQuery(sql)
        while(rs.next){
          for(p <- lst if(p.sku == rs.getString("ProductKeyID_nvarchar"))) {
            p.shopIds += p.shopIds + " " + rs.getString("ShopID_bigint")
          }
        }
        //读取店铺品类信息
        sql = String.format("select CategoryID_int,ProductKeyID_nvarchar from SRM_Plat_ShopCategoryProductRelations where ProductKeyID_nvarchar in (%s)",skus)
        rs = stmt.executeQuery(sql)
        while(rs.next){
          for(p <- lst if(p.sku == rs.getString("ProductKeyID_nvarchar"))) {
            p.shopCategorys += p.shopCategorys + " " + rs.getString("CategoryID_int")
          }
        }
        //read fitproducttype
        sql = String.format("select productid_int,IndexCode_nvarchar from ec_fitproducttype where productid_int in (%s)",ids)
        rs = stmt.executeQuery(sql)
        while(rs.next){
          for(p <- lst if(p.id == rs.getString("productid_int"))) {
            p.fitType += p.fitType + " " + rs.getString("IndexCode_nvarchar")
          }
        }

        for (pro <- lst) {
          total = lst.length
          val time3 = System.currentTimeMillis()
          try {
            if(writeDocNew(pro,writer)) successCount += 1 else skipCount += 1
          } catch {
            case e: Exception => failCount + 1
          }
          val time4 = System.currentTimeMillis()
          log.info("load items {}", time4 - time3)
        }
        log.info("msg.total={}", total)

        val consoleRoot = context.actorFor(Util.getConsoleRootAkkaURLFromMyConfig)
        consoleRoot ! CompleteSubTask(msg.name, msg.date, msg.seq, successCount, failCount, skipCount)
        val time2 = System.currentTimeMillis()
        val arr = new Array[Long](5)
        arr(0) = time2 - time1
        arr(1) = successCount
        arr(2) = failCount
        arr(3) = skipCount
        arr(4) = total
        log.info("index time {} success {} fail {} skip {} total {}", arr)
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

      pAttributeValueField.setStringValue("")
      pIsEventField.setStringValue("")
      pSearchKeywordField.setStringValue("")
      pIsDailyDealField.setStringValue("")
      pIsLifeStyle.setStringValue("")
      shopIdsField.setStringValue("")
      shopCategorysField.setStringValue("")
      fitTypeField.setStringValue("")


      //设置本次结果集的值
      doc = new Document()

      if (StringUtils.isNotBlank(p.fitType)){
        fitTypeField.setStringValue(p.fitType)
        doc.add(fitTypeField)
      }
      
      if (StringUtils.isNotBlank(p.shopIds)){
        shopIdsField.setStringValue(p.shopIds)
        doc.add(shopIdsField)
      }

      if(p.wwwScore != Float.NaN){
        wwwScoreField.setDoubleValue(p.wwwScore)
        doc.add(wwwScoreField)
      }

      if(p.id != null)
        pIdField.setStringValue(p.id)

      if(p.name != null)
        pNameField.setStringValue(p.name)

      if(p.sku != null)
        pSkuField.setStringValue(p.sku)

      if(p.indexCode != null)
        pIndexCodeField.setStringValue(p.indexCode)

      pIsOneSaleField.setStringValue(p.isOneSale)

      pIsAliExpressField.setStringValue(p.isAliExpress)

      if(p.businessName != null)
        pBusinessNameField.setStringValue(p.businessName)

      if(p.createTime != null)
        pCreateTimeField.setStringValue(p.createTime)

      pProductTypeIdField.setStringValue(p.typeId)

      pIsQualityProductField.setStringValue(p.isQuality)

      pVentureStatusField.setStringValue(p.ventureStatus)

      pVentureLevelNewField.setStringValue(p.ventureLevelNew)

      pIsTaobaoField.setStringValue(p.isTaobao)

      pProductBrandIdField.setStringValue(p.brandId)

      if(p.brandName != null)
        pProductBrandNameField.setStringValue(p.brandName)

      if(p.attribute != null)
        pAttributeValueField.setStringValue(p.attribute)

      if(p.areaScore != null) {
        log.info("p.areaScore=="+p.areaScore)

        val len:Int = p.areaScore.split("\\|").length
        val areaScoreSplit = p.areaScore.split("\\|") 
        val i:Int = 0
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
            pAreaScore.setDoubleValue(Double.unbox(score))
            log.info("{}",pAreaScore)
            doc.add(pAreaScore)
          }
        }
      }

      pIsEventField.setStringValue(p.isEvent)

      if(p.searchKeyword != null)
        pSearchKeywordField.setStringValue(p.searchKeyword)

      pIsDailyDealField.setStringValue(p.isDailyDeal)

      if(p.isLifeStyle != None){
        pIsLifeStyle.setStringValue(p.isLifeStyle)
      }


      doc.add(pIdField)
      doc.add(pNameField)
      doc.add(pSkuField)
      doc.add(pIndexCodeField)
      doc.add(pIsOneSaleField)
      doc.add(pIsAliExpressField)
      doc.add(pBusinessNameField)
      doc.add(pCreateTimeField)
      doc.add(pProductTypeIdField)
      doc.add(pIsQualityProductField)
      doc.add(pVentureStatusField)
      doc.add(pVentureLevelNewField)
      doc.add(pIsTaobaoField)
      doc.add(pProductBrandIdField)
      doc.add(pProductBrandNameField)

      doc.add(pAttributeValueField)
      doc.add(pIsEventField)
      doc.add(pSearchKeywordField)
      doc.add(pIsDailyDealField)
      doc.add(pIsLifeStyle)

      writer.addDocument(doc)
      true
    } catch {
      case e: Exception => log.error("index item fail,productId"); e.printStackTrace(); throw e
      false
    }
  }
}
