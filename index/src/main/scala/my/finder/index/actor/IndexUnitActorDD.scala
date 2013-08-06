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
import java.util.Date

/**
 * Created with IntelliJ IDEA.
 * User: bchen
 * Date: 13-7-29
 * Time: 下午4:21
 * To change this template use File | Settings | File Templates.
 */

case class Product(id:Int,name:String,sku:String,  indexCode:String,isOneSale:Int,isAliExpress:Int,
                   businessName:String,var createTime:String,var typeId:Int,isQuality:Int,ventureStatus:Int,ventureLevelNew:Int,
                   isTaobao:Int,brandId:Int,brandName:String,var attribute:String,var iseventproduct:Int,var searchKeyword:String,
                   var areaScore:String,var isLifeStyle:Int
                    )

class IndexUnitActorDD extends Actor with ActorLogging {
  val workDir = Config.get("workDir")
  //val indexBatchSize = Integer.valueOf(Config.get("indexBatchSize"))

  private val pIdField = new IntField("id", 0, Field.Store.YES)
  private val pNameField = new TextField("name", "", Field.Store.YES)
  private val pSkuField = new StringField("sku", "", Field.Store.YES)
  private val pIndexCodeField = new StringField("indexCode", "", Field.Store.YES)
  private val pIsOneSaleField = new IntField("isOneSale", 0, Field.Store.YES)
  private val pIsAliExpressField = new IntField("isAliExpress", 0, Field.Store.YES)
  private val pBusinessNameField = new TextField("businessName", "", Field.Store.YES)
  private val pCreateTimeField = new StringField("createTime", "", Field.Store.YES)
  //osell
  private val pProductTypeIdField = new IntField("typeId", 0, Field.Store.YES)
  private val pIsQualityProductField = new IntField("isQuality", 0, Field.Store.YES)
  private val pVentureStatusField = new IntField("ventureStatus", 0, Field.Store.YES)
  private val pVentureLevelNewField = new IntField("ventureLevelNew", 0, Field.Store.YES)
  private val pIsTaobaoField = new IntField("isTaobao", 0, Field.Store.YES)
  private val pProductBrandIdField = new IntField("brandId", 0, Field.Store.YES)
  private val pProductBrandNameField = new TextField("brandName", "", Field.Store.YES)
  //private val pProductTypeNameField = new TextField("typeName", "", Field.Store.YES)
  //private val skuOrderField = new IntField("skuOrder", 50, Field.Store.YES)


  private val pAttributeValueField =  new StringField("attribute", "", Field.Store.YES)
  //private val pArea =  new StringField("areaId", "", Field.Store.YES)
  //private val pScore =  new StringField("areaScore", "", Field.Store.YES)
  private val pIseventproductField =  new IntField("iseventproduct", 0, Field.Store.YES)
  private val pSearchKeywordField =  new StringField("searchKeyword", "", Field.Store.YES)


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
          product = Product(
            rs.getInt("ProductID_int"),rs.getString("productaliasname_nvarchar"),rs.getString("ProductKeyID_nvarchar"),rs.getString("IndexCode_nvarchar"),rs.getInt("IsOneSale_tinyint"),rs.getInt("IsAliExpress_tinyint"),
            rs.getString("BusinessName_nvarchar"),rs.getString("CreateTime_datetime"),rs.getInt("ProductTypeID_int"),rs.getInt("IsQualityProduct_tinyint"),rs.getInt("VentureStatus_tinyint"),rs.getInt("VentureLevelNew_tinyint"),
            rs.getInt("IsTaoBao_tinyint"),rs.getInt("ProductBrandID_int"),rs.getString("productbrand_nvarchar"),null,0,null,
            null,0
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
          for ( x <- vv){
            sb.append("###").append(rs.getString("itemnameeng_nvarchar")).append("###").append(x).append("###").append(' ')
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
        sql = String.format("select Productid_int,es.ProductTypeID_int,SearchKeyword_nvarchar from EC_SearchKeywordConfig es with(nolock),EC_Product ep with(nolock) where es.ProductTypeID_int = ep.ProductTypeID_int and es.ProductTypeID_int in(%s)",stypeid)
        log.info("EC_SearchKeywordConfig:"+sql)
        rs = stmt.executeQuery(sql)

        while (rs.next()) {
          var s = map.getOrElse(rs.getInt("productid_int"),"")
          var searchkey = rs.getString("SearchKeyword_nvarchar")
          var sb = new StringBuffer()
          sb.append(searchkey).append(' ')
          s = s + sb.toString
          map.put(rs.getInt("productid_int"),s)
        }

        var ite1 = map.iterator
        for (i1 <- ite1) {
          for (p1 <- lst) {
            if(i1._1 == p1.id){
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
          val score = rs.getString("score_float")
          val sb = new StringBuffer()
          var s = map2.getOrElse(skuid,"")
          sb.append(area).append(":").append(score).append("|")
          s = s + sb.toString
          //log.info("============"+s)
          map2 += (skuid -> s)
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

        //读取EC_eventProduct
        sql = String.format("select ProductKeyID_nvarchar,COUNT(ProductKeyID_nvarchar) as count from EC_eventProduct with(nolock) " +
          "where ProductKeyID_nvarchar in(%s) group by ProductKeyID_nvarchar ",skus)
        log.info("EC_eventProduct:"+sql)
        rs = stmt.executeQuery(sql)
        while(rs.next()) {
          if(rs.getInt("count") > 0) {
            for(p <- lst) {
              if(rs.getString("ProductKeyID_nvarchar").equals(p.sku)) {
                p.iseventproduct = 1
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
                p.isLifeStyle = 1
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
                p.isLifeStyle = 1
              }
            }
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
        val arr = new Array[Int](5)
        arr(0) = Integer.valueOf((time2 - time1).toString)
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
      pIdField.setIntValue(-1)
      pNameField.setStringValue("")
      pSkuField.setStringValue("")
      pIndexCodeField.setStringValue("")
      pIsOneSaleField.setIntValue(-1)
      pIsAliExpressField.setIntValue(-1)
      pBusinessNameField.setStringValue("")
      pCreateTimeField.setStringValue("")
      pProductTypeIdField.setIntValue(-1)
      pIsQualityProductField.setIntValue(-1)
      pVentureStatusField.setIntValue(-1)
      pVentureLevelNewField.setIntValue(-1)
      pIsTaobaoField.setIntValue(-1)
      pProductBrandIdField.setIntValue(-1)
      pProductBrandNameField.setStringValue("")


      pAttributeValueField.setStringValue("")
      pIseventproductField.setIntValue(-1)
      pSearchKeywordField.setStringValue("")


      //设置本次结果集的值
      doc = new Document()

      if(p.id != null)
        pIdField.setIntValue(p.id)
      log.info("{}",p.id)

      if(p.name != null)
        pNameField.setStringValue(p.name)
      log.info("{}",p.name)

      if(p.sku != null)
        pSkuField.setStringValue(p.sku)
      log.info("{}",p.sku)

      if(p.indexCode != null)
        pIndexCodeField.setStringValue(p.indexCode)
      log.info("{}",p.indexCode)

      if(p.isOneSale != null)
        pIsOneSaleField.setIntValue(p.isOneSale)
      log.info("{}",p.isOneSale)

      if(p.isAliExpress != null)
        pIsAliExpressField.setIntValue(p.isAliExpress)
      log.info("{}",p.isAliExpress)

      if(p.businessName != null)
        pBusinessNameField.setStringValue(p.businessName)
      log.info("{}",p.businessName)

      if(p.createTime != null)
        pCreateTimeField.setStringValue(p.createTime)
      log.info("{}",p.createTime)

      if(p.typeId != null)
        pProductTypeIdField.setIntValue(p.typeId)
      log.info("{}",p.typeId)

      if(p.isQuality != null)
        pIsQualityProductField.setIntValue(p.isQuality)
      log.info("{}",p.isQuality)

      if(p.ventureStatus != null)
        pVentureStatusField.setIntValue(p.ventureStatus)
      log.info("{}",p.ventureStatus)

      if(p.ventureLevelNew != null)
        pVentureLevelNewField.setIntValue(p.ventureLevelNew)
      log.info("{}",p.ventureLevelNew)

      if(p.isTaobao != null)
        pIsTaobaoField.setIntValue(p.isTaobao)
      log.info("{}",p.isTaobao)

      if(p.brandId != null)
        pProductBrandIdField.setIntValue(p.brandId)
      log.info("{}",p.brandId)

      if(p.brandName != null)
        pProductBrandNameField.setStringValue(p.brandName)
      log.info("{}",p.brandName)

      if(p.attribute != null)
        pAttributeValueField.setStringValue(p.attribute)
      log.info("{}",p.attribute)

      if(p.areaScore != null) {
        log.info("p.areaScore=="+p.areaScore)

        for(i <- 0 to p.areaScore.split("\\|").length-1) {
          var areascores = p.areaScore.split("\\|")(i)
          var areascore = areascores.split(":")
          var countryid = areascore(0).toInt
          var score = areascore(1)
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
            val pArea =  new StringField("areaId"+i, "", Field.Store.YES)
            val pScore =  new StringField("areaScore"+i, "", Field.Store.YES)
            pArea.setStringValue(prefix)
            pScore.setStringValue(score)
            log.info("prefix={}",prefix)
            log.info("score={}",score)
            doc.add(pArea)
            doc.add(pScore)
          }
        }
      }

      if(p.iseventproduct != null)
        pIseventproductField.setIntValue(p.iseventproduct)
      log.info("{}",p.iseventproduct)

      if(p.searchKeyword != null)
        pSearchKeywordField.setStringValue(p.searchKeyword)
      log.info("{}",p.searchKeyword)

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
      doc.add(pIseventproductField)
      doc.add(pSearchKeywordField)

      writer.addDocument(doc)
      true
    } catch {
      case e: Exception => log.error("index item fail,productId"); e.printStackTrace(); throw e
        false
    }
  }
}
