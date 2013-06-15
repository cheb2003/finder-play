package my.finder.index.actor

import akka.actor.{ActorLogging, Actor}
import my.finder.common.util.{Util, MongoUtil, Config}
import com.mongodb.casbah.Imports._
import org.apache.lucene.document._

import my.finder.index.service.{MongoManager, IndexWriteManager}
import my.finder.common.message.{CompleteIncIndexTask, IndexIncremetionalTaskMessage, CompleteSubTask, IndexTaskMessage}
import org.apache.commons.lang.StringUtils
import java.util.Date
import org.apache.lucene.index.IndexWriter
import java.io.File
import my.finder.index.service.DBService
import scala.slick.session.Database
import scala.slick.jdbc.{GetResult, StaticQuery => Q}
import scala.collection.mutable.ListBuffer
import java.sql.{ResultSet, Statement, Connection}

/**
 *
 */
case class SegmentWord(sku:String,word:String,lang:String)

class IndexUnitActor extends Actor with ActorLogging with MongoUtil {
  implicit val getSegmentWordResult = GetResult(r => SegmentWord(r.<<, r.<<,r.<<))
  val workDir = Config.get("workDir")
  val dinobuydb = Config.get("dinobuydb")

  val indexBatchSize = Integer.valueOf(Config.get("indexBatchSize"))
  var productColl:MongoCollection = null


  val fields = MongoDBObject("productid_int" -> 1, "ec_product.productaliasname_nvarchar" -> 1
    , "ec_productprice.unitprice_money" -> 1, "ec_product.productbrand_nvarchar" -> 1
    , "ec_product.businessbrand_nvarchar" -> 1, "ec_product.indexcode_nvarchar" -> 1
    , "ec_product.productbrandid_int" -> 1, "ec_product.isonesale_tinyint" -> 1
    , "ec_product.isaliexpress_tinyint" -> 1, "productkeyid_nvarchar" -> 1
    , "ec_productlanguage" -> 1, "ec_product.createtime_datetime" -> 1
    , "ec_product.businessname_nvarchar" -> 1, "ec_product.isstopsale_bit" -> 1
    , "ec_product.qdwproductstatus_int" -> 1
    //osell需要的属性
    , "ec_product.producttypeid_int" -> 1,"ec_product.isqualityproduct_tinyint" -> 1
    , "ec_product.venturestatus_tinyint" -> 1

  )
  //val sort = MongoDBObject("productid_int" -> 1)

  private val pIdField = new IntField("pId", 0, Field.Store.YES);
  private val pNameField = new TextField("pName", "", Field.Store.YES)
  private val priceField = new DoubleField("unitPrice", 0.0f, Field.Store.YES)
  private val indexCodeField = new StringField("indexCode", "", Field.Store.YES)
  private val isOneSaleField = new IntField("isOneSale", 0, Field.Store.YES)
  private val isAliExpressField = new IntField("isAliExpress", 0, Field.Store.YES)
  private val skuField = new StringField("sku", "", Field.Store.YES)
  private val businessNameField = new StringField("businessName", "", Field.Store.YES)
  private val pNameRuField = new TextField("pNameRU", "", Field.Store.YES)
  private val segmentWordRuField = new TextField("segmentWordRu", "", Field.Store.YES)
  private val segmentWordBrField = new TextField("segmentWordBr", "", Field.Store.YES)
  private val segmentWordEnField = new TextField("segmentWordEn", "", Field.Store.YES)
  private val pNameBrField = new TextField("pNameBR", "", Field.Store.YES)
  private val createTimeField = new StringField("createTime", "", Field.Store.YES)
  //osell
  private val productTypeIdField = new IntField("pTypeId", 0, Field.Store.YES)
  private val isQualityProductField = new IntField("isQuality", 0, Field.Store.YES)
  private val ventureStatusField = new IntField("ventureStatus", 0, Field.Store.YES)

  private val isTaobaoField = new IntField("isTaobao", 0, Field.Store.YES)

  private val productBrandIdField = new IntField("pBrandId", 0, Field.Store.YES)
  private val businessBrandField = new IntField("businessBrand", 0, Field.Store.YES)



  private var doc: Document = null

  override def preStart() {
    val mongo = MongoManager()
    productColl = mongo(dinobuydb)("ec_productinformation")
  }
  def writeDoc(x: DBObject,words:List[SegmentWord], writer: IndexWriter):Boolean = {
    var list: MongoDBList = null
    try {
      if (mvp[Int](x, "qdwproductstatus_int") < 2 && mvp[Boolean](x, "isstopsale_bit") == false
        && x.as[MongoDBList]("ec_productprice").length > 0 && x.as[MongoDBList]("ec_productprice").as[DBObject](0).as[Double]("unitprice_money") > 0) {
        list = x.as[MongoDBList]("ec_productprice")
        doc = new Document()
        pIdField.setIntValue(x.as[Int]("productid_int"));
        pNameField.setStringValue(StringUtils.defaultIfBlank(mvp[String](x, "productaliasname_nvarchar"), StringUtils.defaultIfBlank(mvp[String](x, "businessbrand_nvarchar"), "")) + ' ' + mvp[String](x, "productaliasname_nvarchar"))
        indexCodeField.setStringValue(mvp[String](x, "indexcode_nvarchar"))

        try {
          createTimeField.setStringValue(DateTools.dateToString(mvp[Date](x, "createtime_datetime"), DateTools.Resolution.MINUTE))
          doc.add(createTimeField)
        } catch {
          case e: Exception =>
        }
        if (mvp[String](x, "businessname_nvarchar").trim != "") {
          businessNameField.setStringValue(mvp[String](x, "businessname_nvarchar"))
          doc.add(businessNameField)
        }
        try {
          priceField.setDoubleValue(list.as[DBObject](0).as[Double]("unitprice_money"))
          doc.add(priceField)
        } catch {
          case e: Exception =>
        }
        try {
          isOneSaleField.setIntValue(mvp[Int](x, "isonesale_tinyint"))
          doc.add(isOneSaleField)
        } catch {
          case e: Exception =>
        }
        try {
          isAliExpressField.setIntValue(mvp[Int](x, "isaliexpress_tinyint"))
          doc.add(isAliExpressField)
        } catch {
          case e: Exception =>
        }

        try {
          productTypeIdField.setIntValue(mvp[Int](x, "producttypeid_int"))
          doc.add(productTypeIdField)
        } catch {
          case e: Exception =>
        }
        try {
          isQualityProductField.setIntValue(mvp[Int](x, "isqualityproduct_tinyint"))
          doc.add(isQualityProductField)
        } catch {
          case e: Exception =>
        }
        try {
          ventureStatusField.setIntValue(mvp[Int](x, "venturestatus_tinyint"))
          doc.add(ventureStatusField)
        } catch {
          case e: Exception =>
        }
        try {
          isTaobaoField.setIntValue(mvp[Int](x, "istaobao_tinyint"))
          doc.add(isTaobaoField)
        } catch {
          case e: Exception =>
        }
        try {
          productBrandIdField.setIntValue(mvp[Int](x, "productbrandid_int"))
          doc.add(productBrandIdField)
        } catch {
          case e: Exception =>
        }
        try {
          businessBrandField.setStringValue(mvp[String](x, "businessbrand_nvarchar"))
          doc.add(businessBrandField)
        } catch {
          case e: Exception =>
        }

        val sku = mv[String](x, "productkeyid_nvarchar")
        skuField.setStringValue(sku)
        if(words.length > 0){
          for(x <- words) {
            if (x.sku == sku) {
              if(x.lang.toLowerCase() =="ru"){
                segmentWordRuField.setStringValue(x.word.replace("|"," "))
                doc.add(segmentWordRuField)
              }
              if(x.lang.toLowerCase() == "en"){
                segmentWordEnField.setStringValue(x.word.replace("|"," "))
                doc.add(segmentWordEnField)
              }
              if(x.lang.toLowerCase() == "pt"){
                segmentWordBrField.setStringValue(x.word.replace("|"," "))
                doc.add(segmentWordBrField)
              }
            }
          }
        }

        list = x.as[MongoDBList]("ec_productlanguage")
        for (y <- 0 until list.length) {
          if (list.as[DBObject](y).as[String]("language_nvarchar").toLowerCase() == "ru") {
            pNameRuField.setStringValue(list.as[DBObject](y).as[String]("producttitle_nvarchar"))
            doc.add(pNameRuField)
          }
          if (list.as[DBObject](y).as[String]("language_nvarchar").toLowerCase() == "br") {
            pNameBrField.setStringValue(list.as[DBObject](y).as[String]("producttitle_nvarchar"))
            doc.add(pNameBrField)
          }
        }
        doc.add(pIdField)
        doc.add(pNameField)
        doc.add(indexCodeField)
        doc.add(skuField)



        writer.addDocument(doc)
        true
        //successCount += 1
      } else {
        //skipCount += 1
        false
      }
    } catch {
      case e: Exception => log.error("index item fail,productId {};", x.as[Int]("productid_int")); throw e//failCount += 1
    }

    /*doc = new Document
    doc.add(pIdField)
    doc.add(pNameField)
    doc.add(indexCodeField)
    doc.add(skuField)
    writer.addDocument(doc)*/
  }
  def receive = {
    case msg:IndexIncremetionalTaskMessage => {
      log.info("receive incrementional index message")
      val time1 = System.currentTimeMillis();

      val incPath = Util.getIncrementalPath(msg.name,msg.date)
      val timeFile = new File(workDir + "/" + incPath + "/time")
      val from = new Date(timeFile.lastModified())
      val to = new Date
      var successCount = 0
      var failCount = 0
      var skipCount = 0
      val q = "ec_product.createtime_datetime" $gte from $lt to
      val writer = IndexWriteManager.getIncIndexWriter(msg.name, msg.date)

      val items: MongoCursor = productColl.find(q, fields)
      log.info("index inc item {}",items.size)
      for (x <- items) {
        try{
          if(writeDoc(x,null, writer)) successCount += 1 else skipCount += 1
        } catch {
          case e:Exception => failCount += 1
        }

      }
      /*for (x <- 1 to 100) {
        writeDoc(null, writer)
      }*/
      writer.commit();
      //TODO 应该时间排序取最后一个记录的时间，作为lastupdatetime
      timeFile.delete()
      timeFile.createNewFile()
      timeFile.setLastModified(to.getTime)
      val time2 = System.currentTimeMillis();
      log.info("index incremental spent {}",time2 - time1)
      val consoleRoot = context.actorFor("akka://console@127.0.0.1:2552/user/root")
      log.info("index incremental {}/{}",successCount,items.size)
      consoleRoot ! CompleteIncIndexTask(msg.name, msg.date,successCount,failCount,skipCount)
    }
    case msg: IndexTaskMessage => {
      try{
        //log.info("recevie indextaskmessage {}",msg.date)
        val time1 = System.currentTimeMillis()
        val writer = IndexWriteManager.getIndexWriter(msg.name, msg.date)
        //val now:Date = msg.date
        var successCount:Int = 0
        var failCount:Int = 0
        var skipCount:Int = 0
        var time3 = System.currentTimeMillis()
        //val items: MongoCursor = productColl.find("ec_product.createtime_datetime" $lt now, fields).skip(Integer.valueOf((msg.seq * 2).toString())).limit(2000)
        val items: MongoCursor = productColl.find("productid_int" $in msg.ids, fields,0,indexBatchSize)
        val lst = items.toList


        var time4 = System.currentTimeMillis()
        log.info("load items {}",time4 - time3)
        time3 = System.currentTimeMillis()
        val words = getSegmentWords(lst)
        time4 = System.currentTimeMillis()
        log.info("load words {}",time4 - time3)

        //log.info("find items {}",time4 - time3)


        //log.info("spent {} millisecond in finding items {}",time2 - time1,items.size)
        for (x <- lst) {
          /*if(!b) b = true else log.info("load item {}",time3 -time4)
          time3 = System.currentTimeMillis()*/
          try{
            if(writeDoc(x,words, writer)) successCount += 1 else skipCount += 1
            //if(writeDoc(x,List(), writer)) successCount += 1 else skipCount += 1
          } catch {
            case e:Exception => failCount + 1
          }
          //time4 = System.currentTimeMillis()
        }
        /*for (x <- 1 to 100) {
          writeDoc(null, writer)
        }*/
        val indexManager = context.actorFor("akka://console@127.0.0.1:2552/user/root")
        indexManager ! CompleteSubTask(msg.name, msg.date, msg.seq, successCount, failCount, skipCount)
        val time2 = System.currentTimeMillis()
        items.close()
        val arr = new Array[Int](5)
        arr(0) = Integer.valueOf((time2 - time1).toString)
        arr(1) = successCount
        arr(2) = failCount
        arr(3) = skipCount
        arr(4) = items.size
        log.info("index time {} success {} fail {} skip {} total {}",arr);
      } catch {
        case e:Exception => log.error("{}",e)
      }
    }
  }
  def getSegmentWords(items:List[DBObject]):List[SegmentWord] = {
    /*val db = Database.forDataSource(DBService.dataSource)
    val sb = new StringBuffer()
    sb.append("select ProductKeyID_nvarchar as sku,SegmentWord_nvarchar as word from QDW_TB_ProductTitleSegmentWord where ProductKeyID_nvarchar in (")
    for (x <- items) {
      sb.append('\'').append(mv[String](x,"productkeyid_nvarchar")).append("',")
    }
    val segmentWords = ListBuffer[SegmentWord]()


    val q = Q.queryNA[SegmentWord](sb.substring(0,sb.length() - 1))
    for(x <- q){
      segmentWords += x
    }*/
    val list = ListBuffer[SegmentWord]()
    var conn:Connection = null
    var stmt:Statement = null
    var rs:ResultSet = null
    try{
      conn = DBService.dataSource.getConnection()
      stmt = conn.createStatement()
      val sb = new StringBuffer()
      sb.append("select ProductKeyID_nvarchar as sku,SegmentWord_nvarchar as word,LanguageCode_nvarchar as lang from QDW_TB_ProductTitleSegmentWord WITH (NOLOCK) where ProductKeyID_nvarchar in (")
      for (x <- items) {
        sb.append('\'').append(mv[String](x,"productkeyid_nvarchar")).append("',")
      }
      val sql = sb.substring(0,sb.length() - 1) + ")"
      rs = stmt.executeQuery(sql)
      while(rs.next()){
        list += SegmentWord(rs.getString("sku"),rs.getString("word"),rs.getString("lang"))
      }
    } catch {
      case e:Exception => {
        if(rs != null) rs.close()
        if(stmt != null) stmt.close()
        if(conn != null) conn.close()
      }
    }

    list.toList
  }
}
