package my.finder.index.actor

import edu.fudan.ml.types.Dictionary;
import edu.fudan.nlp.cn.tag.CWSTagger;
import akka.actor.{ ActorLogging, Actor }
import my.finder.common.util.{ Util, MongoUtil, Config }
import com.mongodb.casbah.Imports._
import org.apache.lucene.document._

import my.finder.index.service.{ MongoManager, IndexWriteManager }
import my.finder.common.message._
import my.finder.common.util._
import org.apache.commons.lang.StringUtils
import java.util.Date
import org.apache.lucene.index.IndexWriter
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import java.io.{ FileWriter, File }
import my.finder.index.service.DBService
//import scala.slick.jdbc.{GetResult, StaticQuery => Q}
import scala.collection.mutable.ListBuffer
import java.sql.{ PreparedStatement, ResultSet, Statement, Connection }
import java.text.SimpleDateFormat

import org.apache.lucene.analysis.en.EnglishAnalyzer
import java.io.StringReader
import org.apache.lucene.analysis.TokenStream
import org.apache.lucene.util.Version
/**
 *
 */
case class SegmentWord(sku: String, word: String, lang: String, cn: String, titlecn: String)

class IndexUnitActor extends Actor with ActorLogging with MongoUtil {
  //implicit val getSegmentWordResult = GetResult(r => SegmentWord(r.<<, r.<<,r.<<,r.<<,r.<<))
  val workDir = Config.get("workDir")
  val oldDir = Config.get("oldDir")
  val dinobuydb = Config.get("dinobuydb")
  val englishAnalyzer = new EnglishAnalyzer(Version.LUCENE_43)
  val indexBatchSize = Integer.valueOf(Config.get("indexBatchSize"))
  var productColl: MongoCollection = null
  var productInactiveColl: MongoCollection = null

  val fields = MongoDBObject("_id" -> 0, "productid_int" -> 1, "ec_product.productaliasname_nvarchar" -> 1, "ec_productprice.unitprice_money" -> 1, "ec_product.productbrand_nvarchar" -> 1, "ec_product.businessbrand_nvarchar" -> 1, "ec_product.indexcode_nvarchar" -> 1, "ec_product.productbrandid_int" -> 1, "ec_product.isonesale_tinyint" -> 1, "ec_product.isaliexpress_tinyint" -> 1, "productkeyid_nvarchar" -> 1, "ec_productlanguage" -> 1, "ec_product.createtime_datetime" -> 1, "ec_product.businessname_nvarchar" -> 1, "ec_product.isstopsale_bit" -> 1, "ec_product.qdwproductstatus_int" -> 1, "ec_product.excavatekeywords_nvarchar" -> 1 //osell需要的属性
  , "ec_product.producttypeid_int" -> 1, "ec_product.isqualityproduct_tinyint" -> 1, "ec_product.venturestatus_tinyint" -> 1, "ec_product.istaobao_tinyint" -> 1, "ec_product.venturelevelnew_tinyint" -> 1)
  val fields1 = MongoDBObject("ec_productextendgroup" -> 1, "_id" -> 0)
  //val sort = MongoDBObject("productid_int" -> 1)

  private val pIdField = new IntField("pId", 0, Field.Store.YES);
  private val pNameField = new TextField("pName", "", Field.Store.YES)
  private val priceField = new DoubleField("unitPrice", 0.0f, Field.Store.YES)
  private val indexCodeField = new StringField("indexCode", "", Field.Store.YES)
  private val isOneSaleField = new IntField("isOneSale", 0, Field.Store.YES)
  private val isAliExpressField = new IntField("isAliExpress", 0, Field.Store.YES)
  private val skuField = new StringField("sku", "", Field.Store.YES)
  private val businessNameField = new TextField("businessName", "", Field.Store.YES)
  private val pNameRuField = new TextField("pNameRU", "", Field.Store.YES)

  private val segmentWordRuField = new TextField("segmentWordRu", "", Field.Store.YES)
  private val segmentWordBrField = new TextField("segmentWordBr", "", Field.Store.YES)
  private val segmentWordEnField = new TextField("segmentWordEn", "", Field.Store.YES)
  private val sourceKeywordField = new TextField("sourceKeyword", "", Field.Store.YES)
  private val sourceKeywordCNField = new TextField("sourceKeywordCN", "", Field.Store.YES)
  private val skuOrderField = new IntField("skuOrder", 50, Field.Store.YES)

  private val pNameBrField = new TextField("pNameBR", "", Field.Store.YES)
  private val pNameCnField = new TextField("pNameCN", "", Field.Store.YES)
  private val createTimeField = new StringField("createTime", "", Field.Store.YES)
  //osell
  private val productTypeIdField = new IntField("pTypeId", 0, Field.Store.YES)
  private val isQualityProductField = new IntField("isQuality", 0, Field.Store.YES)
  private val ventureStatusField = new IntField("ventureStatus", 0, Field.Store.YES)
  private val ventureLevelNewField = new IntField("ventureLevelNew", 0, Field.Store.YES)

  private val isTaobaoField = new IntField("isTaobao", 0, Field.Store.YES)

  private val productBrandIdField = new IntField("pBrandId", 0, Field.Store.YES)
  private val productBrandNameField = new TextField("pBrandName", "", Field.Store.YES)
  private val productTypeNameEnField = new TextField("pTypeNameEN", "", Field.Store.YES)
  private val productTypeNameRuField = new TextField("pTypeNameRU", "", Field.Store.YES)
  private val productTypeNameBrField = new TextField("pTypeNameBR", "", Field.Store.YES)
  private val businessBrandField = new TextField("businessBrand", "", Field.Store.YES)

  private val pExtendItemBRField = new TextField("pExtendItemBR", "", Field.Store.YES)
  private val pExtendItemRUField = new TextField("pExtendItemRU", "", Field.Store.YES)
  private val pExtendItemENField = new TextField("pExtendItemEN", "", Field.Store.YES)

  private var doc: Document = null

  private val oldpIdField = new IntField("pId", 0, Field.Store.YES);
  private val oldpNameField = new TextField("pName", "", Field.Store.YES)
  private val oldcreateTimeField = new StringField("createTime", "", Field.Store.YES)
  private val oldsourceKeywordField = new TextField("sourceKeyword", "", Field.Store.YES)

  override def preStart() {
    val mongo = MongoManager()
    productColl = mongo(dinobuydb)("ec_productinformation")
    productInactiveColl = mongo(dinobuydb)("ec_productinactiveinfo")
  }
  def writeDoc(x: DBObject, words: List[SegmentWord], writer: IndexWriter): Boolean = {
    var list: MongoDBList = null
    try {
      if (mvp[Int](x, "qdwproductstatus_int") < 2 && mvp[Boolean](x, "isstopsale_bit") == false
        && x.as[MongoDBList]("ec_productprice").length > 0 && x.as[MongoDBList]("ec_productprice").as[DBObject](0).as[Double]("unitprice_money") > 0) {

        indexCodeField.setStringValue("-1")
        isOneSaleField.setIntValue(-1)
        isAliExpressField.setIntValue(-1)

        businessNameField.setStringValue("")
        pNameRuField.setStringValue("")

        segmentWordRuField.setStringValue("")
        segmentWordBrField.setStringValue("")
        segmentWordEnField.setStringValue("")
        sourceKeywordField.setStringValue("")
        sourceKeywordCNField.setStringValue("")
        productBrandNameField.setStringValue("")
        productTypeNameEnField.setStringValue("")
        //        productTypeNameRuField.setStringValue("")
        productTypeNameBrField.setStringValue("")
        skuOrderField.setIntValue(-1)

        pNameBrField.setStringValue("")
        pNameCnField.setStringValue("")
        createTimeField.setStringValue("")
        //osell
        productTypeIdField.setIntValue(-1)
        isQualityProductField.setIntValue(-1)
        ventureStatusField.setIntValue(-1)
        ventureLevelNewField.setIntValue(-1)

        isTaobaoField.setIntValue(-1)

        productBrandIdField.setIntValue(-1)
        businessBrandField.setStringValue("")

        pExtendItemENField.setStringValue("")
        pExtendItemRUField.setStringValue("")
        pExtendItemBRField.setStringValue("")

        list = x.as[MongoDBList]("ec_productprice")
        doc = new Document()
        pIdField.setIntValue(x.as[Int]("productid_int"));
        val sr = new StringReader(StringUtils.defaultIfBlank(mvp[String](x, "productbrand_nvarchar"), StringUtils.defaultIfBlank(mvp[String](x, "businessbrand_nvarchar"), "")) + ' ' + mvp[String](x, "productaliasname_nvarchar"))
        val ts = englishAnalyzer.tokenStream("", sr)
        ts.reset()
        pNameField.setTokenStream(ts)
        indexCodeField.setStringValue(mvp[String](x, "indexcode_nvarchar"))
        try {
          if (DBService.productTypesEn.getOrElse(mvp[String](x, "indexcode_nvarchar"), "").length > 0) {
            productTypeNameEnField.setStringValue(DBService.productTypesEn.getOrElse(mvp[String](x, "indexcode_nvarchar"), ""))
            doc.add(productTypeNameEnField)
          }
        } catch {
          case e: Exception =>
        }
        try {
          if (DBService.productTypesRu.getOrElse(mvp[String](x, "indexcode_nvarchar"), "").length > 0) {
            productTypeNameRuField.setStringValue(DBService.productTypesRu.getOrElse(mvp[String](x, "indexcode_nvarchar"), ""))
            doc.add(productTypeNameRuField)
          }

        } catch {
          case e: Exception =>
        }
        try {
          if (DBService.productTypesEn.getOrElse(mvp[String](x, "indexcode_nvarchar"), "").length > 0) {
            productTypeNameEnField.setStringValue(DBService.productTypesEn.getOrElse(mvp[String](x, "indexcode_nvarchar"), " "))
            doc.add(productTypeNameBrField)
          }

        } catch {
          case e: Exception =>
        }

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
          val strs = mvp[String](x, "excavatekeywords_nvarchar").split(",")
          if (strs.length > 1) {
            sourceKeywordField.setStringValue(strs(strs.length - 1).trim)
            doc.add(sourceKeywordField)
          }
        } catch {
          case e: Exception =>
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
          ventureLevelNewField.setIntValue(mvp[Int](x, "venturelevelnew_tinyint"))
          doc.add(ventureLevelNewField)
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
          productBrandNameField.setStringValue(mvp[String](x, "productbrand_nvarchar"))
          doc.add(productBrandNameField)
        } catch {
          case e: Exception =>
        }

        try {
          businessBrandField.setStringValue(mvp[String](x, "businessbrand_nvarchar").trim)
          doc.add(businessBrandField)
        } catch {
          case e: Exception =>
        }

        val sku = mv[String](x, "productkeyid_nvarchar")
        if (sku.charAt(0) == 'A') {
          skuOrderField.setIntValue(0)
        } else if (sku.charAt(0) == 'X') {
          skuOrderField.setIntValue(1)
        } else if (sku.charAt(0) == 'T') {
          skuOrderField.setIntValue(2)
        } else {
          skuOrderField.setIntValue(50)
        }
        doc.add(skuOrderField)
        skuField.setStringValue(sku)
        if (words.length > 0) {
          for (x <- words) {
            //log.info("{} == {}",x.sku,sku)
            if (x.sku == sku) {
              //log.info("----------enter words")
              if (x.lang.toLowerCase() == "ru") {
                segmentWordRuField.setStringValue(x.word.replace("|", " ").trim)
                doc.add(segmentWordRuField)
              }
              if (x.lang.toLowerCase() == "en") {
                segmentWordEnField.setStringValue(x.word.replace("|", " ").trim)
                doc.add(segmentWordEnField)
              }
              if (x.lang.toLowerCase() == "pt") {
                segmentWordBrField.setStringValue(x.word.replace("|", " ").trim)
                doc.add(segmentWordBrField)
              }
              sourceKeywordCNField.setStringValue(DBService.tag.tag(x.cn.trim))
              pNameCnField.setStringValue(DBService.tag.tag(x.titlecn.trim))
            }
          }
          doc.add(sourceKeywordCNField)
          doc.add(pNameCnField)
        }

        list = x.as[MongoDBList]("ec_productlanguage")
        for (y <- 0 until list.length) {
          if (list.as[DBObject](y).as[String]("language_nvarchar").toLowerCase() == "ru") {
            pNameRuField.setStringValue(list.as[DBObject](y).as[String]("producttitle_nvarchar").trim)
            doc.add(pNameRuField)
          }
          if (list.as[DBObject](y).as[String]("language_nvarchar").toLowerCase() == "br") {
            pNameBrField.setStringValue(list.as[DBObject](y).as[String]("producttitle_nvarchar").trim)
            doc.add(pNameBrField)
          }
        }

        try {
          val sbEn = new StringBuffer()
          val sbRu = new StringBuffer()
          val sbBr = new StringBuffer()
          val items = productInactiveColl.findOne(MongoDBObject("productid_int" -> x.as[Int]("productid_int")), fields1)
          val lst = items.toList
          if (lst.length > 0) {

            val ei = lst(0)
            list = ei.as[MongoDBList]("ec_productextendgroup")
            for (y <- 0 until list.length) {
              val exItems = list.as[DBObject](y).as[MongoDBList]("ec_productextenditem")
              for (ii <- 0 until exItems.length) {
                sbEn.append(exItems.as[DBObject](ii).as[String]("itemvalueeng_nvarchar")).append(' ')
                val otherValue = exItems.as[DBObject](ii).as[MongoDBList]("itemvaluelanguage")
                for (iii <- 0 until otherValue.length) {
                  if (otherValue.as[DBObject](iii).as[String]("language_nvarchar").toLowerCase == "ru") {
                    sbRu.append(otherValue.as[DBObject](iii).as[String]("itemvalue_nvarchar")).append(' ')
                  }
                  if (otherValue.as[DBObject](iii).as[String]("language_nvarchar").toLowerCase == "br") {
                    sbBr.append(otherValue.as[DBObject](iii).as[String]("itemvalue_nvarchar")).append(' ')
                  }
                }
              }
            }
            pExtendItemENField.setStringValue(sbEn.toString())
            pExtendItemRUField.setStringValue(sbRu.toString())
            pExtendItemBRField.setStringValue(sbBr.toString())
            doc.add(pExtendItemBRField)
            doc.add(pExtendItemENField)
            doc.add(pExtendItemRUField)
          }

        } catch {
          case e: Exception => throw e
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
      case e: Exception => log.error("index item fail,productId {};{}", x.as[Int]("productid_int"), e); e.printStackTrace(); throw e

      //failCount += 1
    }
  }
  def receive = {
    case msg: OldIndexIncremetionalTaskMessage => {
      val timeFile = new File(oldDir + "/time")
      val from = new Date(timeFile.lastModified())
      var successCount: Int = 0

      var conn: Connection = null
      var stmt: PreparedStatement = null
      var rs: ResultSet = null
      val writer = IndexWriteManager.getOldIncIndexWriter(null, null)
      var maxDate: Date = from
      try {
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

        val sql = "select top 100 productid_int as pId,ProductAliasName_nvarchar as alias,ExcavateKeyWords_nvarchar as keyword,CreateTime_datetime as date from ec_product with(nolock) where CreateTime_datetime > '" + sdf.format(from) + ".999'"
        log.info("old inc sql:{} ", sql)
        conn = DBService.dataSource.getConnection()
        stmt = conn.prepareStatement(sql)
        rs = stmt.executeQuery()

        while (rs.next()) {
          val doc = new Document
          try {
            oldcreateTimeField.setStringValue(DateTools.dateToString(rs.getTimestamp("date"), DateTools.Resolution.MINUTE))
            doc.add(oldcreateTimeField)
          } catch {
            case e: Exception =>
          }

          try {
            oldpIdField.setIntValue(rs.getInt("pId"));
            doc.add(oldpIdField)
          } catch {
            case e: Exception =>
          }

          try {
            oldpNameField.setStringValue(rs.getString("alias"));
            doc.add(oldpNameField)
          } catch {
            case e: Exception =>
          }

          try {
            val strs = rs.getString("keyword").split(",")
            if (strs.length > 1) {
              oldsourceKeywordField.setStringValue(strs(strs.length - 1))
              doc.add(oldsourceKeywordField)
            }
          } catch {
            case e: Exception =>
          }
          //log.info("item date {},maxDate {}",sdf.format(rs.getTimestamp("date")),sdf.format(maxDate))
          if (rs.getTimestamp("date").after(maxDate)) {
            //log.info("set maxDate")
            maxDate = rs.getTimestamp("date")
          }
          writer.addDocument(doc)
          successCount += 1
        }
      } catch {
        case e: Exception => {
          e.printStackTrace()
          /*if(rs != null) rs.close()
          if(stmt != null) stmt.close()
          if(conn != null) conn.close()*/
        }
      } finally {
        if (rs != null) rs.close()
        if (stmt != null) stmt.close()
        if (conn != null) conn.close()
      }

      timeFile.delete()
      timeFile.createNewFile()
      timeFile.setLastModified(maxDate.getTime)
      writer.commit()
      log.info("index old incremental {}", successCount)
      context.system.scheduler.scheduleOnce(10 seconds) {
        self ! OldIndexIncremetionalTaskMessage("", null)
      }
    }
    case msg: IndexIncremetionalTaskMessage => {
      log.info("receive incrementional index message")
      val time1 = System.currentTimeMillis();

      val incPath = Util.getIncrementalPath(msg.name, msg.date)
      val timeFile = new File(workDir + "/" + incPath + "/time")
      //val lastId:Int = Integer.valueOf(Source.fromFile(timeFile).getLines().next())
      val from = new Date(timeFile.lastModified())
      //val to = new Date
      var successCount = 0
      var failCount = 0
      var skipCount = 0
      //val q = "ec_product.createtime_datetime" $gte from $lt to
      val q = "ec_product.createtime_datetime" $gte from

      //val q = "productid_int" $gte from $lt to
      val writer = IndexWriteManager.getIncIndexWriter(msg.name, msg.date)
      log.info("reading data")
      val items: MongoCursor = productColl.find(q, fields).limit(2000)
      val lst = items.toList
      log.info("readed data")
      //val words = getSegmentWords(lst)
      //log.info("readed data1")
      log.info("index inc item {}", items.size)
      var maxDate: Date = from
      for (x <- lst) {
        try {
          if (writeDoc(x, List(), writer)) successCount += 1 else skipCount += 1
          if (mvp[Date](x, "createtime_datetime").after(maxDate)) {
            maxDate = mvp[Date](x, "createtime_datetime")
          }
        } catch {
          case e: Exception => failCount += 1
        }

      }
      log.info("readed data2")
      /*for (x <- 1 to 100) {
        writeDoc(null, writer)
      }*/
      writer.commit();
      //TODO 应该时间排序取最后一个记录的时间，作为lastupdatetime
      timeFile.delete()
      timeFile.createNewFile()
      timeFile.setLastModified(maxDate.getTime)
      /*val wf:FileWriter = new FileWriter(timeFile)
      wf.write(maxId.toString)
      wf.close()*/
      val time2 = System.currentTimeMillis();
      log.info("index incremental spent {}", time2 - time1)
      val consoleRoot = context.actorFor(Util.getConsoleRootAkkaURL)
      log.info("index incremental {}/{}", successCount, items.size)
      consoleRoot ! CompleteIncIndexTask(msg.name, msg.date, successCount, failCount, skipCount)
    }
    case msg: IndexTaskMessage => {
      try {
        //log.info("recevie indextaskmessage {}",msg.date)
        val time1 = System.currentTimeMillis()
        val writer = IndexWriteManager.getIndexWriter(msg.name, msg.date)
        //val now:Date = msg.date
        var successCount: Int = 0
        var failCount: Int = 0
        var skipCount: Int = 0
        //read mongo data
        //var time3 = System.currentTimeMillis()
        //val items: MongoCursor = productColl.find("ec_product.createtime_datetime" $lt now, fields).skip(Integer.valueOf((msg.seq * 2).toString())).limit(2000)
        var q: DBObject = ("productid_int" $gte msg.minId $lte msg.maxId) ++ ("ec_product.qdwproductstatus_int" $lt 2) ++ ("ec_product.isstopsale_bit" -> false) ++ ("ec_productprice.unitprice_money" $gt 0)
        val items: MongoCursor = productColl.find(q, fields, 0, msg.batchSize)
        val lst = items.toList
        //var time4 = System.currentTimeMillis()
        //log.info("load items {}", time4 - time3)

        //read segmentWord
        //time3 = System.currentTimeMillis()
        val words = getSegmentWords(lst)
        //time4 = System.currentTimeMillis()
        //log.info("load words {}", time4 - time3)

        //log.info("find items {}",time4 - time3)

        //log.info("spent {} millisecond in finding items {}",time2 - time1,items.size)
        for (x <- lst) {
          /*if(!b) b = true else log.info("load item {}",time3 -time4)
          time3 = System.currentTimeMillis()*/
          try {
            if (writeDoc(x, words, writer)) successCount += 1 else skipCount += 1
            //if(writeDoc(x,List(), writer)) successCount += 1 else skipCount += 1
          } catch {
            case e: Exception => failCount + 1
          }
          //time4 = System.currentTimeMillis()
        }
        /*for (x <- 1 to 100) {
          writeDoc(null, writer)
        }*/

        val consoleRoot = context.actorFor(Util.getConsoleRootAkkaURLFromMyConfig)
        consoleRoot ! CompleteSubTask(msg.name, msg.date, msg.seq, successCount, failCount, skipCount)
        val time2 = System.currentTimeMillis()
        items.close()
        val arr = new Array[Int](5)
        arr(0) = Integer.valueOf((time2 - time1).toString)
        arr(1) = successCount
        arr(2) = failCount
        arr(3) = skipCount
        arr(4) = items.size
        //log.info("index time {} success {} fail {} skip {} total {}", arr);
      } catch {

        case e: Exception => log.error("{}", e); e.printStackTrace()
      }
    }
  }
  def getSegmentWords(items: List[DBObject]): List[SegmentWord] = {

    val list = ListBuffer[SegmentWord]()
    if (items.length > 0) {

      var conn: Connection = null
      var stmt: Statement = null
      var rs: ResultSet = null
      try {
        conn = DBService.dataSource.getConnection()
        stmt = conn.createStatement()
        val sb = new StringBuffer()
        //sb.append("select ProductTitleCN_nvarchar as titlecn, ProductKeyID_nvarchar as sku,SearchKeyWordCN_nvarchar as wordcn,SegmentWord_nvarchar as word,LanguageCode_nvarchar as lang from QDW_TB_ProductTitleSegmentWord WITH (NOLOCK) where ProductKeyID_nvarchar in (")
        sb.append("select ProductTitleCN_nvarchar as titlecn, ProductKeyID_nvarchar as sku,SearchKeyWordCN_nvarchar as wordcn,SegmentWord_nvarchar as word,LanguageCode_nvarchar as lang from QDW_TB_ProductTitleSegmentWord where ProductKeyID_nvarchar in (")
        for (x <- items) {
          sb.append('\'').append(mv[String](x, "productkeyid_nvarchar")).append("',")
        }
        val sql = sb.substring(0, sb.length() - 1) + ")"
        rs = stmt.executeQuery(sql)
        while (rs.next()) {
          list += SegmentWord(rs.getString("sku"), rs.getString("word"), rs.getString("lang"), rs.getString("wordcn"), rs.getString("titlecn"))
          //println(SegmentWord(rs.getString("sku"), rs.getString("word"), rs.getString("lang"), rs.getString("wordcn"), rs.getString("titlecn")))
        }
      } catch {
        case e: Exception => {
          e.printStackTrace()
        }
      } finally {
        if (rs != null) rs.close()
        if (stmt != null) stmt.close()
        if (conn != null) conn.close()
        //println("close Connection")
      }

    }
    list.toList
  }
}