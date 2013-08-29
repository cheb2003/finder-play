package my.finder.console.actor

import akka.actor._
import my.finder.common.message._


import com.mongodb.casbah.Imports._
import my.finder.common.util.{Constants, Util}
import my.finder.console.service.DBMssql
import java.util.Date



import my.finder.console.service.{MongoManager, IndexManage}



import play.api.Play.current
import scala.util.control.Breaks._
import my.finder.index.service.DDService
import java.sql.{ResultSet, Statement, Connection}
import scala.collection.mutable.ListBuffer
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.support.rowset.SqlRowSet

/**
 *
 *
 */
case class  ProductAttr(val id:Int,value:String,name:String)

class PartitionIndexTaskActor extends Actor with ActorLogging {
  var mongoClient:MongoClient = MongoManager()
  val dinobuydb = current.configuration.getString("dinobuydb").get
  val ddProductIndexSize: Int = Integer.valueOf(current.configuration.getString("indexBatchSize").get)
  //val dbProductIndexSize: Int = Integer.valueOf(current.configuration.getString("indexBatchSize").get)
  var productColl:MongoCollection = null
  //TODO 改回来 var q:DBObject = ("ec_productprice.unitprice_money" $gt 0) ++ ("ec_product.isstopsale_bit" -> false)
  val fields = MongoDBObject("productid_int" -> 1)
  //val indexActor = context.system.actorOf(Props[IndexDDProductActor].withRouter(FromConfig()),"node")
  val indexRootActor = context.actorFor(Util.getIndexRootAkkaURL)
  val indexRootManager = context.actorFor(Util.getIndexManagerAkkaURL)

  override def preStart() {
    mongoClient = MongoManager()
    productColl = mongoClient(dinobuydb)("ec_productinformation")
  }

  def receive = {
    case msg: IndexIncremetionalTaskMessage => {
      val i = IndexManage.get(Constants.DD_PRODUCT)
      indexRootActor ! IndexIncremetionalTaskMessage(i.name, i.using)
    }

    case msg: OldIndexIncremetionalTaskMessage => {
      indexRootActor ! OldIndexIncremetionalTaskMessage("", null)
    }
    //分发子任务
    case msg: PartitionIndexTaskMessage => {
      if (msg.name == Constants.DD_PRODUCT) {
        partitionDDProduct()
      }
      if (msg.name == Constants.DD_PRODUCT_FORDB) {
        partitionDDProductForDB()
      }
    }
    case msg:PartitionIndexAttributesTaskMessage => {
      partitionAttributes(msg)
    }

    case msg:PartitionIndexLiftStyleTaskMessage => {
      partitionLiftStyle(msg)
    }

    case msg: ResolutionMessage => {
      resolutionMessage()
    }
  }

  def partitionLiftStyle(msg:PartitionIndexLiftStyleTaskMessage) = {
    val now = new Date()
    var conn: Connection = null
    var stmt: Statement = null
    var rs: ResultSet = null
    try {
      conn = DBMssql.ds.getConnection
      val sql = "SELECT min(SeqID_int) as min,max(SeqID_int) as max from EC_IndexLifeProduct"
      stmt = conn.createStatement()
      rs = stmt.executeQuery(sql)
      rs.next
      var minId = rs.getInt("min")
      val maxId = rs.getInt("max")
      if(current.configuration.getBoolean("debugIndex").get) {
        minId = maxId - current.configuration.getInt("debugItemCount").get
      }

      val totalCount: Long = maxId - minId + 1
      val total: Long = totalCount / ddProductIndexSize + 1

      var id = minId
      var j = 0
      breakable {
        while(true){
          if(id >= maxId){
            break
          }
          j += 1
          sendLift(Constants.DD_PRODUCT_LIFTSTYLE, now, j, id, id + ddProductIndexSize - 1, total, ddProductIndexSize,msg.ddProductIndex)
          id += ddProductIndexSize
        }
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      DBMssql.colseConn(conn,stmt,rs)
    }


  }

  def partitionAttributes(msg:PartitionIndexAttributesTaskMessage) = {
    val now = new Date()
    var conn: Connection = null
    var stmt: Statement = null
    var rs: ResultSet = null
    try { 
      conn = DBMssql.ds.getConnection
      val sql = "SELECT min(keyid_int) as min,max(keyid_int) as max from QDW_AttributeAndValueDictionary"
      stmt = conn.createStatement()
      rs = stmt.executeQuery(sql)
      var minId:Int = 0
      var maxId:Int = 0
      while(rs.next){
        minId = rs.getInt("min")
        maxId = rs.getInt("max")
        if(current.configuration.getBoolean("debugIndex").get) {
          minId = maxId - current.configuration.getInt("debugItemCount").get
        }
      }


      val totalCount: Long = maxId - minId + 1
      val total: Long = totalCount / ddProductIndexSize + 1

      var i = 0
      var id = minId
      var j = 0
      breakable {
        while(true){
          if(id > maxId){
            break
          }
          j += 1
          sendAttr(Constants.DD_PRODUCT_ATTRIBUTE, now, j, id, id + ddProductIndexSize - 1, total, ddProductIndexSize,msg.ddProductIndex)
          id += ddProductIndexSize
        }
      }
    } catch {
      case e: Exception => //logger.error("{}",e)
    } finally {
      DBMssql.colseConn(conn,stmt,rs)
    }
    

  }


  private def sendMsg(name: String, runId: Date, seq: Long,minId:Int, maxId:Int, total: Long, batchSize:Int) {
    //println("----------------send message " + seq)
    indexRootActor ! IndexTaskMessage(name, runId, seq, minId, maxId,batchSize)
    indexRootManager ! CreateSubTask(name, runId, total)
  }

  private def sendAttr(name: String, runId: Date, seq: Long,minId:Int, maxId:Int, total: Long, batchSize:Int,ddProductIndex:String) {
    //println("----------------send message " + seq)
    indexRootActor ! IndexAttributeTaskMessage(name, runId, seq, minId, maxId,batchSize,ddProductIndex)
    indexRootManager ! CreateSubTask(name, runId, total)
  }

  private def sendLift(name: String, runId: Date, seq: Long,minId:Int, maxId:Int, total: Long, batchSize:Int,ddProductIndex:String) {
    indexRootActor ! IndexUnitLiftStyleTaskMessage(name, runId, seq, minId, maxId,batchSize,ddProductIndex)
    indexRootManager ! CreateSubTask(name, runId, total)
  }

  private def sendMsgDD(name: String, runId: Date, seq: Long, ids:ListBuffer[Int], total: Long, batchSize:Int) {
    if(current.configuration.getBoolean("debugIndex").get && seq < current.configuration.getInt("debugTaskCount").get) {
      indexRootActor ! IndexTaskMessageDD(name, runId, seq, ids,current.configuration.getInt("debugTaskCount").get,batchSize)
      indexRootManager ! CreateSubTask(name, runId, current.configuration.getInt("debugTaskCount").get)
    } else {
      indexRootActor ! IndexTaskMessageDD(name, runId, seq, ids,total,batchSize)
      indexRootManager ! CreateSubTask(name, runId, total)
    }
    
  }
  def partitionDDProduct() = {
    val now = new Date()

    log.info("create index {}",now)

    val minItem = productColl.find().sort(MongoDBObject("productid_int" -> 1)).limit(1)
    val maxItem = productColl.find().sort(MongoDBObject("productid_int" -> -1)).limit(1)
    val maxId = maxItem.next().as[Int]("productid_int")
    val minId = if(current.configuration.getBoolean("debugIndex").get) {
      maxId - current.configuration.getInt("debugItemCount").get
    } else {
      minItem.next().as[Int]("productid_int")
    }


    val totalCount: Long = maxId - minId + 1
    val total: Long = totalCount / ddProductIndexSize + 1
    log.info("minId=========={}",minId)
    log.info("maxId=========={}",maxId)
    var id = minId
    var j = 0
    breakable {
      while(true){
        if(id >= maxId){
          break
        }
        j += 1
        sendMsg(Constants.DD_PRODUCT, now, j, id, id + ddProductIndexSize - 1, total, ddProductIndexSize)
        id += ddProductIndexSize
        Thread.sleep(100)
      }
    }
  }

  def partitionDDProductForDB() = {
    log.info("执行到这里partitionDDProductForDB来了")
    val now = new Date()
    log.info("create index {}",now)
    var conn: Connection = null
    var stmt: Statement = null
    var rs: ResultSet = null
    var maxId:Int = 0
    var minId:Int = 0
    var totalCount:Int = 0
    try {
      conn = DDService.dataSource.getConnection()
      stmt = conn.createStatement()
      var sql = "select max(ProductID_int),min(ProductID_int),count(productid_int) from EC_Product ec with(nolock) where " +
                "ec.VentureStatus_tinyint <> 3 and ec.ProductPrice_money > 0 and isnull(ec.VentureLevelNew_tinyint,0) = 0 " +
                "and ec.QDWProductStatus_int = 0 and ec.VentureStatus_tinyint <> 4 "
      rs = stmt.executeQuery(sql)
      if (rs.next()) {
        maxId = rs.getInt(1)
        minId = rs.getInt(2)
        totalCount = rs.getInt(3)
      }
      sql = s"""select productid_int from EC_Product ec with(nolock) where
                ec.VentureStatus_tinyint <> 3 and ec.ProductPrice_money > 0
                and isnull(ec.VentureLevelNew_tinyint,0) = 0
                and ec.QDWProductStatus_int = 0 and ec.VentureStatus_tinyint <> 4
                and productid_int between $minId and $maxId"""
      rs = stmt.executeQuery(sql)
      /*if(current.configuration.getBoolean("debugIndex").get) {
        minId = maxId - current.configuration.getInt("debugItemCount").get
      }*/


      val total: Long = totalCount / ddProductIndexSize + 1
      log.info("minId=========={}",minId)
      log.info("maxId=========={}",maxId)
      log.info("totalCount====={}",totalCount)
      val ids:ListBuffer[Int] = new ListBuffer[Int]
      var j = 0
      while(rs.next()){
        ids += rs.getInt(1)
        if(ids.length == 200){
          j += 1
          sendMsgDD(Constants.DD_PRODUCT_FORDB, now, j, ids, total, ddProductIndexSize)
          ids.clear()
          log.info("send dd index msg {}",j)
        }
      }
      if (ids.length > 0) {
        j += 1
        sendMsgDD(Constants.DD_PRODUCT_FORDB, now, j, ids, total, ddProductIndexSize)
      }
      /*var id = minId
      var j = 0
      breakable {
        while(true){
          if(id >= maxId){
            break
          }
          j += 1
          sendMsgDD(Constants.DD_PRODUCT_FORDB, now, j, id, id + ddProductIndexSize - 1, total, ddProductIndexSize)
          id += ddProductIndexSize
        }
      }*/
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      DBMssql.colseConn(conn,stmt,rs)
    }

  }

  def resolutionMessage() = {
    val dbMssql:JdbcTemplate = new JdbcTemplate(DBMssql.ds)
    dbMssql.setFetchSize(1000)
    val sql:String = "select k.ProductID_int from EC_Product k where k.IndexCode_nvarchar in ('1001','1005')"
    val rs:SqlRowSet = dbMssql.queryForRowSet(sql)
    val pIds:ListBuffer[Int] = new ListBuffer[Int]()
    while ( rs.next() ){
      val pid = rs.getInt("ProductID_int")
      pIds += pid
      if ( pIds.length == ddProductIndexSize ){
        sendResolution( Constants.DD_PRODUCT_RESOLUTION, new Date(), ddProductIndexSize, pIds )
        pIds.clear()
      }
    }
    if ( pIds.length > 0 ){
      sendResolution( Constants.DD_PRODUCT_RESOLUTION, new Date(), ddProductIndexSize, pIds )
    }
  }

  private def sendResolution(name: String, date: Date, ddProductIndexSize: Int, ids:ListBuffer[Int]) {
    indexRootActor ! IndexResolutionMessage(name,date,ddProductIndexSize,ids)
    indexRootManager ! CreateSubTask(name, date, ddProductIndexSize)
  }
}
