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
  }

  def partitionLiftStyle(msg:PartitionIndexLiftStyleTaskMessage) = {
    val now = new Date()
    var conn: Connection = null
    var stmt: Statement = null
    var rs: ResultSet = null

    var minId = 0
    var count = 0
    try {
      conn = DBMssql.ds.getConnection
      var sql = "SELECT min(SeqID_int) as min,count(SeqID_int) as count from EC_IndexLifeProduct with(nolock)"
      stmt = conn.createStatement()
      rs = stmt.executeQuery(sql)
      while(rs.next()){
        minId = rs.getInt("min")
        count = rs.getInt("count")
      }
      if(current.configuration.getBoolean("debugIndex").get && current.configuration.getInt("debugIndex").get > minId) {
        minId = current.configuration.getInt("debugItemCount").get
      }

      sql = s"select SeqID_int from EC_IndexLifeProduct with(nolock) where SeqID_int >= $minId and IsEnabled_int = 1"
      rs.setFetchSize(1000)
      rs = stmt.executeQuery(sql)
      val total: Long = count / ddProductIndexSize + 1

      val ids:ListBuffer[Int] = new ListBuffer[Int]
      var j = 0
      while(rs.next()){
        ids += rs.getInt(1)
        if(ids.length % ddProductIndexSize == 0){
          //记录批次
          j += 1
          sendLift(Constants.DD_PRODUCT_LIFTSTYLE, now, j, ids, total, ddProductIndexSize,msg.ddProductIndex)
          log.info("send dd liftstyle msg {}",j)
        }
      }
      if (ids.length % ddProductIndexSize > 0) {
        j += 1
        sendLift(Constants.DD_PRODUCT_LIFTSTYLE, now, j, ids, total, ddProductIndexSize,msg.ddProductIndex)
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
      var sql = "SELECT min(keyid_int) as min,count(keyid_int) as count from QDW_AttributeAndValueDictionary with(nolock)"
      stmt = conn.createStatement()
      rs = stmt.executeQuery(sql)
      var minId:Int = 0
      var count:Int = 0
      while(rs.next){
        minId = rs.getInt("min")
        count = rs.getInt("count")
      }
      if(current.configuration.getBoolean("debugIndex").get && current.configuration.getInt("debugIndex").get > minId) {
        minId = current.configuration.getInt("debugItemCount").get
      }

      sql = s"SELECT keyid_int from QDW_AttributeAndValueDictionary with(nolock) where keyid_int >= $minId"
      rs.setFetchSize(1000)
      val total: Long = count / ddProductIndexSize + 1

      val ids:ListBuffer[Int] = new ListBuffer[Int]
      var j = 0
      while(rs.next()){
        ids += rs.getInt(1)
        if(ids.length % ddProductIndexSize == 0){
          //记录批次
          j += 1
          sendAttr(Constants.DD_PRODUCT_ATTRIBUTE, now, j, ids, total, ddProductIndexSize,msg.ddProductIndex)
          log.info("send dd attribute msg {}",j)
        }
      }
      if (ids.length % ddProductIndexSize > 0) {
        j += 1
        sendAttr(Constants.DD_PRODUCT_ATTRIBUTE, now, j, ids, total, ddProductIndexSize,msg.ddProductIndex)
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

  private def sendAttr(name: String, runId: Date, seq: Long, ids:ListBuffer[Int],total: Long, batchSize:Int,ddProductIndex:String) {
    //println("----------------send message " + seq)
    indexRootActor ! IndexAttributeTaskMessage(name, runId,seq,ids,batchSize,ddProductIndex)
    indexRootManager ! CreateSubTask(name, runId, total)
  }

  private def sendLift(name: String, runId: Date, seq: Long, ids:ListBuffer[Int],total: Long, batchSize:Int,ddProductIndex:String) {
    indexRootActor ! IndexUnitLiftStyleTaskMessage(name, runId,seq,ids,batchSize,ddProductIndex)
    indexRootManager ! CreateSubTask(name, runId, total)
  }

  private def sendMsgDD(name: String, runId: Date, seq: Long, ids:ListBuffer[Int], total: Long, batchSize:Int) {
    indexRootActor ! IndexTaskMessageDD(name, runId, seq, ids,total,batchSize)
    indexRootManager ! CreateSubTask(name, runId, total)
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

    val now = new Date()
    log.info("create index {}",now)
    var conn: Connection = null
    var stmt: Statement = null
    var rs: ResultSet = null
    var minId: Int = 0
    var totalCount:Int = 0
    try {
      conn = DDService.dataSource.getConnection()
      stmt = conn.createStatement()
      var sql = "select min(productid_int),count(productid_int) from EC_Product ec with(nolock) where " +
                "ec.VentureStatus_tinyint <> 3 and ec.ProductPrice_money > 0 and isnull(ec.VentureLevelNew_tinyint,0) = 0 " +
                "and ec.QDWProductStatus_int = 0 and ec.VentureStatus_tinyint <> 4 "
      rs = stmt.executeQuery(sql)
      if (rs.next()) {
        minId = rs.getInt(1)
        totalCount = rs.getInt(2)
      }

      if(current.configuration.getBoolean("debugIndex").get && current.configuration.getInt("debugItemCount").get > minId) {
        minId = current.configuration.getInt("debugItemCount").get
      }

      sql = s"""select productid_int from EC_Product ec with(nolock) where
                ec.VentureStatus_tinyint <> 3 and ec.ProductPrice_money > 0
                and isnull(ec.VentureLevelNew_tinyint,0) = 0
                and ec.QDWProductStatus_int = 0 and ec.VentureStatus_tinyint <> 4 and productid_int >= $minId"""
      rs.setFetchSize(1000)
      rs = stmt.executeQuery(sql)
      /*if(current.configuration.getBoolean("debugIndex").get) {
        minId = maxId - current.configuration.getInt("debugItemCount").get
      }*/


      
      var total: Long = totalCount / ddProductIndexSize + 1

      if(current.configuration.getBoolean("debugIndex").get && total > current.configuration.getInt("debugTaskCount").get){
        total = current.configuration.getInt("debugTaskCount").get
      }

      log.info("minId=========={}",minId)
      log.info("totalCount====={}",totalCount)

      val ids:ListBuffer[Int] = new ListBuffer[Int]
      var j = 0
      while(rs.next()){
        ids += rs.getInt(1)
        if(ids.length % ddProductIndexSize == 0){
          //记录批次
          j += 1
          sendMsgDD(Constants.DD_PRODUCT_FORDB, now, j, ids, total, ddProductIndexSize)
          ids.clear()
          log.info("send dd index msg {}",j)
        }
      }
      if (ids.length % ddProductIndexSize> 0) {
        j += 1
        sendMsgDD(Constants.DD_PRODUCT_FORDB, now, j, ids, total, ddProductIndexSize)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      DBMssql.colseConn(conn,stmt,rs)
    }

  }
}
