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
      indexRootActor ! IndexIncremetionalTaskMessage(msg.name, msg.date)
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

    var maxId = 0
    var count = 0
    try {
      conn = DBMssql.ds.getConnection
      var sql = "SELECT max(SeqID_int) as max,count(SeqID_int) as count from EC_IndexLifeProduct with(nolock) where IsEnabled_int = 1"
      stmt = conn.createStatement()
      rs = stmt.executeQuery(sql)
      while(rs.next()){
        maxId = rs.getInt("max")
        count = rs.getInt("count")
      }

      sql = s"select SeqID_int from EC_IndexLifeProduct with(nolock) where SeqID_int <= $maxId and IsEnabled_int =1"
      rs.setFetchSize(1000)
      rs = stmt.executeQuery(sql)
      val total: Long = count / ddProductIndexSize + 1

      val ids:ListBuffer[Int] = new ListBuffer[Int]
      var j = 0
      while(rs.next() && rs.getRow <= count){
        ids += rs.getInt(1)
        if(ids.length % ddProductIndexSize == 0){
          //记录批次
          j += 1
          sendLift(Constants.DD_PRODUCT_LIFTSTYLE, now, j, ids, total, ddProductIndexSize,msg.ddProductIndex)
          ids.clear()
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
      var sql = "SELECT max(keyid_int) as max,count(keyid_int) as count from QDW_AttributeAndValueDictionary with(nolock)"
      stmt = conn.createStatement()
      rs = stmt.executeQuery(sql)
      var maxId:Int = 0
      var count:Int = 0
      while(rs.next){
        maxId = rs.getInt("max")
        count = rs.getInt("count")
      }

      sql = s"SELECT keyid_int from QDW_AttributeAndValueDictionary with(nolock) where keyid_int <= $maxId"
      rs.setFetchSize(1000)
      val total: Long = count / ddProductIndexSize + 1

      val ids:ListBuffer[Int] = new ListBuffer[Int]
      var j = 0
      while(rs.next() && rs.getRow <= count) {
        ids += rs.getInt(1)
        if(ids.length % ddProductIndexSize == 0){
          //记录批次
          j += 1
          sendAttr(Constants.DD_PRODUCT_ATTRIBUTE, now, j, ids, total, ddProductIndexSize,msg.ddProductIndex)
          ids.clear()
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
    if(seq <= total){
      indexRootActor ! IndexTaskMessageDD(name, runId, seq, ids,total,batchSize)
      indexRootManager ! CreateSubTask(name, runId, total)  
      log.info("send dd index msg {}",seq)
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

    val now = new Date()
    log.info("create index {}",now)
    var conn: Connection = null
    var stmt: Statement = null
    var rs: ResultSet = null
    var maxId: Int = 0
    var totalCount:Int = 0
    try {
      conn = DDService.dataSource.getConnection()
      stmt = conn.createStatement()
      var sql = "select max(productid_int),count(productid_int) from EC_Product ec with(nolock) where " +
                "ec.VentureStatus_tinyint <> 3 and ec.ProductPrice_money > 0 and isnull(ec.VentureLevelNew_tinyint,0) = 0 " +
                "and ec.QDWProductStatus_int = 0 and ec.VentureStatus_tinyint <> 4 "
      rs = stmt.executeQuery(sql)
      if (rs.next()) {
        maxId = rs.getInt(1)
        totalCount = rs.getInt(2)
      }

      /*if(current.configuration.getBoolean("debugIndex").get && maxId > current.configuration.getInt("debugItemCount").get) {
        maxId = current.configuration.getInt("debugItemCount").get
      }*/
      //with(nolock)会引起脏读，productid_int <= $maxId条件避免由其他ddl引起的脏读
      sql = s"""select productid_int from EC_Product ec with(nolock) where
                ec.VentureStatus_tinyint <> 3 and ec.ProductPrice_money > 0
                and isnull(ec.VentureLevelNew_tinyint,0) = 0
                and ec.QDWProductStatus_int = 0 and ec.VentureStatus_tinyint <> 4 and productid_int <= $maxId"""
      rs.setFetchSize(1000)
      rs = stmt.executeQuery(sql)

      var total: Long = totalCount / ddProductIndexSize + 1
      //debug模式任务分发
      if(current.configuration.getBoolean("debugIndex").get && total > current.configuration.getInt("debugTaskCount").get){
        total = current.configuration.getInt("debugTaskCount").get
      }

      log.info("maxId=========={}",maxId)
      log.info("totalCount====={}",totalCount)

      val ids:ListBuffer[Int] = new ListBuffer[Int]
      var j = 0
      while(rs.next() && rs.getRow <= totalCount){
        ids += rs.getInt(1)
        if(ids.length % ddProductIndexSize == 0){
          //记录批次
          j += 1
          sendMsgDD(Constants.DD_PRODUCT_FORDB, now, j, ids, total, ddProductIndexSize)
          ids.clear()
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

  def resolutionMessage() = {
    val dbMssql:JdbcTemplate = new JdbcTemplate(DBMssql.ds)
    dbMssql.setFetchSize(1000)
    val sql:String = "select k.ProductID_int from EC_Product k where k.IndexCode_nvarchar = '00220009'"
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
