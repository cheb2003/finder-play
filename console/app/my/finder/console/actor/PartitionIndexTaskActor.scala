package my.finder.console.actor

import akka.actor._
import my.finder.common.message._


import com.mongodb.casbah.Imports._
import my.finder.common.util.{Constants}

import java.util.Date

import java.lang


import my.finder.console.service.{MongoManager, IndexManage}
import scala.collection.mutable.ListBuffer

import my.finder.common.message.IndexIncremetionalTaskMessage
import my.finder.common.message.IndexTaskMessage
import my.finder.common.message.CreateSubTask
import my.finder.common.message.PartitionIndexTaskMessage
import akka.routing.RoundRobinRouter

import play.api.Play.current
/**
 *
 *
 */
class PartitionIndexTaskActor extends Actor with ActorLogging {
  var mongoClient:MongoClient = MongoManager()
  val dinobuydb = current.configuration.getString("dinobuydb").get
  val ddProductIndexSize: Int = Integer.valueOf(current.configuration.getString("indexBatchSize").get)
  var productColl:MongoCollection = null
  //TODO 改回来 var q:DBObject = ("ec_productprice.unitprice_money" $gt 0) ++ ("ec_product.isstopsale_bit" -> false)
  val fields = MongoDBObject("productid_int" -> 1)
  //val indexActor = context.system.actorOf(Props[IndexDDProductActor].withRouter(FromConfig()),"node")
  val indexRootActor = context.actorFor("akka://index@127.0.0.1:2554/user/root")
  //val unit = context.actorOf(Props[PartitionIndexTaskUnitActor].withRouter(RoundRobinRouter(nrOfInstances = 10)))
  val indexRootManager = context.actorFor("akka://console@127.0.0.1:2552/user/root/indexManager")
  //val mergeIndex = context.actorOf(Props[MergeIndexActor],"mergeIndex")


  override def preStart() {
    mongoClient = MongoManager()
    productColl = mongoClient(dinobuydb)("ec_productinformation")
  }

  def receive = {
    case msg: IndexIncremetionalTaskMessage => {
      val i = IndexManage.get(Constants.DD_PRODUCT)
      indexRootActor ! IndexIncremetionalTaskMessage(i.name, i.using)
    }
    //分发子任务
    case msg: PartitionIndexTaskMessage => {
      if (msg.name == Constants.DD_PRODUCT) {
        partitionDDProduct()
      }
    }
  }
  private def sendMsg(name: String, runId: Date, seq: Long,ids:ListBuffer[Int], total: Long) {
    indexRootActor ! IndexTaskMessage(Constants.DD_PRODUCT, runId, seq,ids)
    indexRootManager ! CreateSubTask(name, runId, total)
  }

  def partitionDDProduct() = {
    val now = new Date()
    log.info("create index {}",now)
    val set:ListBuffer[Int] = new ListBuffer[Int]
    var i = 0
    var j = 0
    val minItem = productColl.find().sort(MongoDBObject("productid_int" -> 1)).limit(1)
    val maxItem = productColl.find().sort(MongoDBObject("productid_int" -> -1)).limit(1)
    val minId = minItem.next().as[Int]("productid_int")
    val maxId = maxItem.next().as[Int]("productid_int")
    val totalCount: Long = maxId - minId + 1
    val total: Long = totalCount / ddProductIndexSize + 1
    log.info("minId=========={}",minId)
    log.info("maxId=========={}",maxId)
    for (y <- minId to maxId) {
      set += y
      i += 1
      if (i == ddProductIndexSize) {
        i = 0
        j += 1
        sendMsg(Constants.DD_PRODUCT, now, j, set, total)
        set.remove(0, set.length)
      }
    }
    if (i > 0) {
      j += 1
      sendMsg(Constants.DD_PRODUCT, now, j, set, total)
    }
  }
}
