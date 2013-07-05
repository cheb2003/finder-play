package my.finder.console.actor

import akka.actor.{Props, ActorLogging, Actor}
import my.finder.common.message.{CloseIndexWriterMessage, CreateSubTask, MergeIndexMessage, CompleteSubTask}
import my.finder.common.util.Util

import play.api.Play.current
/**
 *
 */
class IndexManagerActor extends Actor with ActorLogging{
  var subTaskMap = Map[String,Map[String,Long]]()


  def receive = {
    //统计子任务完成数，完成后合并索引
    case msg:CompleteSubTask => {
      val key = Util.getKey(msg.name, msg.date)
      var obj:Map[String,Long] = subTaskMap.getOrElse(key,null)
      val i = obj("completed") + 1
      val successCount = obj("successCount") + msg.successCount
      val failCount = obj("failCount") + msg.failCount
      obj += ("completed" -> i,"successCount" -> successCount,"failCount" -> failCount)
      subTaskMap += (key -> obj)
      /*if(msg.seq > 1000){
        log.info("completed sub task {},{},{},current {}/{}", Array(msg.name,msg.date,msg.seq,i,subTaskMap(key)("total")))
      }*/
      log.info("completed sub task {},{},{},current {}/{}", Array(msg.name,msg.date,msg.seq,i,subTaskMap(key)("total")))
      if(subTaskMap(key)("completed") >= subTaskMap(key)("total")){
        val indexRoot = context.actorFor(Util.getIndexRootAkkaURL)
        indexRoot ! CloseIndexWriterMessage(msg.name,msg.date)

       // mergeIndex ! MergeIndexMessage(msg.name,msg.runId,subTaskMap(key)("total"))
      }
    }
    case msg:CreateSubTask => {
      //管理索引子任务
      val key = Util.getKey(msg.name, msg.date)
      val obj: Map[String, Long] = subTaskMap.getOrElse(key, null)
      if (obj == null) {
        var map = Map[String,Long]()
        map += ("completed" -> 0,"total" -> msg.total,"failCount" -> 0,"successCount" -> 0)
        subTaskMap += (key -> map)
      }/* else {
        //累计
        val i = obj("total") + 1
        obj += ("total" -> i)
        subTaskMap += (key -> obj)
        //log.info("add sub task {} to {}", key,subTaskMap(key)("total"))
      }*/
    }
  }
}
