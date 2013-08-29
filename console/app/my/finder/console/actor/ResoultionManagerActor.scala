package my.finder.console.actor

import akka.actor.{ActorLogging, Actor}
import my.finder.common.message.{CreateSubTask, CloseIndexWriterMessage, CompleteSubTask}
import my.finder.common.util.Util

class ResoultionManagerActor extends Actor with ActorLogging{

  var subTaskMap = Map[String,Map[String,Long]]()

  def receive = {
    case msg:CompleteSubTask => {
      val key = Util.getKey(msg.name, msg.date)
      var obj:Map[String,Long] = subTaskMap.getOrElse(key,null)
      val i = obj("completed") + 1
      val successCount = obj("successCount") + msg.successCount
      val failCount = obj("failCount") + msg.failCount
      obj += ("completed" -> i,"successCount" -> successCount,"failCount" -> failCount)
      subTaskMap += (key -> obj)
      log.info("completed sub task {},{},{},current {}/{}", Array(msg.name,msg.date,msg.seq,i,subTaskMap(key)("total")))
      if(subTaskMap(key)("completed") >= subTaskMap(key)("total")){
        val indexRoot = context.actorFor(Util.getIndexRootAkkaURL)
        indexRoot ! CloseIndexWriterMessage(msg.name,msg.date)
      }
    }

    case msg:CreateSubTask2 => {
      val key = Util.getKey( msg.name )
      val obj: Map[String, Long] = subTaskMap.getOrElse(key, null)
      if (obj == null) {
        var map = Map[String,Long]()
        map += ("completed" -> 0,"total" -> msg.productSize,"failCount" -> 0,"successCount" -> 0)
        subTaskMap += (key -> map)
      }
    }
  }
}
