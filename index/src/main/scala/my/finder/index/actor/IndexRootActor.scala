package my.finder.index.actor

import akka.actor.{Props, Actor}
import my.finder.common.message._
import akka.routing.RoundRobinRouter
import my.finder.index.service.IndexWriteManager

/**
 *
 */
class IndexRootActor extends Actor{
  val units = context.actorOf(Props[IndexUnitActor].withDispatcher("my-pinned-dispatcher").withRouter(RoundRobinRouter(nrOfInstances = 32)),"indexUint")
  val ddunits = context.actorOf(Props[IndexUnitActorDD].withDispatcher("my-pinned-dispatcher").withRouter(RoundRobinRouter(nrOfInstances = 32)),"indexUintDD")
  val indexWriterManager = context.actorOf(Props[IndexWriteManager],"indexWriterManager")
  def receive = {
    case msg:IndexTaskMessage => {
      units ! msg
    }
    case msg:IndexTaskMessageDD => {
      ddunits ! msg
    }
    case msg:IndexIncremetionalTaskMessage => {
      units ! msg
    }
    case msg:OldIndexIncremetionalTaskMessage => {
      units ! msg
    }
    case msg:CloseIndexWriterMessage => {
      indexWriterManager ! msg
    }
  }
}
