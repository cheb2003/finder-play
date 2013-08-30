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
  val attrUnits = context.actorOf(Props[IndexUnitAttributesActor].withDispatcher("my-pinned-dispatcher").withRouter(RoundRobinRouter(nrOfInstances = 32)),"indexAttrUnit")
  val liftStyleUnits = context.actorOf(Props[IndexUnitLiftStyleActor].withDispatcher("my-pinned-dispatcher").withRouter(RoundRobinRouter(nrOfInstances = 32)),"indexLiftStyleUnit")
  val indexWriterManager = context.actorOf(Props[IndexWriteManager],"indexWriterManager")

  val resolutionUints = context.actorOf(Props[IndexResolutionActor].withDispatcher("my-pinned-dispatcher").withRouter(RoundRobinRouter(nrOfInstances = 32)),"resolutionUint")
  def receive = {
    case msg:IndexUnitLiftStyleTaskMessage => {
      liftStyleUnits ! msg
    }
    case msg:IndexAttributeTaskMessage => {
      attrUnits ! msg
    }
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
    case msg:IndexResolutionMessage => {
      resolutionUints ! msg
    }
  }
}
