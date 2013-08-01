
import akka.actor.{ ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import my.finder.console.actor.{MessageFacade, ConsoleRootActor}
import my.finder.console.service.{DBMysql, InitJob, GenarateSearchOrderPerDay, IndexManage}
import my.finder.common.util._

import org.quartz.impl.StdSchedulerFactory
import org.quartz.{Scheduler, JobBuilder}
import play.api._
import play.api.Play.current
/**
 *
 */
object Global extends GlobalSettings {

  //private val system:ActorSystem = getActorSystem
  private var system:ActorSystem = null
  private var scheduler:Scheduler = null
  override def onStart(app: Application) {
    synchronized{
      IndexManage.init
      DBMysql.init
      /*system = getActorSystem
      val root = system.actorOf(Props[ConsoleRootActor], "root")
      if (root != null) {
        Logger.debug("console actor system has started")
        MessageFacade.rootActor = root
      } else {
        Logger.debug("console actor system has not started")
        throw new RuntimeException("console actor system has not started")
      }*/

    }
    scheduler = StdSchedulerFactory.getDefaultScheduler()
    scheduler.start()
    InitJob.init(scheduler)
  }

  override def onStop(app: Application) {
    synchronized {
      if (system != null) {
        Logger.debug("console actor system has shutdown")
        system.shutdown()
      }
      if(scheduler != null){
        scheduler.shutdown()
      }
    }
  }

  private def getActorSystem : ActorSystem = {
    //println("-----" + current.configuration.getString("profile").get)
    if(Util.getProfile == Constants.PROFILE_PRODUCTION)
      ActorSystem.create("console", ConfigFactory.load().getConfig("console." + Constants.PROFILE_PRODUCTION))
    else if(Util.getProfile == Constants.PROFILE_TEST)
      ActorSystem.create("console", ConfigFactory.load().getConfig("console." + Constants.PROFILE_TEST))
    else       
      null
  }
}
