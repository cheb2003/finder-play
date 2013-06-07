
import akka.actor.{ ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import my.finder.console.actor.{MessageFacade, ConsoleRootActor}
import play.api._
import play.api.Play.current
/**
 *
 */
object Global extends GlobalSettings {

  private var system:ActorSystem = null
  override def onStart(app: Application) {
    synchronized{
      if(system == null){
        system = ActorSystem.create("console", ConfigFactory.load().getConfig("console"))
        val root = system.actorOf(Props[ConsoleRootActor], "root")
        if (root != null) {
          Logger.debug("console actor system has started")
          MessageFacade.rootActor = root
        } else {
          Logger.debug("console actor system has not started")
          throw new RuntimeException("console actor system has not started")
        }
      }
    }
  }

  override def onStop(app: Application) {
    synchronized {
      if (system != null) {
        Logger.debug("console actor system has shutdown")
        system.shutdown()
      }
    }
  }
}
