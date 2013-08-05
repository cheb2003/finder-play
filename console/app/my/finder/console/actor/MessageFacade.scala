package my.finder.console.actor

import akka.actor.{ActorSystem, Props, ActorRef}
import my.finder.common.util.{Constants, Util}
import com.typesafe.config.ConfigFactory

/**
 *
 */

object MessageFacade {
  private var root: ActorRef = null
  private var system:ActorSystem = null

  def rootActor: ActorRef = {
    if(root == null){
      system = getActorSystem
      root = system.actorOf(Props[ConsoleRootActor], "root")
    }
    root
  }

  private def getActorSystem : ActorSystem = {
    if(Util.getProfile == Constants.PROFILE_PRODUCTION)
      ActorSystem.create("console", ConfigFactory.load().getConfig("console." + Constants.PROFILE_PRODUCTION))
    else if(Util.getProfile == Constants.PROFILE_TEST)
      ActorSystem.create("console", ConfigFactory.load().getConfig("console." + Constants.PROFILE_TEST))
    else
      null
  }

  def shutdown() = {
    if(system != null){
      system.shutdown()
    }
  }
}
