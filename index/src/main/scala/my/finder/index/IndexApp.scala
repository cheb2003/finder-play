package my.finder.index

import akka.actor.{Props, ActorSystem}
import com.typesafe.config.ConfigFactory
import my.finder.index.actor.{IndexRootActor}
import my.finder.common.util._
import my.finder.index.service.DBService

/**
 *
 */
object IndexApp {
  def main(args : Array[String]) {
    DBService.init
    val system = if(Config[String]("profile") == Constants.PROFILE_PRODUCTION)
            ActorSystem("index", ConfigFactory.load().getConfig("index." + Constants.PROFILE_PRODUCTION))
        else if (Config[String]("profile") == Constants.PROFILE_TEST)
            ActorSystem("index", ConfigFactory.load().getConfig("index." + Constants.PROFILE_TEST))
        else null
    system.actorOf(Props[IndexRootActor], name = "root")
  }
}
