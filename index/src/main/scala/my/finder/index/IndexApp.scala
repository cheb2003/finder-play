package my.finder.index

import akka.actor.{Props, ActorSystem}
import com.typesafe.config.ConfigFactory
import my.finder.index.actor.{IndexRootActor}
import my.finder.common.util.Config

/**
 *
 */
object IndexApp {
  def main(args : Array[String]) {
    Config.init("index.properties")
    val system = ActorSystem("index", ConfigFactory.load().getConfig("index"))
    system.actorOf(Props[IndexRootActor], name = "root")
  }

}
