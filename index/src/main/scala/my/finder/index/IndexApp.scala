package my.finder.index

import akka.actor.{Props, ActorSystem}
import com.typesafe.config.ConfigFactory
import my.finder.index.actor.{IndexRootActor}
import my.finder.common.util.Config
import com.jolbox.bonecp.BoneCPDataSource
import java.sql.Connection
import my.finder.index.service.DBService
/**
 *
 */
object IndexApp {
  def main(args : Array[String]) {
    Config.init("index.properties")
    DBService.init
    val system = ActorSystem("index", ConfigFactory.load().getConfig("index"))
    system.actorOf(Props[IndexRootActor], name = "root")
  }

}
