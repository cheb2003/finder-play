package my.finder.index

import akka.actor.{Props, ActorSystem}
import com.typesafe.config.ConfigFactory
import my.finder.index.actor.{IndexRootActor}
import my.finder.common.util._
import my.finder.index.service.DBService

import scala.collection.mutable.Queue
import scala.xml._
/**
 *
 */
object IndexApp {
  def main(args : Array[String]) {
    Config.init("index.properties")
    DBService.init
    val system = if(Config.get("profile") == Constants.PROFILE_PRODUCTION)
            ActorSystem("index", ConfigFactory.load().getConfig("index." + Constants.PROFILE_PRODUCTION))
        else if (Config.get("profile") == Constants.PROFILE_TEST)
            ActorSystem("index", ConfigFactory.load().getConfig("index." + Constants.PROFILE_TEST))
        else null
    system.actorOf(Props[IndexRootActor], name = "root")
    /*var nodes = new Queue[Node]()
    var n = <Service id="1" cnt="12"/> 
    n = n % Attribute(None,"fff",Text("dddd"),Null)
    /*nodes += n
    nodes += <Service id="2" cnt="34"/>*/
    println(<root>{n}</root>)*/
  }

}
