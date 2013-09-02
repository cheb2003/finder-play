package my.finder.console.service


import play.api.Play.current
import com.mongodb.casbah.Imports._
import my.finder.common.util.Config
/**
 *
 */
object MyMongoManager {

  var mongoClient:MongoClient = null
  val myMongoDBPort = Config[Int]("myMongoDBPort")
  val myMongoDBUrl = Config[String]("myMongoDBUrl")
  def apply():MongoClient = {
    synchronized{
      if (mongoClient == null) {
        mongoClient = MongoClient(myMongoDBUrl,myMongoDBPort)
      }
      mongoClient
    }
  }

}
