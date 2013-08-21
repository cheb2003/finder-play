package my.finder.console.service


import play.api.Play.current
import com.mongodb.casbah.Imports._

/**
 *
 */
object MyMongoManager {

  var mongoClient:MongoClient = null
  val myMongoDBPort = current.configuration.getInt("myMongoDBPort").get
  val myMongoDBUrl = current.configuration.getString("myMongoDBUrl").get
  def apply():MongoClient = {
    synchronized{
      if (mongoClient == null) {
        mongoClient = MongoClient(myMongoDBUrl,myMongoDBPort)
      }
      mongoClient
    }
  }

}
