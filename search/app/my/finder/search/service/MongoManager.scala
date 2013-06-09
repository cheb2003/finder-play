package my.finder.search.service

import com.mongodb.casbah.Imports._
import play.api.Play.current
import scala.collection.mutable.ListBuffer

/**
 *
 */
object MongoManager {

  var mongoClient:MongoClient = null
  val mongoCount = current.configuration.getInt("mongoCount")
  def apply():MongoClient = {
    def address:List[ServerAddress] = {
      var i = 0;
      val addresses = ListBuffer[ServerAddress]()
      while (i < mongoCount.get) {
        i += 1
        val url: Option[String] = current.configuration.getString("mongoDBUrl" + i)
        val port: Option[Int] = current.configuration.getInt("mongoDBPort" + i)
        addresses += new ServerAddress(url.get, port.get)
      }
      addresses.toList
    }
    def credentials:List[MongoCredential] = {
      val i = 1
      val credentials = ListBuffer[MongoCredential]()
      val user: Option[String] = current.configuration.getString("mongoDBUser" + i)
      val password: Option[String] = current.configuration.getString("mongoDBPassword" + i)
      val db: Option[String] = current.configuration.getString("mongoAuthDB" + i)
      credentials += MongoCredential(user.get,db.get,password.get.toCharArray)
      credentials.toList
    }
    synchronized{
      if (mongoClient == null) {
        val addr = address
        val crels = credentials
        println(addr)
        println(crels)
        mongoClient = MongoClient(addr, crels, MongoClientOptions.Defaults)
        mongoClient.setReadPreference(ReadPreference.SecondaryPreferred)
      }
      mongoClient
    }
  }
}
