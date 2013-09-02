package my.finder.console.service
import com.mongodb.casbah.Imports._

import scala.collection.mutable.ListBuffer
import my.finder.common.util.Config
/**
 *
 */
object MongoManager {

  var mongoClient:MongoClient = null
  val mongoCount = Config[Int]("mongoCount")
  def apply():MongoClient = {
    def address:List[ServerAddress] = {
      var i = 0;
      val addresses = ListBuffer[ServerAddress]()
      while (i < mongoCount) {
        i += 1
        val url: String = Config[String]("mongoDBUrl" + i)
        val port: Int = Config[Int]("mongoDBPort" + i)
        addresses += new ServerAddress(url, port)
      }
      addresses.toList
    }
    def credentials:List[MongoCredential] = {
      val i = 1
      val credentials = ListBuffer[MongoCredential]()
      val user: String = Config[String]("mongoDBUser" + i)
      val password: String = Config[String]("mongoDBPassword" + i)
      val db: String = Config[String]("mongoAuthDB" + i)
      credentials += MongoCredential(user,db,password.toCharArray)
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
