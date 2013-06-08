package my.finder.index.service

import my.finder.common.util.Config
import java.lang
import com.mongodb.casbah.Imports._
import scala.collection.mutable.ListBuffer

/**
 *
 */
object MongoManager {
  var mongoClient:MongoClient = null
  val mongoCount = Integer.valueOf(Config.get("mongoCount"))
  def apply():MongoClient = {
    def address:List[ServerAddress] = {
      var i = 0;
      val addresses = ListBuffer[ServerAddress]()
      while (i < mongoCount) {
        i += 1
        val url: String = Config.get("mongoDBUrl" + i)
        val port: Int = Integer.valueOf(Config.get("mongoDBPort" + i))
        addresses += new ServerAddress(url, port)
      }
      addresses.toList
    }
    def credentials:List[MongoCredential] = {
      val i = 1;
      val credentials = ListBuffer[MongoCredential]()
      val user: String = Config.get("mongoDBUser" + i)
      val password: String = Config.get("mongoDBPassword" + i)
      val db: String = Config.get("mongoAuthDB" + i)
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
