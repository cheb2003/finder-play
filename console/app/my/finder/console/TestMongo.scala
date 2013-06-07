package my.finder.console

import com.mongodb.casbah.Imports._
import com.mongodb.util.JSON
import scala.io.Source
import my.finder.common.util.Config
import java.util.Date
import java.text.SimpleDateFormat

/**
 *
 */
object TestMongo {
  def main(args: Array[String]) {
    /*Config.init
    println(Config.get("workDir"))*/
    val uri = new MongoClientURI("mongodb://admin:admin@172.16.20.9/?authMechanism=MONGODB-CR")
    val mongoClient =  MongoClient(uri)
    var productColl = mongoClient("dinobuydb")("ec_productinformation")
    //var q = ("ec_product" -> MongoDBObject("isstopsale_bit" -> false))
    var q:DBObject = ("ec_productprice.unitprice_money" $gt 0) ++ ("ec_product.isstopsale_bit" -> false)
    //var q = MongoDBObject.empty
    //var q:DBObject = MongoDBObject("ec_product.isstopsale_bit" -> false)
    val fields = MongoDBObject("productid_int" -> 1,"ec_product.productaliasname_nvarchar" -> 1
      ,"ec_product.createtime_DateTime" -> 1,"ec_product.discountprice_money" -> 1
      ,"ec_product.productscore_float" -> 1)
    val b = MongoDBList.newBuilder
    b+=MongoDBObject("language_nvarchar" -> "ru","producttitle_nvarchar" -> "fdf3")
    b+=MongoDBObject("language_nvarchar" -> "br","producttitle_nvarchar" -> "brf3")
    /*for(x <- productColl.find(q)){
      //x.as[DBObject]("ec_product").as[String]("productaliasname_nvarchar")
      //productColl.update[DBObject,DBObject](MongoDBObject("_id" -> x._id),$set ("ec_productlanguage" -> b.result))
      productColl.update[DBObject,DBObject](MongoDBObject("_id" -> x._id),$set ("ec_product.createtime_datetime" -> new Date()))
    }

    println(productColl.findOne())*/
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
    val from = sdf.parse("2013-05-26-23-00-00")
    val to = sdf.parse("2013-06-01-00-00-00")
    println(productColl.count("ec_product.createtime_datetime" $gt from $lt to ))
    //val sort = MongoDBObject("productid_int" -> -1)
    //,"ec_product.producttypeid_int" -> 1
    /*for(x <- Source.fromFile("f:/product.json").getLines()){

      productColl += JSON.parse(x).asInstanceOf[DBObject]
    }*/
    /*val max = productColl.find(q,fields).limit(1).sort(sort).next
    var lst:Set[Int] = Set()*/
    /*for(x <- productColl.find(q,fields,0,1)){
      lst += x.as[Int]("productid_int")
    }
    println(lst)

    for(x <- productColl.find("productid_int" $in lst,fields).skip(2).limit(2).sort(sort)){
      println(x)
    }*/
    /*def getValue[B](a1: => DBObject, a2: => B){

    }*/
    /*for(x <- productColl.find("ec_product.productprice_money" $gt 0)){
      //println(x.as[DBObject]("ec_product").as[String]("productaliasname_nvarchar"))
      println(x)
    }*/

    /*for(x <- productColl.find(q)){
      println(x.as[DBObject]("ec_product").as[String]("productaliasname_nvarchar"))
    }*/

    //productColl.insert()
    /*val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss")
    val now = new Date()
    println(sdf.parse(sdf.format(now)))*/

  }
}
