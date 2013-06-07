package my.finder.common.util

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.commons.NotNothing


/**
 *
 */
trait MongoUtil {
  def mv[A : NotNothing](obj: DBObject,key: String,p:String) = {
    obj.as[DBObject](key).as[A](p)
    /*val o:DBObject = obj.as[DBObject](key)
    if(o != null){
      o.as[A](p)
    }*/

  }

  def mvp[A : NotNothing](obj: DBObject,p:String): A = {
    obj.as[DBObject]("ec_product").as[A](p)
  }
  def mv[A : NotNothing](obj: DBObject,p:String): A = {
    obj.as[A](p)
  }

}
