package my.finder.index.actor

import akka.actor.{Actor, ActorLogging}
import my.finder.common.message.{CompleteSubTask, IndexResolutionMessage}
import java.lang.String
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.support.rowset.SqlRowSet
import my.finder.common.util.Util
import my.finder.index.service.DBService


class IndexResolutionActor extends Actor with ActorLogging {
  val os_Str = "windows,win,android,ios"
  val cpu_Str = "dual，quad，single,intel,amd"
  val disk_Str = "128mb,256mb,512mb,1gb,2gb,4gb,8gb,16gb,32gb,64gb,120gb,128gb,250gb,320gb"
  val other_Str = "phone,3g,bluetooth,gps,wifi,3d,e-book"
  def receive = {
    case msg: IndexResolutionMessage => {
      val productSize: Int = msg.productSize
      val attr = List((1 ,"windows xp"),(2 ,"windows vista"),(3,"window 7"),(4,"window 8"),(5,"android 2.2"),(6 ,"android 2.3"),(7,"android 4.0"),(8,"android 4.1"),
        (9,"android 4.2"),(10 ,"android 4.3"),(11,"ios 5"),(12,"ios 6"),(13,"ios 7"),(14 ,"dual"),(15,"quad"),(16,"single"),(17,"intel"),(18,"amd"),
        (21 ,"128mb"),(22,"256mb"),(23,"512mb"),(24,"1gb"),(25 ,"2gb"),(26 ,"4gb"),(27,"8gb"),(28,"16gb"),(29,"128gb"),(30 ,"120gb"),(31,"250gb"),(32,"320gb"),
        (33,"5inch"),(34,"7inch"),(35 ,"7.9inch"),(36,"8inch"),(37,"9.4inch"),(38,"9.7inch"),(39,"10.1inch"),(40,"11.6inch"),(41,"12.1inch"),(42,"13inch"),
        (43,"phone call"),(44,"3g"),(45 ,"bluetooth"),(46,"gps"),(47,"wifi"),(48,"e-book"))
      val pids: String = msg.ids.mkString(",")
      val dbService: JdbcTemplate = new JdbcTemplate(DBService.dataSource)
      dbService.setFetchSize(productSize)
      val sql = s"""select k.ProductID_int,k.ProductAliasName_nvarchar,
      k.ProductBrand_nvarchar from EC_Product k where k.ProductID_int in ($pids)"""
      val rs: SqlRowSet = dbService.queryForRowSet(sql)
      var failCount = 0
      val skipCount = 0
      var successCount = 0
      var num:Int = 0
      while ( rs.next() ) {
        val productID = rs.getString("ProductID_int")
        val aliasName = rs.getString("ProductAliasName_nvarchar")
        val list = TermAtt.termAttStr(aliasName)
        if ( list == null ){
          failCount += 1
        }
        else{
           for (i <- 0 until attr.length ) {
              val attrValue = attr(i)._2
              for ( k <- 0 until list.size() ){
                val str = list.get(k)
                //操作系统
                if( os_Str.indexOf(str) > -1){
                    val attrStr = str + " " + list.get( k+1 )
                    if ( attrValue.trim.equals(attrStr.trim) ){
                       val attrId = attr(i)._1
                       dbService.update("insert into FND_RELA_PRODUCT_ATTR values(" + productID + "," + attrId+ ")")
                     }
                 }
                //硬盘容量
                if( disk_Str.indexOf(str) > -1){
                  if ( attrValue.equals(str) || attrValue.substring(0,attrValue.length - 1).equals(str)){
                    val attrId = attr(i)._1
                    dbService.update("insert into FND_RELA_PRODUCT_ATTR values(" + productID + "," + attrId+ ")")
                  }
                }
                //规格大小
                if( (str.indexOf("inch") > -1) || (str.indexOf("inches") > -1) ){
                  if ( attrValue.equals(str) ){
                    println("==========33333============="+ str)
                    val attrId = attr(i)._1
                    dbService.update("insert into FND_RELA_PRODUCT_ATTR values(" + productID + "," + attrId+ ")")
                  }
                }
                //cpu品牌
                if( cpu_Str.indexOf(str) > -1){
                  if ( attrValue.equals(str) ){
                    val attrId = attr(i)._1
                    dbService.update("insert into FND_RELA_PRODUCT_ATTR values(" + productID + "," + attrId+ ")")
                  }
                }
                //其他功能
                if( other_Str.indexOf(str) > -1){
                   if ( attrValue.equals(str) ){
                     println("==========44444============="+ str)
                      val attrId = attr(i)._1
                      dbService.update("insert into FND_RELA_PRODUCT_ATTR values(" + productID + "," + attrId+ ")")
                    }
                }
              }
           }
          successCount += 1
        }
        num +=1
      }
      val consoleRoot = context.actorFor(Util.getConsoleRootAkkaURLFromMyConfig)
      consoleRoot ! CompleteSubTask(msg.name, msg.date, productSize, successCount, failCount, skipCount)
    }
  }
}


