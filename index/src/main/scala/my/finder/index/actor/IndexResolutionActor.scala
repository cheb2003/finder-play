package my.finder.index.actor

import akka.actor.{Actor, ActorLogging}
import my.finder.common.message.{CompleteSubTask, IndexResolutionMessage}
import java.lang.String
import org.apache.lucene.util.Version
import org.apache.lucene.analysis.TokenStream
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import java.io.{StringReader}
import scala.collection.mutable.ListBuffer
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.support.rowset.SqlRowSet
import my.finder.common.util.Util


class IndexResolutionActor  extends Actor with ActorLogging{
  val os_Str:String = "windows,android,ios,meego,wince,webos"
  val ram_Str:String  = "128MB,256MB,512MB,1GB,2GB,4GB"
  val disk_Str:String  = "2GB,4GB,8GB,16GB,32GB,64GB,128GB,120GB,250GB,320GB"
  val feature_Str = "3G,BLUETOOTH,GPS,PHONE,WIFI,E-BOOK"
  def receive = {
    case msg: IndexResolutionMessage => {
      val pIDs:ListBuffer[Int]  = msg.ids
      val productSize:Int = msg.productSize
      var sb = new StringBuffer()
      if (!pIDs.isEmpty) {
        for (x <- pIDs) {
          sb = sb.append(x).append(",")
        }
      }
      var pids:String = null
      if ( sb.length() > 0 ){
        pids = sb.substring(0, sb.length() - 1)
      }
      val DBService:JdbcTemplate = new JdbcTemplate(DBService.getDataSource)
      DBService.setFetchSize(productSize)
      val sql = "select k.ProductID_int,k.ProductAliasName_nvarchar,k.ProductBrand_nvarchar " +
        "from EC_Product k where k.ProductID_int in (" + pids + ")"
      val rs:SqlRowSet = DBService.queryForRowSet(sql)
      var failCount = 0
      var skipCount = 0
      var successCount = 0
        while ( rs.next() ){
            val aliasName:String  = rs.getString("ProductAliasName_nvarchar")
            var brandName:String  = rs.getString("ProductBrand_nvarchar")
            var OS:String = null
            var CPU:String = null
            var RAM:String = null
            var Disk:String = null
            var Size:String = null
            var Feature:String = null
            var stream:TokenStream = null
            var termAtt:CharTermAttribute = null
            try{
                val matchVersion:Version  = Version.LUCENE_43
                val analyzer:StandardAnalyzer = new StandardAnalyzer(matchVersion)
                stream = analyzer.tokenStream("field", new StringReader(aliasName))
                termAtt = stream.addAttribute( CharTermAttribute.class )
                stream.reset()
                var  list:List = List()
                var num:Int = 0;
                while ( stream.incrementToken() ) {
                   if ( num == 0 && brandName == ""){
                      brandName = termAtt.toString()
                   }
                    list += termAtt.toString()
                    num += 1
                }
                var count:Int = 0
                for (i <- 0 until list.length ){
                    val str:String = list(i).toString
                    if ( feature_Str.indexOf( str.toUpperCase ) > -1 ){
                        var Features:StringBuffer = null
                        if( "phone".equals( str.toLowerCase() ) ){
                          Features = Features.append(str +" call").append(",")
                        }else{
                          Features = Features.append(str).append(",")
                        }
                      Feature = Features.substring( 0, Features.length()-1 )
                   }
                    if ( os_Str.indexOf( str.toLowerCase ) > -1 ){
                       OS = list( i ).toString() + " " + list( i+1 ).toString()
                    }
                    if ( ram_Str.indexOf( str.toUpperCase() ) > -1 && count == 0 ){
                       RAM = str
                      count += 1
                    }
                    if ( disk_Str.indexOf( str.toUpperCase() ) > -1 ){
                       Disk = str
                    }
                    if ( "core".equals( str.toLowerCase() ) ){
                       CPU = list( i-1 ).toString() + " Core"
                    }
                    if ( "inch".equals(str.toLowerCase()) || "inches".equals(str.toLowerCase()) ){
                       Size = list( i-1 ).toString() + " " + list( i ).toString()
                    }
                }
                stream.end()
                successCount += 1
            }catch{
              case e:Exception => log.info("{}",e);failCount += 1
            }finally{
                stream.close()
            }
        }
      val consoleRoot = context.actorFor(Util.getConsoleRootAkkaURLFromMyConfig)
      consoleRoot ! CompleteSubTask(msg.name, msg.date, 0, successCount, failCount, skipCount)
    }
  }
}


