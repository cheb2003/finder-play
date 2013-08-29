package my.finder.console.actor

import akka.actor.{ActorLogging, Actor}
import my.finder.common.message._
import my.finder.common.util.{Constants, Util}
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.support.rowset.SqlRowSet
import scala.collection.mutable.ListBuffer
import play.api.Play._
import my.finder.common.message.IndexResolutionMessage
import my.finder.common.message.SearchResolutionMessage
import java.util.Date


/**
 *
 */
class ResolutionActor extends Actor with ActorLogging{

  val productSize: Int = Integer.valueOf(current.configuration.getString("indexBatchSize").get)

  val resolutionActor = context.actorFor(Util.getIndexRootAkkaURL)
  val resolutionManager = context.actorFor(Util.getIndexManagerAkkaURL)

  def receive = {
    case msg: ResolutionMessage => {
      val DBMssql:JdbcTemplate = new JdbcTemplate(DBMssql.getDataSource)
      DBMssql.setFetchSize(1000)
      val sql:String = "select k.ProductID_int from EC_Product k where k.IndexCode_nvarchar = ''"
      val rs:SqlRowSet = DBMssql.queryForRowSet(sql);
      val pIds:ListBuffer[Int] = new ListBuffer[Int]()
      while ( rs.next() ){
        val pid = rs.getInt("ProductID_int")
        pIds += pid
        if ( pIds.length == productSize ){
          sendResolution( Constants.DD_PRODUCT_RESOLUTION, null, productSize, pIds )
          pIds.clear()
        }
      }
      if ( pIds.length > 0 ){
        sendResolution( Constants.DD_PRODUCT_RESOLUTION, null, productSize, pIds )
      }
    }
  }

  private def sendResolution(name:String, date:Date, productSize:Int, pids:ListBuffer[Int]) {
     resolutionActor ! IndexResolutionMessage( name, date, productSize, pids )
     resolutionManager ! CreateSubTask( name, date, productSize )
  }

}
