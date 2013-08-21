package my.finder.console.actor

import akka.actor.{ActorLogging, Actor}
import my.finder.common.message.{IndexResolutionMessage, SearchResolutionMessage, MergeIndexMessage}
import my.finder.common.util.{Util, Config}
import org.apache.lucene.store.{Directory, FSDirectory}
import java.io.File
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.util.Version
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}
import org.springframework.jdbc.core.JdbcTemplate
import my.finder.console.service.DBMssql
import org.springframework.jdbc.support.rowset.SqlRowSet
import scala.collection.mutable.ListBuffer


/**
 *
 */
class ResolutionActor extends Actor with ActorLogging{
  val indexRootActor = context.actorFor(Util.getIndexRootAkkaURL)
  val indexRootManager = context.actorFor(Util.getIndexManagerAkkaURL)
  def receive = {
    case msg: SearchResolutionMessage => {
      getProductIds()
    }
  }

  private def sendResolution(ids:ListBuffer[Int]) {
    indexRootActor ! IndexResolutionMessage(ids)
    indexRootManager ! CreateSubTask(name, runId, total)
  }

  def getProductIds() = {
    val jsMysql:JdbcTemplate = new JdbcTemplate(DBMssql.ds);
    val sql:String = "select * from EC_Product";
    val rs:SqlRowSet = jsMysql.queryForRowSet(sql);
    val pIds:ListBuffer[Int] = new ListBuffer[Int]()
    while ( rs.next() ){
      val pid = rs.getInt("ProductID_int")
      pIds += pid
      if ( pIds.size == 200 ){
          sendResolution(pIds)
      }
    }
  }
}
