package my.finder.index.actor

import akka.actor.{ActorLogging, Actor}
import my.finder.common.message.{IndexUnitLiftStyleTaskMessage, CompleteSubTask}
import my.finder.index.service.{DBService, IndexWriteManager}
import my.finder.common.util.Util
import org.apache.lucene.document.{Field, StringField, Document}


/**
 * LiftStyle索引单元
 */
class IndexUnitLiftStyleActor extends Actor with ActorLogging{
  private val idField = new StringField("id","",Field.Store.YES)
  private val skuField = new StringField("sku","",Field.Store.YES)
  private val indexcodeField = new StringField("indexcode","",Field.Store.YES)
  private val sortField = new StringField("sortno","",Field.Store.YES)
  def receive = {
    case msg: IndexUnitLiftStyleTaskMessage => {
      val writer = IndexWriteManager.getIndexWriter(msg.name,msg.date)
      val conn = DBService.dataSource.getConnection()
      val stmt = conn.createStatement()
      var skipCount = 0
      var failCount = 0
      var successCount = 0
      val ids = msg.ids.mkString(",")
      val sql = s"select ProductID_int,ProductKeyID_nvarchar,IndexCode_nvarchar,SortOrder_int From  EC_IndexLifeProduct with(nolock) where SeqID_int in ($ids)"
      val rs = stmt.executeQuery(sql)
      var doc: Document = null
      while(rs.next()){
        try{
          val id = rs.getString("ProductID_int")
          val sku = rs.getString("ProductKeyID_nvarchar")
          val index = rs.getString("IndexCode_nvarchar")
          val sortNo = rs.getString("SortOrder_int")

          doc = new Document
          idField.setStringValue(id)
          skuField.setStringValue(sku)
          indexcodeField.setStringValue(index)
          sortField.setStringValue(sortNo)

          doc.add(idField)
          doc.add(skuField)
          doc.add(indexcodeField)
          doc.add(sortField)
          writer.addDocument(doc)
          successCount += 1
        } catch {
          case e:Exception => log.info("{}",e);failCount += 1
        }
      }
      val consoleRoot = context.actorFor(Util.getConsoleRootAkkaURLFromMyConfig)
      consoleRoot ! CompleteSubTask(msg.name, msg.date, msg.seq, successCount, failCount, skipCount)
    }
  }
}


