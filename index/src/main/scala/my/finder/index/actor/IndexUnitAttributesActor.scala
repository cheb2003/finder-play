package my.finder.index.actor

import akka.actor.{ActorLogging, Actor}
import my.finder.common.message.{CompleteSubTask, IndexAttributeTaskMessage}
import my.finder.index.service.{SearcherManager, DBService, IndexWriteManager}
import org.apache.lucene.queryparser.classic.{QueryParser, QueryParserBase}
import org.apache.lucene.util.Version
import my.finder.common.util.Util
import scala.collection.mutable.ListBuffer
import org.apache.lucene.document.{Field, StringField, Document}
import org.apache.lucene.index.Term
import org.apache.lucene.search.TermQuery
import org.apache.lucene.analysis.core.WhitespaceAnalyzer


/**
 * 属性索引单元
 */
class IndexUnitAttributesActor extends Actor with ActorLogging{
  private val idField = new StringField("id","",Field.Store.YES)
  private val valueField = new StringField("value","",Field.Store.YES)
  private val nameField = new StringField("name","",Field.Store.YES)
  private val docIdsField = new StringField("docIds","",Field.Store.YES)
  def receive = {
    case msg: IndexAttributeTaskMessage => {
      val writer = IndexWriteManager.getIndexWriter(msg.name,msg.date)
      val searcher = SearcherManager.getSearcher(msg.ddProductIndex)
      val conn = DBService.dataSource.getConnection()
      val stmt = conn.createStatement()
      val ids = msg.ids.mkString(",")
      var skipCount = 0
      var failCount = 0
      var successCount = 0
      val rs = stmt.executeQuery(s"SELECT a.keyid_int as id, a.attributevalue_nvarchar as aValue,c.AttributeName_nvarchar aName FROM dbo.QDW_AttributeAndValueDictionary a,dbo.QDW_CategoryAttributeDictionary c where a.attributeid_bigint = c.keyid_int and a.keyid_int in ($ids)")

      while(rs.next()){
        try{
          val id = rs.getString("id")
          val aValue = rs.getString("aValue")
          val aName = rs.getString("aName")

          val searchStr = QueryParserBase.escape(s"###$aName###$aValue###".toLowerCase)
          val t = new Term("attribute","\"" + searchStr + "\"")
          //val t = new Term("indexCode","s2")
          val q = new TermQuery(t)
          val parser = new QueryParser(Version.LUCENE_43,"attribute",new WhitespaceAnalyzer(Version.LUCENE_43))
          val qAttr = parser.parse("\"" + searchStr + "\"")
          log.info("{}",qAttr)
          val topDocs = searcher.search(qAttr,Integer.MAX_VALUE)
          val scoreDocs = topDocs.scoreDocs
          val list = new ListBuffer[Int]
          idField.setStringValue("")
          valueField.setStringValue("")
          nameField.setStringValue("")
          docIdsField.setStringValue("")
          scoreDocs.foreach{
            list += _.doc
          }
          val docIds = list.mkString(" ")
          val doc = new Document
          idField.setStringValue(id)
          valueField.setStringValue(aValue)
          nameField.setStringValue(aName)
          docIdsField.setStringValue(docIds)

          doc.add(idField)
          doc.add(valueField)
          doc.add(nameField)
          doc.add(docIdsField)
          writer.addDocument(doc)
          successCount += 1
        } catch {
          case e:Exception => log.info("{}",e);failCount += 1
        } finally {
          if (rs != null) rs.close()
          if (stmt != null) stmt.close()
          if (conn != null) conn.close()
        }
      }
      val consoleRoot = context.actorFor(Util.getConsoleRootAkkaURLFromMyConfig)
      consoleRoot ! CompleteSubTask(msg.name, msg.date, msg.seq, successCount, failCount, skipCount)
    }
  }
}


