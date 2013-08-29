package my.finder.index.service

import org.apache.lucene.index.{IndexWriterConfig, IndexWriter}
import org.apache.lucene.store.{FSDirectory, Directory}
import java.io.File

import org.apache.lucene.util.Version
import my.finder.common.util.{MyAnalyzer, Config, Util}
import akka.actor.Actor
import my.finder.common.message.{MergeIndexMessage, CloseIndexWriterMessage}

import java.util.Date

/**
 *
 */
object IndexWriteManager{



  private var writerMap = Map[String, IndexWriter]()
  val workDir = Config.get("workDir")
  val oldDir = Config.get("oldDir")
  private var oldIncWriter:IndexWriter = null

  def getIndexWriter(name: String, date: Date): IndexWriter = {
    synchronized {

      val prefix = Util.getPrefixPath(workDir,Util.getKey(name,date))

      val key = Util.getKey(name, date)
      var writer: IndexWriter = writerMap getOrElse (key,null)
      if (writer == null) {
        val directory = FSDirectory.open(new File(prefix))
        val analyzer = new MyAnalyzer();
        val iwc = new IndexWriterConfig(Version.LUCENE_43, analyzer)
        iwc.setRAMBufferSizeMB(128)
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND)
        writer = new IndexWriter(directory,iwc)
        writerMap += (key -> writer)
      }
      writer
    }
  }
  def getIncIndexWriter(name: String, date: Date): IndexWriter = {
    synchronized {

      val prefix = Util.getPrefixPath(workDir,Util.getIncrementalPath(name,date))

      val key = Util.getIncrementalPath(name, date)
      var writer: IndexWriter = writerMap getOrElse (key,null)
      if (writer == null) {
        val directory = FSDirectory.open(new File(prefix))
        val analyzer = new MyAnalyzer();
        val iwc = new IndexWriterConfig(Version.LUCENE_43, analyzer)
        iwc.setRAMBufferSizeMB(128)
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND)
        writer = new IndexWriter(directory,iwc)
        writerMap += (key -> writer)
      }
      writer
    }
  }
  def getOldIncIndexWriter(name: String, date: Date): IndexWriter = {
    synchronized {
      if (oldIncWriter == null) {
        val directory = FSDirectory.open(new File(oldDir))
        val analyzer = new MyAnalyzer();
        val iwc = new IndexWriterConfig(Version.LUCENE_43, analyzer)
        iwc.setRAMBufferSizeMB(128)
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND)
        oldIncWriter = new IndexWriter(directory,iwc)

      }
      oldIncWriter
    }
  }

}
class IndexWriteManager extends Actor{
  def receive = {
    case msg:CloseIndexWriterMessage => {
      val writer = IndexWriteManager.getIndexWriter(msg.name,msg.date)
      writer.forceMerge(1)
      writer.commit
      val console = context.actorFor(Util.getConsoleRootAkkaURLFromMyConfig)
      val incPath = Util.getKey(msg.name,msg.date)
      val workDir = Config.get("workDir")
      val file = new File(workDir + "/" + incPath)
      val timeFile = new File(workDir + "/" + incPath + "/time")
      if(!file.exists()){
        file.mkdir();
      }
      timeFile.createNewFile()
      timeFile.setLastModified(msg.date.getTime)
      console ! MergeIndexMessage(msg.name,msg.date)
    }
  }
}
