package my.finder.search.service

import play.api.Play._
import org.apache.lucene.store.{FSDirectory, Directory}
import java.io.File
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.IndexSearcher
import play.api.libs.concurrent.Akka
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
 *
 */
object SearcherManager {
  val wordDir = current.configuration.getString("indexDir")
  val ddIndexDir = current.configuration.getString("ddIndexDir")
  val attrIndexDir = current.configuration.getString("attrIndexDir")
  val oldDir = current.configuration.getString("oldDir")
  var searcher:IndexSearcher = null
  var ddSearcher:IndexSearcher = null
  var attrSearcher:IndexSearcher = null
  var oldIncSearcher:IndexSearcher = null
  var oldReader:DirectoryReader = null
  def init = {
    val dir:Directory = FSDirectory.open(new File(wordDir.get));
    val reader = DirectoryReader.open(dir);
    searcher  = new IndexSearcher(reader);

    val ddDir:Directory = FSDirectory.open(new File(ddIndexDir.get));
    val ddReader = DirectoryReader.open(ddDir);
    ddSearcher  = new IndexSearcher(ddReader);

    val attrDir:Directory = FSDirectory.open(new File(attrIndexDir.get));
    val attrReader = DirectoryReader.open(attrDir);
    attrSearcher  = new IndexSearcher(attrReader);

    val oldDirectory:Directory = FSDirectory.open(new File(oldDir.get))
    oldReader = DirectoryReader.open(oldDirectory)

    oldIncSearcher = new IndexSearcher(oldReader)
    fn
  }
  def changeIncDD = {
    println("changing inc dd")
    val newReader = DirectoryReader.openIfChanged(oldReader);
    if(newReader != null){
      oldIncSearcher = new IndexSearcher(newReader)
      oldReader.close()
      oldReader = newReader
      println("has change inc dd")
    }
    println("changed inc dd")
  }
  def fn = {
    Akka.system.scheduler.schedule(0 second,10 second) {
      changeIncDD
    }
  }
}
