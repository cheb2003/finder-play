package my.finder.search.service

import play.api.Play._
import org.apache.lucene.store.{FSDirectory, Directory}
import java.io.File
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.IndexSearcher

/**
 *
 */
object SearcherManager {
  val wordDir = current.configuration.getString("indexDir")
  val oldDir = current.configuration.getString("oldDir")
  var searcher:IndexSearcher = null
  var oldIncSearcher:IndexSearcher = null
  def init = {
    val dir:Directory = FSDirectory.open(new File(wordDir.get));
    val reader = DirectoryReader.open(dir);
    searcher  = new IndexSearcher(reader);

    val oldDirectory:Directory = FSDirectory.open(new File(oldDir.get))
    val oldReader = DirectoryReader.open(oldDirectory)
    oldIncSearcher = new IndexSearcher(oldReader)
  }
}
