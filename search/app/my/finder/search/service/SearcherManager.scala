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
  val searcher = init
  def init:IndexSearcher = {
    val dir:Directory = FSDirectory.open(new File(wordDir.get));
    val reader = DirectoryReader.open(dir);
    val searcher:IndexSearcher = new IndexSearcher(reader);
    searcher
  }
}
