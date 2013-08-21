package my.finder.index.service

import org.apache.lucene.store.FSDirectory
import java.io.File
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.IndexSearcher
import my.finder.common.util.{Util, Config}

/**
 *
 */
object SearcherManager {
  val workDir = Config.get("workDir")
  val oldDir = Config.get("oldDir")
  private var searcherMap = Map[String, IndexSearcher]()
  def getSearcher(path:String): IndexSearcher = {
    synchronized {
      val prefix = Util.getPrefixPath(workDir,path)
      var searcher: IndexSearcher = searcherMap getOrElse (path,null)
      if (searcher == null) {
        val directory = FSDirectory.open(new File(prefix))
        val reader = DirectoryReader.open(directory);
        searcher  = new IndexSearcher(reader);
        searcherMap += (path -> searcher)
      }
      searcher
    }
  }
}
