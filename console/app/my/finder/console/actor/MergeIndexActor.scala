package my.finder.console.actor

import akka.actor.{ActorLogging, Actor}
import my.finder.common.message.MergeIndexMessage
import my.finder.common.util.{Util, Config}
import org.apache.lucene.store.{Directory, FSDirectory}
import java.io.File
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.util.Version
import org.apache.lucene.index.{IndexWriter, IndexWriterConfig}


/**
 *
 */
class MergeIndexActor extends Actor with ActorLogging{
  val workDir = Config.get("workDir")
  def receive = {
    case msg:MergeIndexMessage => {
      val key = Util.getKey(msg.name,msg.date)
      log.info("合并索引，{}",key);
      val prefix = Util.getPrefixPath(workDir,key)

      val analyzer = new StandardAnalyzer(Version.LUCENE_43);
      val iwc = new IndexWriterConfig(Version.LUCENE_43, analyzer)
      iwc.setRAMBufferSizeMB(128)
      iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND)

      val writer = new IndexWriter(FSDirectory.open(new File(prefix + "final")),iwc)
      /*for(x <- 1 to msg.total){
        writer.addIndexes(FSDirectory.open(new File(prefix + x)))
      }*/
      writer.forceMerge(1)
      writer.close()
      log.info("合并索引完成，{}",key);
    }
  }
}
