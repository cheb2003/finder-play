package my.finder.common.util

import org.apache.lucene.analysis.{TokenStream, Analyzer}
import org.apache.lucene.analysis.core.{LowerCaseFilter, WhitespaceTokenizer}
import org.apache.lucene.util.Version
import java.io.Reader

/**
 *
 */

/**
 *
 */
class MyAnalyzer extends Analyzer {
  protected def createComponents(fieldName: String, reader: Reader): Analyzer.TokenStreamComponents = {
    val src: WhitespaceTokenizer = new WhitespaceTokenizer(Version.LUCENE_43, reader)
    val tok: TokenStream = new LowerCaseFilter(Version.LUCENE_43, src)
    return new Analyzer.TokenStreamComponents(src, tok) {
      protected override def setReader(reader: Reader) {
        super.setReader(reader)
      }
    }
  }
}