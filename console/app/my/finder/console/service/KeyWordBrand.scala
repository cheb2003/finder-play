package my.finder.console.service

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.support.rowset.SqlRowSet
import org.apache.lucene.index.Term
import org.apache.lucene.search.{IndexSearcher, BooleanQuery, TermQuery, BooleanClause}
import play.api.Play._
import org.slf4j.LoggerFactory
import scala.collection.mutable.ListBuffer
import org.apache.lucene.util.BytesRef
import org.apache.lucene.search.grouping.{TopGroups, GroupingSearch}
import my.finder.search.service.SearcherManager

class KeyWordBrand {

  def getKeyWord() = {
    val jsMysql:JdbcTemplate = new JdbcTemplate(DBMysql.ds)
    val sql = "select distinct k.Keyword_varchar from sea_keywordsTrace k where k.ProjectName_varchar like '%dinodirect%'"
    val rs: SqlRowSet = jsMysql.queryForRowSet(sql)
    var list :ListBuffer[String] = new ListBuffer[String]
    while ( rs.next() ){
      val keyword = rs.getString("Keyword_varchar")
      val keyWord = KeywordUtil.normalizeKeyword(keyword)
      if ( keyWord != null ){
        searchIndex(keyWord)
      }
    }
  }

  def  searchIndex(keyWord:String) = {
    val bq: BooleanQuery = new BooleanQuery()
    if (keyWord.length > 0) {
       val bqpKeyWord: BooleanQuery = new BooleanQuery()
       val term: Term = new Term("Keyword_varchar", keyWord)
       val pq: TermQuery = new TermQuery(term)
       bqpKeyWord.add(pq, BooleanClause.Occur.MUST)
       bq.add(bqpKeyWord, BooleanClause.Occur.SHOULD)
    }
    val searcher: IndexSearcher = SearcherManager.searcher
    val groupingSearch = new GroupingSearch("indexCode")
    groupingSearch.setAllGroups(true)
    val result: TopGroups[BytesRef] = groupingSearch.search(searcher, bq, 0, 10000)
    var list:ListBuffer[String] = new ListBuffer[String]
    for( x <- result.groups ){
      val code:String = x.groupValue.utf8ToString().substring(0,12)
         if(  !(list.exists(s => s == code)) ){
              list += code
         }
    }
    var counts:ListBuffer[Int] = new ListBuffer[Int]
    for ( i <- 0 until(list.length) ){
      var count = 0
      val codeStr = list(i)
      for (k <- result.groups) {
        val code:String = k.groupValue.utf8ToString()
        if ( codeStr.equals( code.substring(0,12)) ){
             count += k.totalHits
        }
      }
      counts += count
    }




  }
}
