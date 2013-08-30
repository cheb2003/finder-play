package my.finder.console.service

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.support.rowset.SqlRowSet
import org.apache.lucene.index.Term
import org.apache.lucene.search.{IndexSearcher, BooleanQuery, TermQuery, BooleanClause}
import scala.collection.mutable.ListBuffer
import org.apache.lucene.util.BytesRef
import org.apache.lucene.search.grouping.{TopGroups, GroupingSearch}

class KeyWordBrand {

  def getKeyWord() = {
    val jsMysql:JdbcTemplate = new JdbcTemplate(DBMysql.ds)
    val sql = "select distinct k.Keyword_varchar from sea_keywordsTrace k where k.ProjectName_varchar like '%dinodirect%'"
    val rs: SqlRowSet = jsMysql.queryForRowSet(sql)
    while ( rs.next() ){
      val keyword = rs.getString("Keyword_varchar")
      val keyWord = KeywordUtil.normalizeKeyword(keyword)
      if ( keyWord != null ){
          val list = searchIndex(keyWord)
          if ( list.size > 0 ){
              var sql =  "insert into EC_Keyword_Index(Keywords_varchar,indexCode_varchar) values"
              var count = 0
              for ( k <- list ){
                  if ( count == (list.size -1) ){
                    sql += "(" + keyWord + "," + k + ")"
                  }else{
                    sql += "(" + keyWord + "," + k + "),"
                  }
                count += 1
              }
              jsMysql.update(sql)
          }
      }
    }
  }

  def  searchIndex(keyWord:String):ListBuffer[String] = {
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
      if (  x.groupValue.utf8ToString().length >= 12 ){
          val code:String = x.groupValue.utf8ToString().substring(0,12)
          if(  !(list.exists(s => s == code)) ){
                list += code
          }
      }
    }
    val arr:Array[Int]= new Array[Int](list.size)
    var map:Map[Int,String] = Map[Int,String]()
    for ( i <- 0 until(list.length) ){
      var count = 0
      val codeStr = list(i)
      for (k <- result.groups) {
        val code:String = k.groupValue.utf8ToString()
        if ( code.length >= 12 ){
            if ( codeStr.equals( code.substring(0,12)) ){
               count += k.totalHits
            }
        }
      }
      arr(i) = count
      map += (count -> codeStr )
    }
    val newArr = sort(arr)
    var resultList:ListBuffer[String] = new ListBuffer[String]
    if( newArr.length < 3 ){
       for ( i <- 0 to newArr.length ){
         resultList += map.get( newArr(i) ).toString
       }
    }else{
      for ( i <- (newArr.length-3) to (newArr.length) ){
        resultList += map.get( newArr(i) ).toString
      }
    }
    resultList
  }

 //数组排序
  def sort(xs: Array[Int]):Array[Int] = {
      if(xs.length <= 1) xs
      else {
         val pivot = xs(xs.length /2)
         Array.concat(
            sort(xs filter (pivot >) ),xs filter (pivot == ),sort(xs filter (pivot <) )  )
      }
  }

}
