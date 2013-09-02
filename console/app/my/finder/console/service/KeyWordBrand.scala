package my.finder.console.service

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.support.rowset.SqlRowSet
import org.apache.lucene.index.{DirectoryReader, Term}
import org.apache.lucene.search._
import org.apache.lucene.util.BytesRef
import org.apache.lucene.search.grouping.{TopGroups, GroupingSearch}
import org.apache.lucene.store.{FSDirectory, Directory}
import java.io.File
import org.apache.commons.lang.StringUtils

class KeyWordBrand {

  def getKeyWord() = {
    val jsMysql:JdbcTemplate = new JdbcTemplate(DBMysql.ds)
    val sql = "select distinct k.Keyword_varchar from sea_keywordsTrace k where k.ProjectName_varchar like '%dinodirect%'"
    val rs: SqlRowSet = jsMysql.queryForRowSet(sql)
    while ( rs.next() ){
      val keyword = rs.getString("Keyword_varchar")
      val keyWord = KeywordUtil.normalizeKeyword(keyword)
      if ( keyWord != null ){
          val list:List[(String,Int)] = searchIndex(keyWord)
          if ( list != null ) {
              if ( list.size > 0 ){
                var sql =  "insert into EC_Keyword_Index(Keywords_varchar,indexCode_varchar) values"
                if ( list.size >= 3 ) {
                   for ( k <- 0 to 2 ){
                      if ( k == 2 ){ sql += "(" + list(k)._1 + "," + list(k)._2 + ")" }
                      else{ sql += "(" + list(k)._1 + "," + list(k)._2 + ")," }
                   }
                }else{
                   for ( k <- 0 to list.size ){
                      if ( k == list.size-1 ){ sql += "(" + list(k)._1 + "," + list(k)._2 + ")" }
                      else{ sql += "(" + list(k)._1 + "," + list(k)._2 + ")," }
                   }
                }
                jsMysql.update(sql)
              }
              jsMysql.update(sql)
          }
      }
    }
  }

  private def  searchIndex(keyWord:String):List[(String,Int)] = {
    val bq: BooleanQuery = getKeyWordQuery(keyWord)
    val dir: Directory = FSDirectory.open(new File("E:\\IdeaProjects\\indexdata\\ddProductForDB_2013-08-22-14-42-52"))
    val reader = DirectoryReader.open(dir)
    val searcher = new IndexSearcher(reader)
    val groupingSearch = new GroupingSearch("indexCode")
    groupingSearch.setAllGroups(true)
    val result: TopGroups[BytesRef] = groupingSearch.search(searcher, bq, 0, 10000)
    var map:Map[String,Int] = Map[String,Int]()
    for( x <- result.groups ){
      if (  x.groupValue.utf8ToString().length >= 12 ){
          val code:String = x.groupValue.utf8ToString().substring(0,12)
          val num:Int = x.totalHits
          if( map.contains( code ) ){
              val count:Int = map.get( code ).get + num
              map += (code -> count )
          }else{ map += (code -> num )
          }
      }
    }
    val list:List[(String,Int)] = map.toList.sortWith(_._2 < _._2)
    if ( list.size == 0 ){ null }
    else{ list }
  }

  def getKeyWordQuery(pKeyword: String): BooleanQuery = {
    if(StringUtils.isNotBlank(pKeyword)){
      val keyword = pKeyword.trim.toLowerCase
      val bq: BooleanQuery = new BooleanQuery()
      val keywordSplit = keyword.split(" ")

      val bqKeyEn: BooleanQuery = new BooleanQuery()
      val bqKeywordBoundCategory: BooleanQuery = new BooleanQuery()
      val bqSeokeyword: BooleanQuery = new BooleanQuery()
      val bqType: BooleanQuery = new BooleanQuery()
      val bqShortDes: BooleanQuery = new BooleanQuery()
      val bqBrand: BooleanQuery = new BooleanQuery()

      for (k <- keywordSplit) {
        //名称 40
        val nameTerm: Term = new Term("name", k)
        val namePq: PrefixQuery = new PrefixQuery(nameTerm)
        bqKeyEn.add(namePq, BooleanClause.Occur.MUST)

        //绑定品类 90
        val keywordBoundCategoryTerm: Term = new Term("keywordBoundCategory", k)
        val keywordBoundCategoryPq: TermQuery = new TermQuery(keywordBoundCategoryTerm)
        bqKeywordBoundCategory.add(keywordBoundCategoryPq, BooleanClause.Occur.MUST)

        //seokeyword 7
        val seokeywordTerm: Term = new Term("seokeyword", k)
        val seokeywordPq: TermQuery = new TermQuery(seokeywordTerm)
        bqSeokeyword.add(seokeywordPq, BooleanClause.Occur.MUST)

        //品类 3
        val typeTerm: Term = new Term("indexCode", k)
        val typePq: TermQuery = new TermQuery(typeTerm)
        bqType.add(typePq, BooleanClause.Occur.MUST)

        //品牌 18
        val brandTerm: Term = new Term("brandName", k)
        val brandPq: TermQuery = new TermQuery(brandTerm)
        bqBrand.add(brandPq, BooleanClause.Occur.MUST)
      }
      bqKeyEn.setBoost(40f)
      bqKeywordBoundCategory.setBoost(90f)
      bqSeokeyword.setBoost(7f)
      bqType.setBoost(3f)
      bqShortDes.setBoost(2f)
      bqBrand.setBoost(18f)

      bq.add(bqKeyEn, BooleanClause.Occur.SHOULD)
      bq.add(bqKeywordBoundCategory, BooleanClause.Occur.SHOULD)
      bq.add(bqSeokeyword, BooleanClause.Occur.SHOULD)
      bq.add(bqType, BooleanClause.Occur.SHOULD)
      bq.add(bqShortDes, BooleanClause.Occur.SHOULD)
      bq.add(bqBrand, BooleanClause.Occur.SHOULD)
      bq
    } else {
      null
    }
  }

}
