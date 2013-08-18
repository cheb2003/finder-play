package my.finder.search.service

/**
 * dd前台搜索接口服务
 */

object DDSearchService {
	def method(queryParams:Map[String,String]) = {
		
	}
}

/**
 * 产品查询结果
 */
case class PSearchResult(ids:List[Int],page:Int,size:Int,total:Int)