package my.finder.common.model

import scala.collection.mutable.ListBuffer
import play.api.libs.json._
import play.api.libs.json.JsObject
import play.api.libs.json.JsNumber

class Doc (pId:Int,pName:String,unitPrice:Double,indexCode:String,isOneSale:Int,isAliExpress:Int,sku:String,businessName:String,pNameRU:String,segmentWordRu:String,segmentWordEn:String,segmentWordBr:String,sourceKeyword:String,sourceKeywordCN:String,skuOrder:Int,pNameBR:String,pNameCN:String,createTime:String,pTypeId:Int,isQuality:Int,ventureStatus:Int,ventureLevelNew:Int,isTaobao:Int,pBrandId:Int,businessBrand:String) {
	def toXML = 
		<doc>
			<pId>{pId}</pId>
			<pName>{pName}</pName>
			<unitPrice>{unitPrice}</unitPrice>
			<indexCode>{indexCode}</indexCode>
			<isOneSale>{isOneSale}</isOneSale>
			<isAliExpress>{isAliExpress}</isAliExpress>
			<sku>{sku}</sku>
			<businessName>{businessName}</businessName>
			<pNameRU>{pNameRU}</pNameRU>
			<segmentWordRu>{segmentWordRu}</segmentWordRu>
			<segmentWordEn>{segmentWordEn}</segmentWordEn>
			<segmentWordBr>{segmentWordBr}</segmentWordBr>
			<sourceKeyword>{sourceKeyword}</sourceKeyword>
			<sourceKeywordCN>{sourceKeywordCN}</sourceKeywordCN>
			<skuOrder>{skuOrder}</skuOrder>
			<pNameBR>{pNameBR}</pNameBR>
			<pNameCN>{pNameCN}</pNameCN>
			<createTime>{createTime}</createTime>
			<pTypeId>{pTypeId}</pTypeId>
			<isQuality>{isQuality}</isQuality>
			<ventureStatus>{ventureStatus}</ventureStatus>
			<ventureLevelNew>{ventureLevelNew}</ventureLevelNew>
			<isTaobao>{isTaobao}</isTaobao>
			<pBrandId>{pBrandId}</pBrandId>
			<businessBrand>{businessBrand}</businessBrand>
		</doc>
}

abstract class PageResult

abstract class DataPageResult[A] (
    val data:A,
    val page:Int,
    val size:Int,
    val total:Int
) extends PageResult

abstract class QueryResult[A](data:A,page:Int,size:Int,total:Int,val query:String) extends DataPageResult[A](data,page,size,total)


class IdsPageResult(data:ListBuffer[Int],page:Int,size:Int,total:Int,query:String) extends QueryResult[ListBuffer[Int]](data,page,size,total,query){
}

class ErrorResult(error:String) extends DataPageResult[ListBuffer[Int]](null,-1,-1,-1)