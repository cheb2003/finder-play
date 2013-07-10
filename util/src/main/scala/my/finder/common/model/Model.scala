package my.finder.common.model
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