CREATE TABLE EC_Product(
	ProductID_int int primary key,
	ProductAliasName_nvarchar nvarchar(100) NULL,
	QDWProductStatus_int int NULL,
	VentureStatus_tinyint tinyint NULL,
	ProductPrice_money money NULL,
	ProductBrand_nvarchar nvarchar(50) NULL,
	IndexCode_nvarchar nvarchar(50) NULL,
	IsOneSale_tinyint tinyint NULL,
	IsAliExpress_tinyint tinyint NULL,
	ProductKeyID_nvarchar nvarchar(50),
	BusinessName_nvarchar nvarchar(200),
	CreateTime_datetime datetime NULL,
	ProductTypeID_int int NULL,
	IsQualityProduct_tinyint tinyint NULL,
	VentureLevelNew_tinyint tinyint NULL,
	IsTaoBao_tinyint tinyint NULL,
	ProductBrandID_int int NULL,
	BusinessBrand_nvarchar nvarchar(50) NULL,
	isClearance_tinyint int null
)
insert into EC_Product values(1003,'iphone',0,1,45.5,'s1','0001',3,4,'s3','s4','2013-07-07 13:00:00',1003,1,0,3,1,'s5')
insert into EC_Product values(1004,'iphone',0,1,45.5,'s1','s2',3,4,'s3','s4','2013-07-07 13:00:00',3,1,0,3,1,'s5')
insert into EC_Product values(1005,'iphone',0,1,45.5,'s1','s2',3,4,'s3','s4','2013-07-07 13:00:00',3,1,0,3,1,'s5')
insert into EC_Product values(1006,'iphone',0,1,45.5,'s1','s2',3,4,'s3','s4','2013-07-07 13:00:00',3,1,0,3,1,'s5')
insert into EC_Product values(1007,'iphone',0,1,45.5,'s1','s2',3,4,'s3','s4','2013-07-07 13:00:00',3,1,0,3,1,'s5')
update ec_product set ProductTypeID_int='1301',ProductKeyID_nvarchar='k3' where ProductID_int=1004
-- drop table EC_Product
-- select * From EC_Product

CREATE TABLE EC_ProductExtendItem(
	ID_bigint bigint primary key,
	ProductId_int int NULL,
	ItemValueEng_nvarchar nvarchar(max) NULL,
	ItemNameEng_nvarchar nvarchar(max) NULL,
	WebSiteID_smallint smallint NULL,
	AttributeInputType_int int NULL
)
insert into EC_ProductExtendItem values(2,1003,'Sound Card','Type',61,1)

CREATE TABLE EC_ExtendsForProductId(
 	ProductId_int int primary key,
 	AttributeValue_nvarchar nvarchar(max) NULL
)
insert into EC_ExtendsForProductId values(1003,'###Type###Audi Sound Card### hasAttributes')
 

CREATE TABLE EC_SearchKeywordConfig(
	ProductTypeID_int int NULL,
	SearchKeyword_nvarchar nvarchar(max) NULL
) 
insert into EC_SearchKeywordConfig values(1300,'Sound')
insert into EC_SearchKeywordConfig values(1300,'voice')


CREATE TABLE EC_eventProduct(
	ProductKeyID_nvarchar nvarchar(max) NULL	 
) 
insert into EC_eventProduct values ('s3')
-- select ee.ProductKeyID_nvarchar,COUNT(ee.ProductKeyID_nvarchar) as count from EC_eventProduct ee 
-- where ee.ProductKeyID_nvarchar in(%s) group by ee.ProductKeyID_nvarchar

--≤‚ ‘”√
CREATE TABLE EC_ProductType(
	ProductTypeAliasName_nvarchar varchar(500) NULL,
	IndexCode_nvarchar varchar(500)  NULL
)
insert into EC_ProductType(ProductTypeAliasName_nvarchar,IndexCode_nvarchar) values ('my type','0001')




create table rs_dd_prod_score_area (
	CountryID_int int,
	ProductKeyID_nvarchar nvarchar(100),
	ProductID_int int,
	ProductTypeID_int int,
	Score_float float
)
insert into rs_dd_prod_score_area values(25,'s3',1003,1300,90)
insert into rs_dd_prod_score_area values(7,'k3',1004,1301,99)

CREATE TABLE RS_DD_PROD_SCORE(
  ProductKeyID_nvarchar nvarchar(50) NOT NULL,
  ProductID_int int NULL,
  ProductTypeID_int int NULL,
  Score_float float NULL,
  Status_tinyint tinyint NOT NULL
)	

insert into RS_DD_PROD_SCORE values('s3',1003,1300,121,1);
insert into RS_DD_PROD_SCORE values('k3',1003,1301,112,1);


create table ec_indexproduct(
	productid_int int NOT NULL,
	starttime_datetime datetime,
	endtime_datetime datetime
)
insert into ec_indexproduct values(1003,'2013-08-03 12:23:34','2013-08-07 12:23:34')


create table ec_indexlifeproduct(
	productid_int int NOT NULL
)
insert into ec_indexlifeproduct values(1003)
insert into ec_indexlifeproduct values(1004)

create table ec_product001(
	productid_int int NOT NULL,
	ProductCountryInfoForCreator_nvarchar varchar(100)
)

insert into ec_product001 values(1003,'5_0,6_0');

CREATE TABLE SRM_Plat_ShopInfo(
	ShoID_bigint bigint NOT NULL
);

insert into SRM_Plat_ShopInfo values(1);

CREATE TABLE SRM_Plat_ShopAndProductRelation(
	ProductKeyID_nvarchar varchar(50) NOT NULL,
	ShopID_bigint bigint NULL
);

insert into SRM_Plat_ShopAndProductRelation values('s3',1);

CREATE TABLE SRM_Plat_ShopCategories(
	ID_bigint bigint NOT NULL,
	ShopID_int int NULL
);

insert into SRM_Plat_ShopCategories values(1,1)

CREATE TABLE SRM_Plat_ShopCategoryProductRelations(
	CategoryID_int int NULL,
	ProductKeyID_nvarchar varchar(10) NULL
)

insert into SRM_Plat_ShopCategoryProductRelations values(1,'s3');

CREATE TABLE ec_fitproducttype(
	ProductID_int int NOT NULL,
	IndexCode_nvarchar varchar(50) not NULL
);

insert into ec_fitproducttype values(1003,'00010001');
insert into ec_fitproducttype values(1004,'00010002');

CREATE TABLE QDW_CategoryAttributeDictionary(
	KeyID_int int NULL,
	AttributeName_nvarchar varchar(200) NULL
)
insert into QDW_CategoryAttributeDictionary values(1,'Type')

CREATE TABLE QDW_AttributeAndValueDictionary(
	KeyID_int int null,
	AttributeID_bigint bigint NULL,
	AttributeValue_nvarchar varchar(4000) NULL,
)

insert into QDW_AttributeAndValueDictionary values(1,1,'Sound Card')


create TABLE EC_TypeShow(
	ProductKeyID_nvarchar varchar(20),
	IndexCode_nvarchar varchar(100),
	ShowPosition_int int
);

insert into EC_TypeShow('s3','0001',12)

create TABLE EC_ProductComment_QDW(
	productid_int int not null
);

insert into EC_ProductComment_QDW values(1003);
insert into EC_ProductComment_QDW values(1003);

create TABLE EC_ProductComment(
	productid_int int not null
);

insert into EC_ProductComment values(1003);
insert into EC_ProductComment values(1003);
insert into EC_ProductComment values(1003);

update ec_product set isClearance_tinyint = 1 where productid_int = 1003