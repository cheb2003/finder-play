CREATE TABLE EC_Product(
	ProductID_int int NOT NULL,
	ProductAliasName_nvarchar varchar(200) NULL,
	ExcavateKeyWords_nvarchar varchar(500) NULL,
	CreateTime_datetime datetime NULL,
)

CREATE TABLE EC_ProductType(
	producttypeid_int int null,
	ProductTypeAliasName_nvarchar varchar(500) NULL,
	IndexCode_nvarchar varchar(500)  NULL
)

CREATE TABLE MKT_TYPE_MONTH_AREASALES(
	ProductTypeID_int int null,
	WebSiteID_smallint int null,
	AliasName_nvarchar varchar(100) null,
	AreaCode_nvarchar varchar(10)

)

insert into EC_ProductType(ProductTypeAliasName_nvarchar,IndexCode_nvarchar) values ('my type','0001')
insert into MKT_TYPE_MONTH_AREASALES(ProductTypeID_int,WebSiteID_smallint,AliasName_nvarchar,AreaCode_nvarchar) values (1,61,'ddf3','ru')