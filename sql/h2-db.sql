CREATE TABLE EC_Product(
	ProductID_int int NOT NULL,
	ProductAliasName_nvarchar varchar(200) NULL,
	ExcavateKeyWords_nvarchar varchar(500) NULL,
	CreateTime_datetime datetime NULL,
)

CREATE TABLE EC_ProductType(
	ProductTypeAliasName_nvarchar varchar(500) NULL,
	IndexCode_nvarchar varchar(500)  NULL
)

insert into EC_ProductType(ProductTypeAliasName_nvarchar,IndexCode_nvarchar) values ('my type','0001')