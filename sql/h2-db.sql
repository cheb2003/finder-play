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






/*搜索统计测试表及数据*/

CREATE TABLE ec_order (
  orderId_int int(11) PRIMARY KEY NOT NULL,
  TrackingPC_nvarchar varchar(50) null,
  discountSum_money double null
)

CREATE TABLE ec_orderdetail (
  orderid_int int(11) NOT NULL,
  productkeyid_nvarchar char(100) null
)

CREATE TABLE ec_transaction (
  orderId_int int(11) PRIMARY KEY NOT NULL,
  callbackTime_datetime datetime null,
  PaymentStatus_char char(20) null
)

CREATE TABLE sea_keywordstrace (
  sea_id bigint(20) NOT NULL,
  ProjectName_varchar varchar(50) null,
  Keyword_varchar text,
  SearchCount_int int(11) null,
  TranslatedKeyWord_varchar text,
  TranslatedKeyWordID_bigint bigint(20) null,
  language_varchar varchar(50) null,
  InsertTime_timestamp timestamp,
  UserEmail_varchar varchar(200) null,
  PCCookieID_varchar varchar(50) null,
  SearchUUID_varchar varchar(50) null,
  TracePageUrl_varchar text,
  TraceStep_varchar varchar(50) null,
  TraceSKU_varchar varchar(15) null,
  TraceItemID_varchar varchar(50) null,
  TraceCategory_varchar text,
  TraceOrderNO_varchar varchar(50) null,
  TraceOrderAmount_money decimal(8,2) null,
  PreviousPageUrl_varchar text,
  ServerName_varchar text,
  UserOS_varchar varchar(100) null,
  UserBrowser_varchar text,
  UserBrowserVer_varchar varchar(50) null,
  UserDevice_varchar varchar(100) null,
  UserCountryCode_varchar varchar(50) null,
  UserIPAddress_varchar varchar(100) null,
  CustomerType_varchar varchar(50) null
) 


insert  into ec_order(orderId_int,TrackingPC_nvarchar,discountSum_money) 
	values (1001,'23001',6530.5),(1002,'23002',4200),(1003,'23003',6383),(1005,'23004',5000),
	(1006,'23005',5050),(1007,'23006',39400),(1008,'23018',5000),(1009,'23020',8888),(1010,'23021',5000);

	
insert  into ec_orderdetail(orderid_int,productkeyid_nvarchar) values (1001,'p5001'),(1002,'p5002'),
	(1003,'p5003'),(1004,'p5004'),(1005,'p5011'),(1006,'p5012'),(1088,'p5013'),(1089,'p5014'),(90,'p5015');
	

insert  into ec_transaction(orderId_int,callbackTime_datetime,PaymentStatus_char) values
 (1001,'2013-07-28 00:00:00','Completed'),(1002,'2013-07-28 10:30:58','Completed'),(1003,'2013-07-28 10:30:58','Completed'),
 (1005,'2013-07-28 16:23:58',''),(1006,'2013-07-28 10:30:58','Completed'),(1007,'2013-07-29 12:30:58','Completed'),(1008,'2013-07-28 10:30:58','Completed'),
 (1009,'2013-07-27 16:30:58','Completed'),(1010,'2013-07-28 18:30:58','Completed');
	
	
insert  into sea_keywordstrace(sea_id,ProjectName_varchar,Keyword_varchar,SearchCount_int,InsertTime_timestamp,TraceSKU_varchar,TraceOrderNO_varchar) 
	values (1,'www.dinodirect.com','dddd',3,'2013-06-28 16:24:22','111','1006'),(2,'www.dinodirect.com','key',0,'2013-07-28 16:24:25',null,'0088'),
	(3,'www.dinodirect.com','jduddu',7,'2013-07-27 16:24:26','null','0089'),(4,'www.dinodirect.com','jduddu',4,'2013-07-28 16:24:28',,'111','1001'),
	(5,'www.dinodirect.com','jduddu',0,'2013-07-28 12:24:29',null,'1002'),(6,'www.dinodirect.com','psm',2,'2013-07-28 16:24:31','111','1003'),
	(7,'www.dinodirect.com','psm',4,'2013-07-27 16:24:34',null,'1004'),(8,'www.dinodirect.com','jduddu',1,'2013-06-29 16:24:36','111','1005'),
	(9,'www.dinodirect.com','yyyy',14,'2013-07-26 16:24:40',null,'0090'),(11,NULL,'key',3,'2013-07-28 10:27:46',null,'1007');
