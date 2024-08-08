
/*

Sample Database transfer script from OLTP to Staging environment where entities are 
Purchasetransaction, SalesTransaction, Sore, City, State, Customer, Department, Employee, MaritalStatus, Product, Promotion, PromotionType and Vendor 
*/

use cansoft_oltp

Select  * from PurchaseTransaction

----join SalesTransaction with Promotion, and PromotionType tables for staging
	select * from SalesTransaction s  inner join Promotion  p  on p.PromotionID=s.PromotionID
	inner join PromotionType t  on t.PromotionTypeID=p.PromotionTypeID

--create schema for staging

use cansoft_staging

create schema oltp

---create schema for edw

use cansoft_edw
create schema edw

---- inner join the product table with department
use cansoft_oltp

select p.ProductID, p.Product, p.ProductNumber, p.UnitPrice, d.Department,getdate() as LoadDate  from Product p
inner join Department d on p.DepartmentID=d.DepartmentID

---Create and Load Product on Staging environment on each day and set null 

use cansoft_staging

select OBJECT_ID('oltp.Product')
IF OBJECT_ID('oltp.Product') is not null
	truncate table oltp.Product 

create table oltp.Product
( 
   productID int, 
   product nvarchar(50),
   ProductNumber nvarchar(50),
   Unitprice float,
   Department nvarchar(50),
   LoadDate datetime default getdate(),
   constraint oltp_product_pk  primary key(productid)
)

-----check Product data
select *  from oltp.Product

---Create and Load Product on OLTP environment/ Enterprise database Warehouse

use cansoft_edw

create table edw.dimProduct
( 
  productsk int identity(1,1),
  productID int, 
  product nvarchar(50),
  ProductNumber nvarchar(50),
  Unitprice float,
  Department nvarchar(50),
  effectiveStartdate datetime,
  effectiveEnddate datetime,  
   constraint edw_dimproduct_sk  primary key(productsk)
)

--- store -------
use tescaoltp
select s.StoreID,s.StoreName AS Store,s.StreetAddress, c.CityName as City,st.State, getdate() as LoadDate  from Store s
inner join City c on s.CityID=c.CityID
inner join State st on st.StateID=c.StateID


select count(*) as OltpCount  from Store s
inner join City c on s.CityID=c.CityID
inner join State st on st.StateID=c.StateID


use tescastaging

IF OBJECT_ID('oltp.store') is  not null 
  truncate table oltp.store

Create table  oltp.store
(
 storeid int,
 Store nvarchar(50),
 streetAddress nvarchar(50),
 city nvarchar(50),
 State nvarchar(50),
 LoadDate datetime default getdate(),
 constraint oltp_store_pk primary key(storeid)
)

use tescastaging
select count(*) as StageCount  from oltp.store s

select storeid,s.Store,s.streetAddress,s.city,s.State  from oltp.store s

select count(*) as Precount from oltp.store
use tescaedw

select * from edw.dimstore

select count(*) as Precount from edw.dimstore



select count(*) as Postcount from edw.dimstore

create table edw.dimstore
(
 storesk int identity(1,1),
 storeid int,
 Store nvarchar(50),
 streetAddress nvarchar(50),
 city nvarchar(50),
 State nvarchar(50),
 EffectiveStartDate datetime, 
 constraint edw_dimstore_sk  primary key(storesk)
 )


 ---- Promotion ----

 use tescaoltp

 select PromotionID, pt.Promotion,StartDate as PromotionStartDate,EndDate as PromotionEndDate,DiscountPercent,getdate() as loadDate from  Promotion p 
 inner join PromotionType pt on p.PromotionTypeID=pt.PromotionTypeID


 select count(*) as OltpCount from  Promotion p 
 inner join PromotionType pt on p.PromotionTypeID=pt.PromotionTypeID


 use tescastaging

IF OBJECT_ID('oltp.promotion') is not null 
  Truncate  table oltp.promotion

  alter tab e 
create table oltp.promotion 
(
  PromotionID int,
  Promotion nvarchar(50),
  PromotionStartDate date,
  PromotionEndDate date,
  DiscountPercent  float,
  loadDate  datetime,
  constraint oltp_promotion_pk primary key(promotionid)
)

select count(*) as StageCount from oltp.promotion

select PromotionID, Promotion,PromotionStartDate,PromotionEndDate,DiscountPercent  from oltp.promotion


use tescaedw

select count(*) as PreCount from  edw.dimpromotion

select count(*) as PostCount from  edw.dimpromotion

create table edw.dimpromotion 
(
  PromotionSk int identity(1,1),
  PromotionID int,
  Promotion nvarchar(50),
  PromotionStartDate date,
  PromotionEndDate date,
  DiscountPercent  float,
  EffectiveStartDate  datetime,
  constraint edw_dimpromotion_sk primary key(promotionsk)
)

---- Customer ----

use tescaoltp



select c.CustomerID, c.LastName,c.FirstName,c.CustomerAddress,ct.CityName City, s.State,
GETDATE() as loaddate 
from Customer c
inner join City ct  on c.CityID=ct.CityID
inner join State s on s.StateID=ct.StateID


select count(*) as OltpCount from Customer c
inner join City ct  on c.CityID=ct.CityID
inner join State s on s.StateID=ct.StateID


select c.CustomerID, concat(Upper(c.LastName),',',c.FirstName) CustomerName,c.CustomerAddress,ct.CityName City, s.State,
GETDATE() as loaddate 
from Customer c
inner join City ct  on c.CityID=ct.CityID
inner join State s on s.StateID=ct.StateID

use tescastaging

IF OBJECT_ID('oltp.Customer')  is not null 
  TRUNCATE TABLE oltp.Customer


create table oltp.Customer
(
CustomerID int,
CustomerName  nvarchar(250),
CustomerAddress  nvarchar(50),
City nvarchar(50),
State nvarchar(50),
LoadDate datetime default getdate(),
constraint oltp_customer_pk primary key(CustomerId)
)

select  count(*) as StageCount  from oltp.Customer 

select  CustomerID,CustomerName,CustomerAddress,City,State  from oltp.Customer 

use tescaedw

select count(*) as PreCount from edw.dimCustomer

select count(*) as PostCount from edw.dimCustomer

create table edw.dimCustomer
(
CustomerSk int identity(1,1),
CustomerID int,
CustomerName  nvarchar(250),
CustomerAddress  nvarchar(50),
City nvarchar(50),
State nvarchar(50),
EffectiveStartDate datetime,
constraint edw_dimCustomer_sk  primary key(CustomerSk)
)

--- PoSChannel-----

use tescaoltp

Select count(*) as OltpCount from POSChannel 

Select ChannelID,ChannelNo,DeviceModel,SerialNo,InstallationDate,GETDATE() as LoadDate  from POSChannel 

use tescastaging

IF OBJECT_ID('oltp.PosChannel') is not null 
   TRUNCATE Table  oltp.PosChannel

Create table oltp.PosChannel
(
 ChannelID int,
 ChannelNo nvarchar(50),
 DeviceModel nvarchar(50),
 SerialNo nvarchar(50),
 InstallationDate date,
 LoadDate Datetime default getdate(),
 Constraint oltp_PosChannel_pk primary key(ChannelID)
)

Select count(*) as StageCount  from oltp.POSChannel 

Select ChannelID,ChannelNo,DeviceModel,SerialNo,InstallationDate  from oltp.POSChannel 

use tescaedw
 
 Select count(*) as PreCount from edw.dimPosChannel

 Select count(*) as PostCount from edw.dimPosChannel

 Select * from edw.dimPosChannel  Where ChannelID in (1,9)

Create table edw.dimPosChannel
(
 ChannelIDSK int identity(1,1),
 ChannelID int,
 ChannelNo nvarchar(50),
 DeviceModel nvarchar(50),
 SerialNo nvarchar(50),
 InstallationDate date, 
 EffectiveStartDate Datetime,
 EffectiveEndDate Datetime,
 Constraint edw_dimPosChannel_sk primary key(ChannelIDSk)
)


---- Employee--- 

use tescaoltp

select e.EmployeeID, e.EmployeeNo,Concat(upper(e.LastName),',',e.FirstName) EmployeeName,m.MaritalStatus,e.DoB DateofBirth,getdate() LoadDate  from Employee e
inner join MaritalStatus m on e.MaritalStatus=m.MaritalStatusID

select count(*) as OltpCount  from Employee e
inner join MaritalStatus m on e.MaritalStatus=m.MaritalStatusID



use tescastaging

IF OBJECT_ID('oltp.Employee') IS Not null
 TRUNCATE TABLE oltp.Employee

Create table  oltp.Employee
(
 EmployeeID int, 
 EmployeeNo nvarchar(50),
 EmployeeName nvarchar(250),
 MaritalStatus nvarchar(50),
 DateofBirth date,
 LoaDate Datetime default getdate(),
 constraint oltp_employee_pk primary key (EmployeeID)
 )

  select  count(*) as StageCount  from oltp.Employee

 select  EmployeeID, EmployeeNo ,EmployeeName ,MaritalStatus ,DateofBirth  from oltp.Employee



 use tescaedw

 select count(*) as PreCount from  edw.dimEmployee

 select count(*) as PostCount from  edw.dimEmployee

 select * from  edw.dimEmployee

 drop  table  edw.Employee

Create table  edw.dimEmployee
(
EmployeeSK int identity(1,1), 
 EmployeeID int, 
 EmployeeNo nvarchar(50),
 EmployeeName nvarchar(250),
 MaritalStatus nvarchar(50),
 DateofBirth date,
 EffectiveStartDate Datetime,
 EffectiveEndDate Datetime,
 constraint edw_dimemployee_sk primary key (EmployeeSK)
 )


 -----Vendor------

 use tescaoltp

 select v.VendorID,v.VendorNo,v.RegistrationNo ,concat_ws(',',Upper(v.LastName),v.FirstName) VendorName,
 v.VendorAddress,c.CityName City,s.State,getDate() LoadDate   from vendor v 
 inner join City c  on v.CityID=c.CityID
 inner join State s on s.StateID=c.StateID

 select  count(*) as OltpCount   from vendor v 
 inner join City c  on v.CityID=c.CityID
 inner join State s on s.StateID=c.StateID


 use tescastaging

 IF OBJECT_ID('oltp.Vendor') is not null
  TRUNCATE TABLE oltp.Vendor

create table oltp.vendor
(
	vendorid int,
	VendorNo nvarchar(50),
	Registration nvarchar(50),
	VendorName nvarchar(250),
	VendorAddress nvarchar(50),
	City nvarchar(50),
	State nvarchar(50),
	Loaddate datetime default getdate(),
	constraint oltp_vendor_pk primary key(vendorid)
)

select count(*) as StageCount  from oltp.Vendor

select vendorid,VendorNo,vendorName,Registration,VendorAddress,City,State  from oltp.Vendor

use tescaedw

select count(*) as PreCount from edw.dimvendor

select count(*) as PostCount from edw.dimvendor


select * from edw.dimvendor where vendorid=1

create table edw.dimvendor
(
	vendorsk int identity(1,1),
	vendorid int,
	VendorNo nvarchar(50),
	Registration nvarchar(50),
	VendorName nvarchar(250),
	VendorAddress nvarchar(50),
	City nvarchar(50),
	State nvarchar(50),
	EffectiveStartDate Datetime,
	EffectiveEndDate Datetime,
	
	constraint edw_dimvendor_sk primary key(vendorsk)
)
----- dimTime-----


 create table  edw.dimTime
 (
 hoursk  int  identity(1,1),
 [hour] int,
 dayperiod nvarchar(20),
 effectiveStart datetime,
 constraint edw_dimTime_sk primary key(hoursk) 
 )

 
/*
 hour -> 0 to 23
 dayperiod -> 0 to 3 -> midnight, 4 to 11 morning 12 noon, 13 to 16 afternoon, 17 to 20 evening, 21 to 23 night 
 */

 truncate table edw.dimTime

alter procedure edw.spTime AS
 BEGIN
 set nocount on
 declare @starthour int=0 
 IF (select count(*) from edw.dimTime) >0 
    TRUNCATE Table edw.dimTime

 while @starthour<=23
 BEGIN 
 insert into edw.dimTime(hour,dayperiod,effectiveStart)
 select @starthour as [hour], case
     when @starthour>=0 and @starthour<=3 then 'MidNight'
	 when @starthour>=4 and @starthour<=11 then 'Morning'
	 when @starthour=12 then 'Noon'
	 when @starthour>=13 and @starthour<=16 then 'Afternoon'
	 when @starthour>=17 and @starthour<=20 then 'Evening'
	 when @starthour>=21 and @starthour<=23 then 'Night'
	 end Dayperiod, GETDATE() as effectivestartdate

 select @starthour =@starthour+1
 END
 END

 exec edw.spTime
 select * from edw.dimTime



 ------ dimDate-------



drop table edw.dimDate

create table edw.dimDate
(
   businessdatekey int,
   businessdate date,
   businessyear int,
   businessquarter nvarchar(2), 
   businessWeekNo int, 
   businessday int,
   Englishmonth nvarchar(50),
   EnglishDayofWeek nvarchar(50),
   Hindumonth nvarchar(50),
   HinduDayofWeek nvarchar(50),
   Frenchmonth nvarchar(50),
   FrenchDayofWeek nvarchar(50),
   Yorubahmonth nvarchar(50),
   YorubaDayofWeek nvarchar(50),
   effectivedate datetime,
   constraint edw_dimdate_sk primary key(businessdatekey)
  )


select  convert(nvarchar(8),GETDATE(),112) , convert(date,GETDATE()), year(getdate()), DATEPART(QUARTER, getdate()),
	DATEPART(WEEK,GETDATE()),DATEPART(Day,GETDATE()),DATENAME(month,getdate()),DATENAME(WEEKDAY,getdate())


select DATEADD(YEAR,70,GETDATE())

select DATEFROMPARTS(2001,12,31)



create procedure edw.spGenerateCalender(@generateyear int)
AS
BEGIN 
set nocount on
declare @startdate date = 
		(
			select  min(a.trandate) from (
			select min(convert(date,transDate)) as trandate from tescaoltp.dbo.SalesTransaction
			union 
			select min(convert(date,transDate)) as transdate  from tescaoltp.dbo.PurchaseTransaction
		) a
		)
declare @enddate date =dateadd(year,@generateyear,datefromparts(year(@startdate),12,31))
declare @nofdays int=DATEDIFF(day,@startdate,@enddate)
declare @currentday int=0
declare @currentdate date
IF (select count(*) from edw.dimDate )>0 
  truncate table edw.dimDate

While @currentday<=@nofdays
BEGIN 
  select @currentdate=DATEADD(day,@currentday,@startdate) 
  insert into edw.dimDate(businessdatekey,businessdate,businessyear,businessquarter,businessWeekNo,businessday,Englishmonth,EnglishDayofWeek,
                          Hindumonth,HinduDayofWeek,Frenchmonth,FrenchDayofWeek,Yorubahmonth,YorubaDayofWeek,effectivedate)
					select convert(nvarchar(8),@currentdate,112) businessdatekey ,@currentdate businessdate,year(@currentDate) businessyear,
					concat('Q',datepart(quarter,@currentDate)) businessquarter,datepart(week,@currentdate) businessWeekNo,
					datepart(day,@currentDate) businessday, datename(month,@currentDate) EnglishMonth,datename(WEEKDAY,@currentDate) EnglishDayofWeek,
					 case datepart(month,@Currentdate)
					 when 1  then 'Magha'  When 2 then 'Phalguna' When 3 then 'Chaitra' When 4 then 'Vaisakha' When 5 then 'Jyaistha'
					 when 6 then 'Asadha' when 7 then 'Shravana' when 8 then 'Bhadra' when 9 then 'Asvina' when 10 then 'Kartika'
					 when 11 then'Agrahayana' when 12 then 'Pausa' end Hindumonth, 

					 case datepart(weekday,@currentdate)
					 when 1 then 'Raviãra' when 2 then 'Somavãra' When 3 then 'Mañgalvã' when 4 then 'Budhavãra' 
					 when 5 then 'Guruvãra' when 6 then 'Sukravãra'  when 7 then 'Sanivãra' end HinduDayofWeek,

					 case datepart(month,@Currentdate)
					 when 1  then 'janvier'  When 2 then 'février' When 3 then 'mars' When 4 then 'avril' When 5 then 'mai'
					 when 6 then 'juin' when 7 then 'juillet' when 8 then 'aout' when 9 then 'septembre' when 10 then 'Octobre'
					 when 11 then'Novembre' when 12 then 'Decembre' end Frenchmonth, 

					 case datepart(weekday,@currentdate)
					 when 1 then 'dimanche' when 2 then 'lundi' When 3 then 'mardi' when 4 then 'mercredi' 
					 when 5 then 'jeudi' when 6 then 'vendredi'  when 7 then 'samedi' end FrenchDayofWeek,


					 case datepart(month,@Currentdate)
					 when 1  then 'Sere'  When 2 then 'Èrèlè' When 3 then 'Erenà' When 4 then 'Ìgbé' When 5 then 'Ebibi'
					 when 6 then 'Òkúdu' when 7 then 'Agemo' when 8 then 'Ògún' when 9 then 'Owewe' when 10 then 'Owàrà'
					 when 11 then'Bélú' when 12 then 'Ope' end Yorubahmonth, 

					 case datepart(weekday,@currentdate)
					 when 1 then 'Aiku' when 2 then 'Ajé' When 3 then 'Isegun' when 4 then 'Ru' 
					 when 5 then 'Bo' when 6 then 'Eti'  when 7 then 'Abameta' end YorubaDayofWeek,
					 getdate() effectivedate					

  select @currentday=@currentday+1
END
END 
---select @generateyear generatedyear,@startdate startdate ,@enddate enddate, DATEDIFF(day,@startdate,@enddate) nofdays


select * from edw.dimDate

exec edw.spGenerateCalender 200

--------- Sales fact table

use tescaoltp

select * from SalesTransaction
select convert(date,getdate())

IF (select count(*) from tescaedw.edw.factsales)=0
	BEGIN 
		select Count(*) as OltpCount from SalesTransaction s
		where convert(date, transdate) <=  dateadd(day,-1,convert(date,getdate()))
	END
ELSE
	BEGIN 
		select Count(*) as OltpCount from SalesTransaction s		
		where convert(date, transdate) =  dateadd(day,-1,convert(date,getdate()))
	END


IF (select count(*) from tescaedw.edw.factsales)=0
	BEGIN 
		select s.TransactionID,s.TransactionNO, convert(date,s.TransDate)TransDate, datepart(hour,s.TransDate) Transhour,
		convert(date,s.OrderDate) OrderDate, datepart(hour,s.OrderDate) Orderhour, convert(date,s.DeliveryDate) DeliveryDate,
		ChannelID,CustomerID, EmployeeID, ProductID,StoreID,PromotionID, Quantity,TaxAmount,LineAmount,LineDiscountAmount,getdate() as Loaddate
		from SalesTransaction s
		where convert(date, transdate) <=  dateadd(day,-1,convert(date,getdate()))
	END
ELSE
	BEGIN 
		select s.TransactionID,s.TransactionNO, convert(date,s.TransDate)TransDate, datepart(hour,s.TransDate) Transhour,
		convert(date,s.OrderDate) OrderDate, datepart(hour,s.OrderDate) Orderhour, convert(date,s.DeliveryDate) DeliveryDate,
		ChannelID,CustomerID, EmployeeID, ProductID,StoreID,PromotionID, Quantity,TaxAmount,LineAmount,LineDiscountAmount,getdate() as Loaddate
		from SalesTransaction s
		where convert(date, transdate) =  dateadd(day,-1,convert(date,getdate()))
	END



-----metric fact sales




use tescastaging

 create table oltp.sales
 (
   transactionid int, 
   transactionNo nvarchar(50),
   TransDate date, 
   Transhour int,
   orderdate date,
   OrderHour int, 
   deliverydate date,
   channelID int, 
   CustomerID int, 
   EmployeeID int,
   Productid int,
   storeid int ,
   promotionid int,
   Quantity float,
   taxmamount float,
   lineamount float,
   LineDiscountAmount float,
   LoadDate datetime  default getdate(),
   constraint oltp_sales_pk  primary key(transactionid)   	 
 )

IF object_id('oltp.sales') is not null 
  Truncate table oltp.sales

use tescastaging

select  count(*) as StageCount from oltp.sales


select  transactionNo,TransDate,Transhour,orderdate,OrderHour,deliverydate,channelID,CustomerID,EmployeeID,Productid,storeid,
promotionid,Quantity,taxmamount,lineamount,LineDiscountAmount,getdate() as LoadDate
from oltp.sales

use tescaedw

select count(*) as PreCount  from edw.factsales

select count(*) as PostCount  from edw.factsales


create table  edw.factsales
(
 salesk bigint identity(1,1),
  transactionNo nvarchar(50),
  TransDatesk int, 
  Transhoursk int,
   Orderdatesk int,
   OrderHoursk int, 
   deliverydatesk int,
   channelIDsk int, 
   CustomerSk int, 
   EmployeeSk int,
   Productsk int,
   storesk int ,
   promotionsk int,
   Quantity float,
   taxmamount float,
   lineamount float,
   LineDiscountAmount float,
   loaddate datetime,
   constraint edw_factsales_sk  primary key(salesk),
   constraint edw_factsales_transdatesk_dimdate  foreign key(transDatesk) references edw.dimdate(businessdatekey),
   constraint edw_factsales_transhoursk_dimtime foreign key(Transhoursk)  references  edw.dimTime(hoursk),
   constraint edw_factsales_orderdatesk_dimdate  foreign key(orderDatesk) references edw.dimdate(businessdatekey),
   constraint edw_factsales_orderhoursk_dimtime foreign key(orderhoursk)  references  edw.dimTime(hoursk),
   constraint edw_factsales_deliverydatesk_dimdate  foreign key(deliveryDatesk) references edw.dimdate(businessdatekey),
   constraint edw_factsales_channelIDSk_dimPoschannel  foreign key(channelIDsk) references edw.dimPosChannel(ChannelIDSK),
   constraint edw_factsales_customersk_dimCustomer  foreign key(Customersk) references edw.dimCustomer(CustomerSk),
   constraint edw_factsales_employeesk_dimemployee foreign key(Employeesk) references edw.dimEmployee(employeesk),
   constraint edw_factsales_productsk_dimproduct foreign key(ProductSk) references edw.dimProduct(productsk),
   constraint edw_factsales_storesk_dimstore foreign key(StoreSk) references edw.dimStore(storesk),
   constraint edw_factsales_promotionsk_dimpromotion foreign key(promotionSk) references edw.dimpromotion(promotionsk),
)

----- Purchase fact -----------

select * from PurchaseTransaction

update PurchaseTransaction 
 set DeliveryDate=ShipDate, 
 ShipDate=DeliveryDate



 
 Use tescaoltp

 IF (select count(*) from tescaedw.edw.fact_purchases)=0 
	BEGIN
		select p.TransactionID,p.TransactionNO, convert(date,p.TransDate)TransDate,
		convert(date,p.OrderDate) OrderDate,  convert(date,p.DeliveryDate) DeliveryDate,convert(date,p.shipdate) shipdate,
		VendorID, EmployeeID, ProductID,StoreID, Quantity,TaxAmount,LineAmount,		
		  DateDiff(day, OrderDate, DeliveryDate) deliveryEfficiency,getdate() as Loaddate
		from PurchaseTransaction p Where convert(date,p.TransDate) <= DATEADD(day,-1,convert(date,getdate()))
	END 
 ELSE
	BEGIN 
	select p.TransactionID,p.TransactionNO, convert(date,p.TransDate)TransDate,
	convert(date,p.OrderDate) OrderDate,  convert(date,p.DeliveryDate) DeliveryDate,convert(date,p.shipdate) shipdate,
		VendorID, EmployeeID, ProductID,StoreID, Quantity,TaxAmount,LineAmount,
		DateDiff (day, OrderDate, DeliveryDate) deliveryEfficiency,getdate() as Loaddate
		from PurchaseTransaction p Where convert(date,p.TransDate) = DATEADD(day,-1,convert(date,getdate()))
	END

	---metric 

IF (select count(*) from tescaedw.edw.fact_purchases)=0 
	BEGIN
		select count(*) as OltpCount from PurchaseTransaction p Where convert(date,p.TransDate) <= DATEADD(day,-1,convert(date,getdate()))
	END 
 ELSE
	BEGIN 
	select count(*) as OltpCount from PurchaseTransaction p Where convert(date,p.TransDate) = DATEADD(day,-1,convert(date,getdate()))
END

use tescastaging

drop table oltp.purchases

If  OBJECT_ID('oltp.purchases') is not null
   TRUNCATE Table oltp.purchases

 create table oltp.purchases
 (
   transactionid int, 
   transactionNo nvarchar(50),
   TransDate date,    
   orderdate date,   
   deliverydate date,
   ShipDate date,
   VendorID int, 
   EmployeeID int,
   Productid int,
   storeid int ,   
   Quantity float,
   taxmamount float,
   lineamount float,
   deliveryEfficiency int,
   LoadDate datetime  default getdate(),
   constraint oltp_purchase_pk  primary key(transactionid)   	 
 )

 select count(*) as StageCount from oltp.purchases

 select transactionid,transactionNo,TransDate,orderdate,deliverydate,ShipDate,VendorID,EmployeeID,Productid,storeid,Quantity,taxmamount,
 lineamount,deliveryefficiency,LoadDate
 from oltp.purchases

 use tescaedw

 drop table edw.fact_purchases

 select * from   edw.fact_purchases
 select Count(*) as PreCount from   edw.fact_purchases

 select Count(*) as PostCount from   edw.fact_purchases

 create table edw.fact_purchases
 (
   purchasesk bigint identity(1,1),   
   transactionNo nvarchar(50),
   TransDatesk int,    
   orderdatesk int,   
   deliverydatesk int,
   ShipDatesk int,
   Vendorsk int, 
   Employeesk int,
   Productsk int,
   storesk int unique ,   
   Quantity float,
   taxmamount float,
   lineamount float,   
   deliveryEfficiency int,
   LoadDate datetime,
   constraint edw_fact_purchases_sk  primary key(purchasesk),
   constraint edw_fact_purchase_transdatesk_dimdate  foreign key(transDatesk) references edw.dimdate(businessdatekey),
   constraint edw_fact_purchase_orderdatesk_dimdate  foreign key(orderdatesk) references edw.dimdate(businessdatekey),
   constraint edw_fact_purchase_deliverydatesk_dimdate  foreign key(deliverydatesk) references edw.dimdate(businessdatekey),
   constraint edw_fact_purchase_shipdatesk_dimdate  foreign key(shipdatesk) references edw.dimdate(businessdatekey),
   constraint edw_fact_purchase_vendorsk_dimvendor  foreign key(vendorsk) references edw.dimvendor(vendorsk),
   constraint edw_fact_purchase_employeesk_dimemployee  foreign key(employeesk) references edw.dimemployee(employeesk),
   constraint edw_fact_purchase_productsk_dimproduct  foreign key(productsk) references edw.dimproduct(productsk),
   constraint edw_fact_purchase_storesk_dimstore  foreign key(storesk) references edw.dimstore(storesk),
 )

 select * from edw.dimDate




-- Sample dataset under Cansoft_Project'
