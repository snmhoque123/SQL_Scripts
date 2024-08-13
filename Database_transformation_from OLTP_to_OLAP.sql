/*

Sample Database transfer script from OLTP to Staging environment where entities are 
Purchasetransaction, SalesTransaction, Sore, City, State, Customer, Department, Employee, MaritalStatus, Product, Promotion, PromotionType and Vendor 
*/

use cansoft_oltp

Select  * from PurchaseTransaction

----join SalesTransaction with Promotion, and PromotionType tables for staging
	select * from SalesTransaction s  inner join Promotion  p  on p.PromotionID=s.PromotionID
	inner join PromotionType t  on t.PromotionTypeID=p.PromotionTypeID

--create schema for staging

use cansoft_staging

create schema oltp

---create schema for edw

use cansoft_edw
create schema edw

---- inner join the product table with department
use cansoft_oltp

select p.ProductID, p.Product, p.ProductNumber, p.UnitPrice, d.Department,getdate() as LoadDate  from Product p
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
   constraint oltp_product_pk  primary key(productid)
)

-----check Product data
select *  from oltp.Product

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
   constraint edw_dimproduct_sk  primary key(productsk)
)

--- store -------
use tescaoltp
select s.StoreID,s.StoreName AS Store,s.StreetAddress, c.CityName as City,st.State, getdate() as LoadDate  from Store s
inner join City c on s.CityID=c.CityID
inner join State st on st.StateID=c.StateID


select count(*) as OltpCount  from Store s
inner join City c on s.CityID=c.CityID
inner join State st on st.StateID=c.StateID


------- Create Time dimension ------
alter procedure edw.spTime AS
 BEGIN
 set nocount on
 declare @starthour int=0 
 IF (select count(*) from edw.dimTime) >0 
    TRUNCATE Table edw.dimTime

-----create day
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

------

