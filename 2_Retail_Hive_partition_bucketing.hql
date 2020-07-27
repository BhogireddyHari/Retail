drop database if exists retail_mart cascade; 
create database retail_mart; 
use retail_mart;

set hive.enforce.bucketing = true ;
set map.reduce.tasks = 3;
set hive.exec.dynamic.partition.mode=nonstrict; 
set hive.exec.dynamic.partition=true;

select "creating external table custdetpartbuckext started";

drop table if exists custdetpartbuckext;
create external table custdetpartbuckext (customernumber STRING, customername STRING, contactfullname string, 
address struct<addressLine1:string,city:string,state:string,postalCode:bigint,country:string,phone:bigint>,creditlimit float,checknum string,checkamt int)
 partitioned by (paymentdate date) clustered by (customernumber) INTO 3 buckets row format delimited fields terminated by '~' collection items terminated by '$'
stored as textfile location '/user/hduser/custorders/custdetpartbuckext';

select "create external table custdetpartbuckext completed and data insertion started ustdetpartbuckext partition(paymentdate) using retail_stg.custdetstg table ";

insert into table custdetpartbuckext partition(paymentdate) select customernumber,customername, contactfullname, address ,creditlimit,checknum,checkamt,paymentdate from retail_stg.custdetstg ;
select " data insertion ustdetpartbuckext comnpleted.
#started creating external table orddetpartbuckext";


drop table if exists orddetpartbuckext;

create external table orddetpartbuckext(customernumber STRING, ordernumber STRING, shippeddate date,
status string, comments string,productcode string,quantityordered int,priceeach decimal(10,2),orderlinenumber int,productName STRING,productLine STRING, productScale STRING,productVendor STRING,productDescription STRING,quantityInStock int,buyPrice decimal(10,2),MSRP decimal(10,2)) partitioned by (orderdate date)
clustered by (customernumber) INTO 3 buckets
row format delimited fields terminated by '~' collection items terminated by '$'
stored as textfile location '/user/hduser/custorders/orddetpartbuckext';

select "create external table orddetpartbuckext completed and data insertion started ustdetpartbuckext partition(paymentdate) using retail_stg.orddetstg table ";

insert into table retail_mart.orddetpartbuckext partition(orderdate) select customernumber,ordernumber, shippeddate,status,comments,productcode,quantityordered ,priceeach ,orderlinenumber ,productName ,productLine,productScale,productVendor,productDescription,quantityInStock,buyPrice,MSRP,orderdate from retail_stg.orddetstg ;

select " data insertion orddetpartbuckext comnpleted."

