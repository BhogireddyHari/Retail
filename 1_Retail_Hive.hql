create database if not exists retail_stg;
use retail_stg;
drop table if exists custdetails;
create table custdetails(customerNumber string,customerName string,contactFirstName string,contactLastName string,phone bigint,addressLine1 string,city string,state string,postalCode bigint,country string,salesRepEmployeeNumber string,creditLimit float,checknumber string,paymentdate date, checkamt float)
row format delimited fields terminated by '~'
location '/user/hduser/custdetails/2016-10';

Select  "#-----------Table creation completed custdetails with location '/user/hduser/custdetails/2016-10";
drop table if exists custdetstg;
create table custdetstg(customernumber STRING, customername STRING, contactfullname string, address struct<addressLine1:string,city:string,state:string,postalCode:bigint,country:string,phone:bigint>, creditlimit float,checknum string,checkamt int,paymentdate date)
row format delimited fields terminated by '~'
stored as textfile;

Select  "#-----------Table creation custdetstg custdetails";

insert overwrite table custdetstg select customernumber,contactfirstname, concat(contactfirstname, ' ', contactlastname) , named_struct('addressLine1', addressLine1, 'city', city, 'state', state, 'postalCode', postalCode, 'country', country, 'phone',phone), creditlimit,checknumber,checkamt,paymentdate from custdetails;

Select  "#-----------data inserted into  custdetstg table using custdetails table";

truncate table orddetstg;

Select  "#-----------table truncated orddetstg";

create external table if not exists orddetstg (customerNumber string, ordernumber string,orderdate date, shippeddate date,status string, comments string, quantityordered int,priceeach decimal(10,2),orderlinenumber int, productcode string, productName STRING,productLine STRING, productScale STRING,productVendor STRING,productDescription STRING,quantityInStock int,buyPrice decimal(10,2),MSRP decimal(10,2))
row format delimited fields terminated by '~'
location '/user/hduser/orderdetails/2016-10/';

Select  "#-----------eorddetstg external table created with location /user/hduser/orderdetails/2016-10/";