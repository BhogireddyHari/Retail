use retail_stg; 

drop table if exists order_rate;
select "# start create retail_stg.order_rate table";
create table retail_stg.order_rate (rid int,orddesc varchar(200),comp_cust varchar(10),siverity int) row format delimited fields terminated by ',';
select "# table created retail_stg.order_rate table and data insert started /home/hduser/retailorders/orders_rate.csv";
load data local inpath '/home/hduser/retailorders/orders_rate.csv' overwrite into table order_rate;
select "# data load completed  /home/hduser/retailorders/orders_rate.csv
# table creatipon started orddetstg";

drop table if exists orddetstg;
create table retail_stg.orddetstg (customernumber string,comments string,pagenavigation array
<string>,pagenavigationidx array <int>) row format delimited fields terminated by ',' collection items terminated by '$';
select "#data load started retail_stg.orddetstg ";
load data local inpath '/home/hduser/retailorders/stgdata' overwrite into table retail_stg.orddetstg; 

drop table if exists retail_mart.cust_navigation;

select "# start create retail_mart.cust_navigation table";
create external table retail_mart.cust_navigation (customernumber string,navigation_pg string,navigation_index int) row format delimited fields terminated by ',' location '/user/hduser/custmart/';
select "# data load completed  retail_mart.cust_navigation table";

insert overwrite table retail_mart.cust_navigation select customernumber,pgnavigation as pagenavig
,pgnavigationidx as pagenavigindex from retail_stg.orddetstg lateral view posexplode(pagenavigation) exploded_data1 as x, pgnavigation lateral view posexplode(pagenavigationidx) exploded_data2 as y, pgnavigationidx where x=y;
select "# data load completed retail_mart.cust_navigation:";

select customernumber,navigation_pg ,row_number() over (partition by customernumber order by navigation_index desc) as visit_number from retail_mart.cust_navigation;

select "# create table retail_mart.cust_frustration_level";
create external table retail_mart.cust_frustration_level (customernumber string,total_siverity int,frustration_level string) row format delimited fields terminated by ',' location '/user/hduser/custmartfrustration/';
insert overwrite table retail_mart.cust_frustration_level select customernumber,total_siverity,case when total_siverity between -10 and -3 then 'highly frustrated' when total_siverity between -2 and -1 then 'low frustrated' when total_siverity = 0 then 'neutral' when total_siverity between 1 and 2 then 'happy' when total_siverity between 3 and 10 then 'overwhelming' else 'unknown' end as customer_frustration_level from ( select customernumber,sum(siverity) as total_siverity from ( select o.customernumber,o.comments,r.orddesc,siverity from retail_stg.orddetstg o left outer join order_rate r where o.comments like concat('%',r.orddesc,'%')) temp1
group by customernumber) temp2;

select "# data load  retail_mart.cust_frustration_level completed";