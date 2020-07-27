Logfile=/home/hduser/Retail_Project/execution.out
Logfileerror=/home/hduser/Retail_Project/executionerror.out
hivelog=/home/hduser/Retail_Project/hivelog.out
echo $(date) > $Logfile
echo $(date +"%y/%m/%d %H:%M:%S")"# Script execution started"
echo $(date +"%y/%m/%d %H:%M:%S")"# Mysql Tables creation started ordersproduct_ORIG ,custpayments_ORIG,empoffice"
mysql -u root -proot -D custdb -e "source /home/hduser/retailorders/ordersproduct_ORIG.sql;
source /home/hduser/retailorders/custpayments_ORIG.sql;
source /home/hduser/retailorders/empoffice.sql;
select count(1) employees from empoffice.employees;
select count(1) orders from ordersproducts.orders;
select count(1) customers from custpayments.customers;"
echo $(date +"%y/%m/%d %H:%M:%S")"# Mysql Tables creation completed ordersproduct_ORIG ,custpayments_ORIG,empoffice"
#echo "# Create password file to store Mysqldb credenbtials -- one time setup"
#echo -n "root" > ~/root.password
#hadoop fs -mkdir /user/hduser/retailorders/
#hadoop fs -put ~/root.password /user/hduser/retailorders/root.password
#hadoop dfs -chown 400 /user/hduser/retailorders/root.password
#echo "# Password file creation completed"
echo "import employees table data into hdfs"
sqoop --options-file /home/hduser/retailorders/empofficeoption --password-file /user/hduser/retailorders/root.password -table employees -m 2 --delete-target-dir --target-dir employees/ -fields-terminated-by '~' --lines-terminated-by '\n' 2>$Logfileerror
employees_cnt=$(hadoop fs -cat /user/hduser/employees/* | wc -l)
echo $(date +"%y/%m/%d %H:%M:%S")"#Sqoop Command Data Import Completed employees table to hdfs path /user/hduser/employees count $employees_cnt"
echo $(date +"%y/%m/%d %H:%M:%S")"import offices  table data into hdfs"
sqoop --options-file /home/hduser/retailorders/empofficeoption --password-file /user/hduser/retailorders/root.password -table offices -m 1 --delete-target-dir --target-dir offices/ --fields-terminated-by '~' --lines-terminated-by '\n' 2>>$Logfileerror
offices_cnt=$(hadoop fs -cat /user/hduser/offices/* | wc -l)
echo $(date +"%y/%m/%d %H:%M:%S")"#Sqoop Command Data Import Completed offices table to hdfs path /user/hduser/offices count $offices_cnt"
echo $(date +"%y/%m/%d %H:%M:%S")"#Import the Customer and payments data joined with most of the options used for better optimization and best practices"
sqoop --options-file /home/hduser/retailorders/custoption --password-file /user/hduser/retailorders/root.password --boundary-query " select min(customerNumber), max(customerNumber) from payments " --query 'select c.customerNumber, upper(c.customerName),c.contactFirstName,c.contactLastName,c.phone,c.addressLine1,c.city,c.state,c.postalCode,c.country ,c.salesRepEmployeeNumber,c.creditLimit ,p.checknumber,p.paymentdate,p.amount from customers c inner join payments p on c.customernumber=p.customernumber where $CONDITIONS' --split-by c.customernumber --delete-target-dir --target-dir custdetails/2016-10/ --null-string 'NA' --direct --num-mappers 2 --fields-terminated-by '~' --lines-terminated-by '\n' 2>>$Logfileerror
custdetails_cnt=$(hadoop fs -cat /user/hduser/custdetails/2016-10/* | wc -l)
echo $(date +"%y/%m/%d %H:%M:%S")"#Sqoop Command Data Import Completed offices table to hdfs path /user/hduser/custdetails/2016-10/ count $custdetails_cnt"
echo $(date +"%y/%m/%d %H:%M:%S")"#Import the orders, orderdetail and products data joined with most of the options used for better optimization and best practices"
sqoop --options-file /home/hduser/retailorders/ordersoption --password-file /user/hduser/retailorders/root.password --boundary-query "select min(customerNumber), max(customerNumber) from orders" --query 'select o.customernumber,o.ordernumber,o.orderdate,o.shippeddate,o.status,o.comments,od.productcode,od.quantityordered,od.priceeach,od.orderlinenumber,p.productCode,p.productName,p.productLine,p.productScale,p.productVendor,p.productDescription,p.quantityInStock,p.buyPrice,p.MSRP from orders o inner join orderdetails od on o.ordernumber=od.ordernumber inner join products p on od.productCode=p.productCode where $CONDITIONS' --split-by o.customernumber --delete-target-dir --target-dir orderdetails/2016-10/ --null-string 'NA' --direct --num-mappers 4 --fields-terminated-by '~' --lines-terminated-by '\n' ;2>>$Logfileerror
orderdetails_cnt=$(hadoop fs -cat /user/hduser/orderdetails/2016-10/* | wc -l)
echo $(date +"%y/%m/%d %H:%M:%S")"#Sqoop Command Data Import Completed rders, orderdetail and products table to hdfs path /user/hduser/orderdetails/2016-10/ count $orderdetails_cnt"
echo $(date +"%y/%m/%d %H:%M:%S")"# started executing 1_Retail_Hive.hql script,creating managed tables"
hive -f /home/hduser/Retail_Project/1_Retail_Hive.hql &>$hivelog
echo $(date +"%y/%m/%d %H:%M:%S")"# completed executing 1_Retail_Hive.hql script,created managed tables"
echo $(date +"%y/%m/%d %H:%M:%S")"#deleting /user/hduser/custorders/custdetpartbuckext directory"
hadoop fs -rmr /user/hduser/custorders/custdetpartbuckext 
echo $(date +"%y/%m/%d %H:%M:%S")"# Started executing 2_Retail_Hive_partition_bucketing.hql script, creating External tables with partition/bucketing usecases :"
hive -f /home/hduser/Retail_Project/2_Retail_Hive_partition_bucketing.hql &>>$hivelog
echo $(date +"%y/%m/%d %H:%M:%S")"# completed executing 2_Retail_Hive_partition_bucketing.hql script, created External tables with partition/bucketing usecases :"
echo $(date +"%y/%m/%d %H:%M:%S")"# execution started 3_retail_Hive_orc_text_index.hql script -- started Createting a final external tables with orc and text format & Index creation for high cardinal values."
hive -f /home/hduser/Retail_Project/3_retail_Hive_orc_text_index.hql &>>$hivelog
echo $(date +"%y/%m/%d %H:%M:%S")"# script execution completed started 3_retail_Hive_orc_text_index.hql "
echo $(date +"%y/%m/%d %H:%M:%S")"#droping directory /user/hduser/custmart/"
hadoop fs -rmr /user/hduser/custmart/
echo $(date +"%y/%m/%d %H:%M:%S")"# execution started 4_viewship_pattern_and_Frustration_Scoring.hql script -- Customer Website viewship pattern and Frustration Scoring use case"
hive -f /home/hduser/Retail_Project/4_viewship_pattern_and_Frustration_Scoring.hql &>>$hivelog
echo $(date +"%y/%m/%d %H:%M:%S")"# execution completed 4_viewship_pattern_and_Frustration_Scoring.hql script -- Customer Website viewship pattern and Frustration Scoring use case"
echo $(date +"%y/%m/%d %H:%M:%S")"# create customer_reports.customer_frustration_level database and tables"
mysql -u root -proot -D custdb -e "create database if not exists customer_reports;
drop table if exists customer_reports.customer_frustration_level;
CREATE TABLE customer_reports.customer_frustration_level ( customernumber varchar(200), total_siverity float,frustration_level varchar(100) );"
echo $(date +"%y/%m/%d %H:%M:%S")"#  customer_reports.customer_frustration_level database and tables creation completed ; data export started from dhfs to mysql using sqoop"
sqoop export --connect jdbc:mysql://localhost/customer_reports --username root --password root --table customer_frustration_level --export-dir /user/hduser/custmartfrustration/ 2>>$Logfileerror
#customer_frustration_level_cnt = $(mysql -u root -proot -D custdb -e " select count(1) from customer_reports.customer_frustration_level")
customer_frustration_level_cnt=$(mysql customer_reports -u root -proot<<<"select count(1) from customer_reports.customer_frustration_level")
echo $(date +"%y/%m/%d %H:%M:%S")"# Data load completed customer_reports.customer_frustration_level total count = $customer_frustration_level_cnt"
echo $(date +"%y/%m/%d %H:%M:%S")"#Script execution completed successfully "
