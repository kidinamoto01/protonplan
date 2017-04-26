#!/bin/sh

# Author : Jay
# Date : 2017/1/17.
# Func : hive stat



namenode=hdfs://10.20.10.2:8020


#获取当前时间绝对秒数
currenttime=`date +%s`
#获取当前时间 yyyy-MM-dd HH:mm:ss 格式
nowtime=`date --date='0 days ago - 5 min' "+%Y-%m-%d %H:%M:%S"`
#获取1小时前时间 yyyy-MM-dd HH:mm:ss 格式
onehourage=`date --date='1 hours ago' "+%Y-%m-%d %H:%M:%S"`
#获取1天前时间 yyyy-MM-dd HH:mm:ss 格式
onedayage=`date --date='1 day ago' "+%Y-%m-%d %H:%M:%S"`
#获取7天前时间 yyyy-MM-dd HH:mm:ss 格式
sevendayage=`date --date='7 day ago' "+%Y-%m-%d %H:%M:%S"`
#获取30天前时间 yyyy-MM-dd HH:mm:ss 格式
onemonthage=`date --date='30 day ago' "+%Y-%m-%d %H:%M:%S"`

log()
{
	datetime=$( date  "+%F %T" )
	echo "[ ${datetime} ] : $*"
}

#创建web_middle_attack
function create_web_middle_attack(){
sql="use sheshou;
create table IF NOT EXISTS web_middle_attack (attack_type string,collecttime string,country_code string,country_name String
)STORED AS PARQUET LOCATION '/sheshou/data/parquet/web_middle_attack';"
echo $sql
result=`hive -e "$sql">>/tmp/hivetest.log`
}


#创建 middle_result
function create_middle_result() {
sql="use sheshou;
create table IF NOT EXISTS  middle_result (attack_type string,country_code string,country_name string,
  current_count string,last_hour_count string,last_24hour_count string,last_7days_count string,
  last_month_count string,history_count string)STORED AS PARQUET  LOCATION '/sheshou/data/parquet/middle_result';"
echo $sql
result=`hive -e "$sql">>/tmp/hivetest.log`
}



#清洗webmiddle,统计攻击，把结果存入web_middle_attack
function cleanwebmiddle() {
    sql="use sheshou;
set hive.execution.engine=mr;
add jar  ${namenode}/tmp/HiveUDFString.jar;
CREATE  FUNCTION sql_attack_filter AS 'com.andlinks.hive.function.AttackClassifySqlUDF';
CREATE  FUNCTION command_attack_filter AS 'com.andlinks.hive.function.AttackClassifyCommandUDF';
CREATE  FUNCTION package_attack_filter AS 'com.andlinks.hive.function.AttackClassifyPackageUDF';
CREATE  FUNCTION pathscan_attack_filter AS 'com.andlinks.hive.function.AttackClassifyPathScanUDF';
CREATE  FUNCTION struts_attack_filter AS 'com.andlinks.hive.function.AttackClassifyStrutsUDF';
CREATE  FUNCTION vague_attack_filter AS 'com.andlinks.hive.function.AttackClassifyVagueUDF';
CREATE  FUNCTION xss_attack_filter AS 'com.andlinks.hive.function.AttackClassifyXssUDF';
insert into table web_middle_attack
select a.attack_type,a.collecttime,a.country_code,a.country_name from (select case when (sql_attack_filter(requestpage))!='Nothing' then sql_attack_filter(requestpage) when (command_attack_filter(requestpage))!='Nothing' then command_attack_filter(requestpage) when (package_attack_filter(requestpage))!='Nothing' then package_attack_filter(requestpage) when (pathscan_attack_filter(requestpage))!='Nothing' then pathscan_attack_filter(requestpage) when (struts_attack_filter(requestpage))!='Nothing' then struts_attack_filter(requestpage) when (vague_attack_filter(requestpage))!='Nothing' then vague_attack_filter(requestpage) when (xss_attack_filter(requestpage))!='Nothing' then xss_attack_filter(requestpage) else 0 end as attack_type,collecttime,destcountrycode as country_code,destcountry as country_name from webmiddle ) a where a.attack_type !=0;"
echo $sql
hive -e "$sql" >>/tmp/hivetest.log
}


#对attack_list和web_middle_attack按时间片段进行统计，结果存middle_result
function stat_middle_result() {
   sql="use sheshou;set hive.execution.engine=mr;
   select * from (select attack_type,dst_country_code,dst_country,count(attack_time) as current_count,0 as last_hour_count,0 as
last_24hour_count,0 as last_7days_count,0 as last_month_count,0 as history_count from attack_list where unix_timestamp(attack_time)
> unix_timestamp('$nowtime') group by dst_country_code,dst_country,attack_type union all select attack_type,dst_country_code,dst_country,0 as current_count,count(attack_time) as last_hour_count,0 as
last_24hour_count,0 as last_7days_count,0 as last_month_count,0 as history_count from attack_list where unix_timestamp(attack_time)
> unix_timestamp('$onehourage') group by dst_country_code,dst_country,attack_type union all select attack_type,dst_country_code,dst_country,0 as current_count,0 as last_hour_count,count(attack_time) as last_24hour_count
,0 as last_7days_count,0 as last_month_count,0 as history_count from attack_list where unix_timestamp(attack_time)
> unix_timestamp('$onedayage') group by dst_country_code,dst_country,attack_type union all select attack_type,dst_country_code,dst_country,0 as current_count,0 as last_hour_count,0 as last_24hour_count,
count(attack_time) as last_7days_count,0 as last_month_count,0 as history_count from attack_list where unix_timestamp(attack_time)
> unix_timestamp('$sevendayage') group by dst_country_code,dst_country,attack_type union all select attack_type,dst_country_code,dst_country,0 as current_count,0 as last_hour_count,0 as last_24hour_count,
0 as last_7days_count,count(attack_time) as last_month_count,0 as history_count from attack_list where unix_timestamp(attack_time)
> unix_timestamp('$onemonthage') group by dst_country_code,dst_country,attack_type union all select attack_type,dst_country_code,dst_country,0 as current_count,0 as last_hour_count,0 as last_24hour_count,
0 as last_7days_count,0 as last_month_count,count(attack_time) as history_count from attack_list  group by dst_country_code,dst_country,attack_type union all
select attack_type,country_code,country_name,count(collecttime) as current_count,0 as last_hour_count,0 as
last_24hour_count,0 as last_7days_count,0 as last_month_count,0 as history_count from web_middle_attack where unix_timestamp(collecttime)
> unix_timestamp('$nowtime') group by country_code,country_name,attack_type union all select attack_type,country_code,country_name,0 as current_count,count(collecttime) as last_hour_count,0 as
last_24hour_count,0 as last_7days_count,0 as last_month_count,0 as history_count from web_middle_attack where unix_timestamp(collecttime)
> unix_timestamp('onehourage') group by country_code,country_name,attack_type union all select attack_type,country_code,country_name,0 as current_count,0 as last_hour_count,count(collecttime) as last_24hour_count
,0 as last_7days_count,0 as last_month_count,0 as history_count from web_middle_attack where unix_timestamp(collecttime)
> unix_timestamp('$onedayage') group by country_code,country_name,attack_type union all select attack_type,country_code,country_name,0 as current_count,0 as last_hour_count,0 as last_24hour_count,
count(collecttime) as last_7days_count,0 as last_month_count,0 as history_count from web_middle_attack where unix_timestamp(collecttime)
> unix_timestamp('$sevendayage') group by country_code,country_name,attack_type union all select attack_type,country_code,country_name,0 as current_count,0 as last_hour_count,0 as last_24hour_count,
0 as last_7days_count,count(collecttime) as last_month_count,0 as history_count from web_middle_attack where unix_timestamp(collecttime)
> unix_timestamp('$onemonthage') group by country_code,country_name,attack_type union all select attack_type,country_code,country_name,0 as current_count,0 as last_hour_count,0 as last_24hour_count,
0 as last_7days_count,0 as last_month_count,count(collecttime) as history_count from web_middle_attack  group by country_code,country_name,attack_type)c;"
echo $sql
result=`hive -e "$sql">>/tmp/hivetest.log`
}

#对 middle_result 结果进行汇总，生成 attack_geo_distribution 表数据
function stat_attack_geo_distribution() {
   sql="use sheshou;set hive.execution.engine=mr;insert overwrite table attack_geo_distribution
select '$currenttime',country_code,country_name,floor(sum(last_24hour_count)),floor(sum(last_7days_count)),floor(sum(last_month_count))
from middle_result group by country_code,country_name;"
echo $sql
result=`hive -e "$sql"`
}

#对 middle_result 结果进行汇总，生成 attack_type_stat 表数据
function stat_attack_type_stat() {
    sql="use sheshou;set hive.execution.engine=mr;insert overwrite table attack_type_stat
select '$currenttime',attack_type,floor(sum(current_count)),floor(sum(last_hour_count)),floor(sum(last_24hour_count))
,floor(sum(last_7days_count)),floor(sum(last_month_count)),floor(sum(history_count)),'0','0','0' from middle_result group by attack_type;"
echo $sql
result=`hive -e "$sql"`
}

#统计数据插入 dayly_stat
function stat_dayly_stat() {
    sql="use sheshou;set hive.execution.engine=mr;insert into table dayly_stat
	select day,'0','0','0',floor(sum(attack_num)),floor(sum(attack_num)),'0','0','0','0','0' from (select to_date(attack_time) as day,count(attack_time) as attack_num from attack_list group by to_date(attack_time)
union all select to_date(collecttime) as day,count(collecttime) as attack_num from web_middle_attack group by to_date(collecttime)) c group by day;"
echo $sql
result=`hive -e "$sql"`


}

#统计数据出入 hour_stat 中
function stat_hour_stat() {
      sql="use sheshou;set hive.execution.engine=mr;insert overwrite table hourly_stat
	  select hour,'0','0','0',floor(sum(attack_num)),floor(sum(attack_num)),'0','0','0','0','0' from (select from_unixtime(unix_timestamp(attack_time),'yyyy-MM-dd HH:00') as hour ,count(attack_time) as attack_num from attack_list where unix_timestamp(attack_time) > unix_timestamp('$onedayage') group by from_unixtime(unix_timestamp(attack_time),'yyyy-MM-dd HH:00') union all
select from_unixtime(unix_timestamp(collecttime),'yyyy-MM-dd HH:00') as hour,count(collecttime) as attack_num from web_middle_attack where unix_timestamp(collecttime) > unix_timestamp('$onedayage') group by from_unixtime(unix_timestamp(collecttime),'yyyy-MM-dd HH:00') ) c group by hour;"
echo $sql
result=`hive -e "$sql"`
}
create_web_middle_attack
create_middle_result
cleanwebmiddle
stat_middle_result
stat_attack_geo_distribution
stat_attack_type_stat
stat_dayly_stat
stat_hour_stat
