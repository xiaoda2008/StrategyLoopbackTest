-- 创建数据库及相关表格，并初始化

-- 创建数据库
-- create database tsdata;

-- 选择数据库
use tsdata;

/*
-- 清理掉老数据
select concat( 'drop table ', table_name, ';' ) 
from information_schema.tables 
where table_name like 's_kdata_%'>"d:\init1.sql";
source d:\init1.sql;

select concat( 'drop table ', table_name, ';' ) 
from information_schema.tables 
where table_name like 's_adjdata_%'>"d:\init2.sql";
source d:\init2.sql;

select concat( 'drop table ', table_name, ';' ) 
from information_schema.tables 
where table_name like 's_balancesheet_%'>"d:\init3.sql";
source d:\init3.sql;

select concat( 'drop table ', table_name, ';' ) 
from information_schema.tables 
where table_name like 's_cashflow_%'>"d:\init4.sql";
source d:\init4.sql;

select concat( 'drop table ', table_name, ';' ) 
from information_schema.tables 
where table_name like 's_income_%'>"d:\init5.sql";
source d:\init5.sql;


drop table u_dataupdatelog;
drop table u_trade_cal;
drop table u_stock_list;
*/

-- 建立用于记录数据库更新进度的表
create table u_dataupdatelog (
content_name varchar(100),
content varchar(200) not null default '',
comments varchar(300) not null default ''
);

-- 初始化数据更新表：
-- 最近一次成功的、完整的数据更新，在完成所有数据更新任务后才更新该字段
insert into u_dataupdatelog (content_name,content,comments) values ('last_total_update_time','','the date of last total data update');
-- 最近一次数据更新（不论是否完成完整的数据更新），在每个数据更新后都更新该字段
insert into u_dataupdatelog (content_name,content,comments) values ('last_update_time','','the date of last data update');
-- 财务报表最新更新的股票代码，每个股票代码更新财务报表数据后都更新该字段
insert into u_dataupdatelog (content_name,content,comments) values ('finance_report_update_to','','the stock code of last update of finance report');
-- k线数据最新更新的股票代码，每个股票的k线数据更新后都更新该字段
insert into u_dataupdatelog (content_name,content,comments) values ('kdata_update_to','','the stock code of last update of k data');
-- 复权因子数据最新更新的股票代码，每个股票的复权因子数据更新后都更新该字段
insert into u_dataupdatelog (content_name,content,comments) values ('adjdata_update_to','','复权因子数据最后一个更新的股票代码');


