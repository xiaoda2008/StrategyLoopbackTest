#初始化所有表

#表格式
create table u_dataupdatelog (
content_name varchar(100),
content varchar(200)
);

#表内容：
insert into u_dataupdatelog (content_name,content) values ('last_total_update_time','','最近一次完整数据更新日期')#最近一次成功的、完整的数据更新，在完成所有数据更新任务后才更新该字段
insert into u_dataupdatelog (content_name,content) values ('last_update_time','','最近一次更新时间，可能因为失败而结束')#最近一次数据更新（不论是否完成完整的数据更新），在每个数据更新后都更新该字段
insert into u_dataupdatelog (content_name,content) values ('finance_report_update_to','财务数据最后一个更新的股票代码') #财务报表最新更新的股票代码，每个股票代码更新财务报表数据后都更新该字段
insert into u_dataupdatelog (content_name,content) values ('kdata_update_to','k线数据最后一个更新的股票代码') #k线数据最新更新的股票代码，每个股票的k线数据更新后都更新该字段
insert into u_dataupdatelog (content_name,content) values ('adjdata_update_to','复权因子数据最后一个更新的股票代码') #复权因子数据最新更新的股票代码，每个股票的复权因子数据更新后都更新该字段


#对股票清单表，进行扩充，标记沪深300、上证50、中证100、深证100等指数标记

alter table u_stock_list add column hs300 boolean not null default 0; --添加表列：沪深300
alter table u_stock_list add column sh50 boolean not null default 0; --添加表列：上证50
alter table u_stock_list add column zz100 boolean not null default 0; --添加表列：中证100
alter table u_stock_list add column sz100 boolean not null default 0; --添加表列：深证100



#上证股票：以SH结尾的
#深证股票：以SZ结尾的
#创业板股票：以300开头的
