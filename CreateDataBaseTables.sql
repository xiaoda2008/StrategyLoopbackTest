#表格式
create table u_dataupdatelog (
content_name varchar(100),
content varchar(200)
);

#表内容：
insert into u_dataupdatelog (content_name,content) values ("last_total_update_time", "2000-01-01 00:00:00")#最近一次成功的、完整的数据更新，在完成所有数据更新任务后才更新该字段

insert into u_dataupdatelog (content_name,content) values ("last_update_time", "2000-01-01 00:00:00")#最近一次数据更新（不论是否完成完整的数据更新），在每个数据更新后都更新该字段

insert into u_dataupdatelog (content_name,content) values ("finance_report_update_to","000001") #财务报表最新更新的股票代码，每个股票代码更新财务报表数据后都更新该字段

insert into u_dataupdatelog (content_name,content) values ("kdata_update_to","000001") #k线数据最新更新的股票代码，每个股票的k线数据更新后都更新该字段
