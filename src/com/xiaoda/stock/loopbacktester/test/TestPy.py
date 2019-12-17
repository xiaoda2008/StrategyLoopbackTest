'''
Created on 2019年10月18日

@author: picc
'''
import tushare
import math
from datetime import datetime as dt
import pandas as pd
import datetime
import numpy
from pathlib import Path
from com.xiaoda.stock.loopbacktester.utils.ParamUtils import OUTPUTDIR
import pandas

import traceback
import time
import sqlalchemy
import os
import sys
from cmath import isnan
from sqlalchemy.util.langhelpers import NoneType
from pandas.core.frame import DataFrame
from com.xiaoda.stock.loopbacktester.utils.FileUtils import FileProcessor
from sqlalchemy.orm import sessionmaker, scoped_session

import numpy as np
import datetime
from scipy import optimize
import copy

from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlProcessor
from com.xiaoda.stock.loopbacktester.utils.StockDataUtils import StockDataProcessor


from timeit import default_timer as timer



import baostock as bs
import pandas as pd





import multiprocessing

from multiprocessing import Process



def testFun(i):
    print("test Func%s"%(i))
    print(os.getpid())


if __name__ == '__main__':
    print("主进程执行中>>> pid={0}".format(os.getpid()))
    ps=[]
    # 创建子进程实例
    for i in range(2):
        p=Process(target=testFun,name="worker%s"%(i),args=(i,))
        ps.append(p)
    # 开启进程
    for i in range(2):
        ps[i].start()
    # 阻塞进程
    for i in range(2):
        ps[i].join()
    print("主进程终止")
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    def gcd(pair):
        a, b = pair
        low = min(a, b)
        for i in range(low, 0, -1):
            if a % i == 0 and b % i == 0:
                return i
    
    numbers = [
        (1963309, 2265973), (1879675, 2493670), (2030677, 3814172),
        (1551645, 2229620), (1988912, 4736670), (2198964, 7876293)
    ]
    import time
    
    start = time.time()
    results = list(map(gcd, numbers))
    end = time.time()
    
    print('Took %.3f seconds.' % (end - start))
    
    tushare.set_token('221f96cece132551e42922af6004a622404ae812e41a3fe175391df8')
    
    sdDataAPI = tushare.pro_api()
    
    bs = sdDataAPI.balancesheet(ts_code='001914.SZ',start_date='20150101',end_date=dt.now().strftime('%Y%m%d'))
     
    
    print()
    
    
    
    lg = bs.login()
    # 显示登陆返回信息
    print('login respond error_code:'+lg.error_code)
    print('login respond  error_msg:'+lg.error_msg)
    
    #### 获取历史K线数据 ####
    # 详细指标参数，参见“历史行情指标参数”章节
    rs = bs.query_history_k_data_plus("sh.600000",
        "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST",
        start_date='2017-06-01', end_date='2017-12-31', 
        frequency="d", adjustflag="3") #frequency="d"取日k线，adjustflag="3"默认不复权
    print('query_history_k_data_plus respond error_code:'+rs.error_code)
    print('query_history_k_data_plus respond  error_msg:'+rs.error_msg)
    
    #### 打印结果集 ####
    data_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        data_list.append(rs.get_row_data())
    result = pd.DataFrame(data_list, columns=rs.fields)
    #### 结果集输出到csv文件 ####
    result.to_csv("D:/history_k_data.csv", encoding="gbk", index=False)
    print(result)
    
    #### 登出系统 ####
    bs.logout()
    
    
    # 登陆系统
    lg = bs.login()
    # 显示登陆返回信息
    print('login respond error_code:'+lg.error_code)
    print('login respond  error_msg:'+lg.error_msg)
    
    # 获取行业分类数据
    rs = bs.query_stock_industry()
    # rs = bs.query_stock_basic(code_name="浦发银行")
    print('query_stock_industry error_code:'+rs.error_code)
    print('query_stock_industry respond  error_msg:'+rs.error_msg)
    
    # 打印结果集
    industry_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        industry_list.append(rs.get_row_data())
    result = pd.DataFrame(industry_list, columns=rs.fields)
    # 结果集输出到csv文件
    result.to_csv("D:/stock_industry.csv", encoding="gbk", index=False)
    print(result)
    
    # 登出系统
    bs.logout()# 登陆系统
    lg = bs.login()
    # 显示登陆返回信息
    print('login respond error_code:'+lg.error_code)
    print('login respond  error_msg:'+lg.error_msg)
    
    # 获取行业分类数据
    rs = bs.query_stock_industry()
    # rs = bs.query_stock_basic(code_name="浦发银行")
    print('query_stock_industry error_code:'+rs.error_code)
    print('query_stock_industry respond  error_msg:'+rs.error_msg)
    
    # 打印结果集
    industry_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        industry_list.append(rs.get_row_data())
    result = pd.DataFrame(industry_list, columns=rs.fields)
    # 结果集输出到csv文件
    result.to_csv("D:/stock_industry.csv", encoding="gbk", index=False)
    print(result)
    
    # 登出系统
    bs.logout()
    
    dis=StockDataProcessor.getDateDistance('20190101', '20191201')
       
    print(dis)
       
       
    #显示所有列
    pd.set_option('display.max_columns', None)
    #显示所有行
    pd.set_option('display.max_rows',None)
    np.set_printoptions(threshold = np.inf)
    #若想不以科学计数显示:
    np.set_printoptions(suppress = True)
    tushare.set_token('221f96cece132551e42922af6004a622404ae812e41a3fe175391df8')
    sdDataAPI = tushare.pro_api()
    ic = sdDataAPI.income(ts_code='000001.SZ',start_date='19911219',end_date='20191212')
    print(ic)
    
    
    
    
    
    lastDealDayData=pandas.DataFrame([['20190101','0',3.45]])
    
    
    processor=StockDataProcessor()
    
    processor.isDealDay('20190103')
    
    tic = timer()
    
    outputFile=r"D:\outputdir\20190103-20190304-RawStrategy\BuylowSellhighStrategy\000001.SZ.csv"
    
    i=0
    while i<30:
       print(i)
       i=i+1 
    
    origStdout=sys.stdout  #保存标准输出流
    sys.stdout=open(outputFile,'at+')
    
    
    sys.stdout=origStdout  #恢复标准输出
    i=0
    while i<30:
        fileDF=FileProcessor.readFile(outputFile)
        i=i+1
        
    # 待测试的代码
    toc = timer()
    
    print(toc - tic) # 输出的时间，秒为单位
    
        
    #显示所有列
    pd.set_option('display.max_columns', None)
    #显示所有行
    pd.set_option('display.max_rows',None)
    
    np.set_printoptions(threshold = np.inf)
    #若想不以科学计数显示:
    np.set_printoptions(suppress = True)
    
    
    
    
    StockDataProcessor.getNextDealDay('20180101',False)
    tushare.set_token('221f96cece132551e42922af6004a622404ae812e41a3fe175391df8')
    sdDataAPI = tushare.pro_api()
    
    '''
    #获取半年报、月报
    
    #获取利润表
    ic = sdDataAPI.income(ts_code='000003.SZ',start_date='20190101',
                          end_date=dt.now().strftime('%Y%m%d'))
    print(ic)
    for idx in ic.index:
        if not(pd.isnull(ic.at[idx,'ebit']) or pd.isnull(ic.at[idx,'ebitda'])\
        or pd.isnull(ic.at[idx,'n_income']) or pd.isnull(ic.at[idx,'n_income_attr_p'])\
        or pd.isnull(ic.at[idx,'income_tax']) or pd.isnull(ic.at[idx,'int_income'])\
        or pd.isnull(ic.at[idx,'int_exp']) or pd.isnull(ic.at[idx,'biz_tax_surchg'])):
            print('ebit: ',ic.at[idx,'ebit'])
            print('ebitda: ',ic.at[idx,'ebitda'])
            print('n_income: ',ic.at[idx,'n_income'])
            print('n_income_attr_p: ',ic.at[idx,'n_income_attr_p'])
            print('income_tax: ',ic.at[idx,'income_tax'])
            print('int_income: ',ic.at[idx,'int_income'])
            print('int_exp: ',ic.at[idx,'int_exp'])
            print('biz_tax_surchg: ',ic.at[idx,'biz_tax_surchg'])
    '''    
    #查询语句
    sql="select * from s_income_000001 where end_date like '%0630' or end_date like '%1231' order by end_date desc;"
    aosr=MysqlProcessor.querySql(sql)
    
    '''
    #取最近一期的半年报？
    
    
    #（营业总收入-营业税金及附加）-（营业成本+利息支出+手续费及佣金支出+销售费用+管理费用+研发费用+坏账损失+存货跌价损失）+其他收益
    
    #营业总收入
    total_revenue
    #营业税金及附加
    biz_tax_surchg
    #营业成本
    oper_cost
    #利息支出
    int_exp
    #手续费及佣金支出
    comm_exp
    #销售费用
    sell_exp
    #管理费用
    admin_exp
    #研发费用
    df = pro.fina_indicator(ts_code='600000.SH')
    rd_exp
    #坏账损失
    
    #存货跌价损失
    
    #其他收益（投资收益)
    ？？？
    
    
        
    print('ebit: ',ic[ic['ebit'].notnull() and ic['ebitda'].notnull()].reset_index(drop=True).at[0,'ebit'])
    print('ebitda: ',ic[ic['ebitda'].notnull()].reset_index(drop=True).at[0,'ebitda'])
    print('n_income: ',ic[ic['n_income'].notnull() and ic['ebitda'].notnull()].reset_index(drop=True).at[0,'n_income'])
    print('n_income_attr_p: ',ic[ic['n_income_attr_p'].notnull()].reset_index(drop=True).at[0,'n_income_attr_p'])
    print('income_tax: ',ic[ic['income_tax'].notnull()].reset_index(drop=True).at[0,'income_tax'])
    print('int_income: ',ic[ic['int_income'].notnull()].reset_index(drop=True).at[0,'int_income'])
    print('int_exp: ',ic[ic['int_exp'].notnull()].reset_index(drop=True).at[0,'int_exp'])
    print('biz_tax_surchg: ',ic[ic['biz_tax_surchg'].notnull()].reset_index(drop=True).at[0,'biz_tax_surchg'])
    
    
    
    '''
    
    rs=tushare.pro_bar(ts_code="000000.SZ", start_date="20191116", end_date="20191117")
    
    print(rs.empty)
    
    sql = "select table_name from information_schema.tables where table_name='s_balancesheet_000000'"
    res=MysqlProcessor.querySql(sql)
    
    
    print(res.empty)
    
    mysqlEngine = MysqlProcessor.getMysqlEngine()
    
    #部分更新语句
    pupdatesql="update u_data_desc set content='%s' where content_name='last_update_time';"%(dt.now().strftime('%Y-%m-%d %H:%M:%S'))
    
    pupdatesqltxt = sqlalchemy.text(pupdatesql)
    
    #数据库回话
    DBSession= sessionmaker(bind=mysqlEngine)
    
    session= DBSession()
    
    #查询结果
    try:
        #df=pandas.read_sql_query(sqltxt,engine)
        session.execute(pupdatesqltxt)
    except:
        traceback.print_exc()
    #except Exception as e:
    #    print (e)
    finally:
        session.commit()
    
    
    
    
    print()
    
    
    
    
    
    
    sdf=StockDataProcessor.getAllStockDataDict()
    
    
    
    sdf.set_index('ts_code')['list_date'].to_dict()
    
    
    
    
    
    
    tushare.set_token('221f96cece132551e42922af6004a622404ae812e41a3fe175391df8')
    
    sdDataAPI = tushare.pro_api()
    
    
    #显示所有列
    pandas.set_option('display.max_columns', None)
    #显示所有行
    pandas.set_option('display.max_rows',None)
    
    
    stock_k_data = tushare.pro_bar(ts_code='000002.SZ',start_date='20100101',end_date='20191031',adj='qfq')
    stock_k_data.sort_index(inplace=True,ascending=False)
    savedStdout = sys.stdout  #保存标准输出流
    sys.stdout = open('d:/tstest.csv','wt+')
    print(stock_k_data)
    sys.stdout = savedStdout #恢复标准输出流
    
    '''
    stock_k_data = tushare.pro_bar(ts_code='000001.SZ',start_date='20190623',end_date='20191031',adj='None')
    
    stock_k_data.sort_index(inplace=True,ascending=False)
    print(stock_k_data)
    '''
    
    stock_k_data=StockDataProcessor.getStockKData('000001.SZ', '20100101', '20191031', 'qfq')
    savedStdout = sys.stdout  #保存标准输出流
    sys.stdout = open('d:/mysqltest.csv','wt+')
    print(stock_k_data)
    sys.stdout = savedStdout #恢复标准输出流
    print()
    '''
    engine = MysqlProcessor.getMysqlEngine()
    sql_kdata = 'select * from s_kdata_000001 where trade_date>=20190101 and trade_date<=20191031 order by trade_date'
    sqltxt_kdata = sqlalchemy.text(sql_kdata)
    
    sql_adj = 'select * from s_adjdata_000001 where trade_date>=20190101 and trade_date<=20191031 order by trade_date'
    sqltxt_adj = sqlalchemy.text(sql_adj)   
    
    kdatadf=pandas.read_sql_query(sqltxt_kdata,engine)
    adjdf=pandas.read_sql_query(sqltxt_adj,engine)
    
    kdatadf_hfq=copy.deepcopy(kdatadf)
    kdatadf_hfq['open']=kdatadf_hfq['open']*adjdf['adj_factor']
    print()
    
    
    latest_adj_factor=float(adjdf.at[adjdf.shape[0]-1,'adj_factor'])
    kdatadf_hfq['open']=kdatadf_hfq['open']/latest_adj_factor
    
    '''
    
    
    
    
    #print()
    
    
    df1=DataFrame(numpy.arange(15).reshape(3,5),columns=['a','b','c','d','e'],index=['one','two','three'])
    
    #print(df1)
    
    
    df2=DataFrame(numpy.arange(12).reshape(4,3),columns=['1','2','3'],index=['one','three','four','five'])
    
    #print(df2)
    
    df1['a']=df1['a']*df2['1']
    
    #print(df1)
    #stock_k_data=MysqlProcessor.getStockKData('000001.SZ', '20190624', '20191031','None')
    
    #print(stock_k_data.columns)
    
    #print(stock_k_data)
    
    #adj_data = sdDataAPI.adj_factor(ts_code='000001.SZ',start_date='20190624',end_date='20191031')
    
    #adj_data.sort_index(inplace=True,ascending=False)
    #stock_k_data.reset_index(drop=True,inplace=True)
    
    #print(adj_data)
    
    
    
    
    
    
    
    def getlastquarterfirstday():
        today=dt.now()
        quarter = (today.month-1)/3+1
        if quarter == 1:
            return dt(today.year-1,10,1)
        elif quarter == 2:
            return dt(today.year,1,1)
        elif quarter == 3:
            return dt(today.year,4,1)
        else:
            return dt(today.year,7,1)
    
    startday=getlastquarterfirstday().strftime('%Y%m%d')
    
    
    sdf = sdDataAPI.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    
    
    #stockTuples={}
    
    cfRatioDict={}
    
    cnt=0
    for idx in sdf.index:
    #    stockTuples[sdf.at[idx,'ts_code']]=sdf.at[idx,'name']
    
        if cnt>120:
            break
        else:
            cnt=cnt+1
        #获取资产负债表，总资产
        bs = sdDataAPI.balancesheet(ts_code=sdf.at[idx,'ts_code'],start_date=startday,end_date=dt.now().strftime('%Y%m%d'))
        print(bs.columns)
        bs.at[0,'total_assets']
        time.sleep(0.75)
        
        #获取现金流量表中，现金等价物总数
        cf = sdDataAPI.cashflow(ts_code=sdf.at[idx,'ts_code'],start_date=startday,end_date=dt.now().strftime('%Y%m%d'))#, period='20190930')
        cf.at[0,'c_cash_equ_end_period']
    
        ratio=cf.at[0,'c_cash_equ_end_period']/bs.at[0,'total_assets']
        
        cfRatioDict[sdf.at[idx,'ts_code']]=ratio
    
    print()
    
    sortedCFRatioList=sorted(cfRatioDict.items(),key=lambda x:x[1],reverse=True)
    
    
    for cd, ratio in sortedCFRatioList:
        print(cd,' : ',round(ratio*100,2),'%')
    
    
    topCFRatio100 = sortedCFRatioList[:100]
    
    #gpr:毛利率
    #npr:净利率
    
    
    df= tushare.get_stock_basics()
    
    #df=df.set_index('name')
    
    
    idx=df.name.index
    val=df.name.values
    
    print(idx,val)
    
    for cd in idx:
        
        print(cd, df.at[cd,'name'])
    '''
        if 'ST' in df.at[cd,'name']:
            print('ST,%s,%s'%(cd,df.at[cd,'name']))
            df.drop(cd,inplace=True)
        elif '退' in df.at[cd,'name']:
            print('退,%s,%s'%(cd,df.at[cd,'name']))
            df.drop(cd,inplace=True)
        elif cd=='002680':
            print()
    '''        
    
    print()
    
    
    '''
    i=0
    
    for nm in stNames:
        if 'ST' in nm:
            print('ST')
    
        i=i+1
    '''
    
    '''
    df=df[~df.name.str.contains('ST')]
    
    print(df)
    '''
    
    #获取不复权的数据
    stock_k_data = tushare.pro_bar(ts_code='000001.SZ',start_date='20000101')
    
    stock_k_data.to_csv('D:/outputDir/000001.csv',mode='wt')
    df = sdDataAPI.adj_factor(ts_code='000001.SZ',trade_date='')
    
    df.to_csv('D:/outputDir/000001-adj.csv',mode='wt')
    #获取不复权的数据
    
    stock_k_data = tushare.pro_bar(ts_code='000001.SZ',adj='qfq',start_date='20000101')
    
    stock_k_data.to_csv('D:/outputDir/000001-qfq.csv',mode='wt')
    
    #1、获取交易日信息，并存入数据库
    STARTDATE = '20050101'#20071016
    ENDDATE = '20050624'#20081031
    
    
    #不能这样处理，不同区间取到的前复权数据不同，会影像处理的准确性
    stock_k_data = tushare.pro_bar(ts_code='000001.SZ', adj='qfq', start_date='20000101')
    
    #sdDataAPI.query('trade_cal', start_date='20180101', end_date='20181231')
    
    trade_cal_data = sdDataAPI.trade_cal(exchange='', start_date=STARTDATE, end_date=ENDDATE)
    
    
    
    # 函数
    def xnpv(rate, cashflows):
        return sum([cf/(1+rate)**((t-cashflows[0][0]).days/365.0) for (t,cf) in cashflows])
     
    def xirr(cashflows, guess=0.1):
        try:
            return optimize.newton(lambda r: xnpv(r,cashflows),guess)
        except:
            print('Calc Wrong')
     
     
    # 测试
    data = [(datetime.date(2006, 1, 1), -10000), (datetime.date(2007, 1, 1), 20000)]
    print(xirr(data))
    
    
    
    
    
    
    
    
    
    
    #记录csv内容的列表
    fileContentTupleList = []
    
    strOutterOutputDir=OUTPUTDIR+'/'+STARTDATE+'-'+ENDDATE+'/SMAStrategy/'
    
    #读取文件列表
    fileList = os.listdir(strOutterOutputDir)
    
    #对文件列表中的文件进行处理，获取内容列表
    for fileStr in fileList:
        if not fileStr=="Summary.csv":
            df = FileProcessor.readFile(strOutterOutputDir+fileStr)
            fileContentTupleList.append((fileStr[:-4],df))
    
    
    
    #对各个日期计算相应的资金净流量
    cashFlowTuple= {}
    #对已有的内容列表进行处理
    for fileName,fileDF in fileContentTupleList:
        print(fileName)
    
        #如果Summary-all.csv已经存在，则直接覆盖
      
        i=0
        while True:
            if not (fileDF.at[i,'日期'] in cashFlowTuple):
                cashFlowTuple[fileDF.at[i,'日期']] = float(fileDF.at[i,'当天资金净流量'])
            else:
                cashFlowTuple[fileDF.at[i,'日期']] = float(cashFlowTuple[fileDF.at[i,'日期']])+float(fileDF.at[i,'当天资金净流量'])
            i=i+1
            if i==len(fileDF):
                break
        
    
    savedStdout = sys.stdout  #保存标准输出流
    sys.stdout = open(OUTPUTDIR+'/'+STARTDATE+'-'+ENDDATE+'/Summary-all.csv','wt+')
    
    print('日期,当日资金净流量')
    for key in cashFlowTuple.keys():
        print(key,end=',')
        print(cashFlowTuple.get(key))
    
    sys.stdout = savedStdout  #恢复标准输出流
    
    
    
    
    
    
    
    
    tushare.set_token('221f96cece132551e42922af6004a622404ae812e41a3fe175391df8')
    
    sdDataAPI = tushare.pro_api()
    
    #1、获取交易日信息，并存入数据库
    STARTDATE = '20050101'#20071016
    ENDDATE = '20050624'#20081031
    
    trade_cal_data = sdDataAPI.trade_cal(exchange='', start_date=STARTDATE, end_date=ENDDATE)
    
    #不能这样处理，不同区间取到的前复权数据不同，会影像处理的准确性
    stock_k_data = tushare.pro_bar(ts_code='000001.SZ', adj='qfq', start_date=STARTDATE, end_date=ENDDATE)
    
    stock_k_data=stock_k_data.set_index('trade_date')
    
    stock_k_data[(stock_k_data.index=='20000622')]
    
    if (stock_k_data[(stock_k_data.index=='20000622')]).empty:
        print("empty")
    
    #写入数据库的引擎
    engine = sqlalchemy.create_engine('mysql+pymysql://root:xiaoda001@localhost/tsdata?charset=utf8mb4')
    
    
    k_data = DataFrame()
    
    k_data.sort_index(inplace=True,ascending=False)
    
    k_data.to_sql(name='k_data_test', con=engine, chunksize=1000, if_exists='replace', index=None)
    
    
    
    tushare.set_token('221f96cece132551e42922af6004a622404ae812e41a3fe175391df8')
    
    sdDataAPI = tushare.pro_api()
    
    sdf = sdDataAPI.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    
    stockCodeList = sdf['ts_code']
    
    
    for index,stockCode in stockCodeList.items():
    
        print(index, stockCode)
    
    #取000001的前复权行情
    stock_k_data = tushare.pro_bar(ts_code='000001.SZ', adj='qfq', start_date='20091031', end_date='20191031')
    
    
    if type(stock_k_data)==NoneType:
        print("empty")
        
    stock_k_data.sort_index(inplace=True,ascending=False)
    
    stock_k_data.reset_index(drop=True,inplace=True)
    
    
    stock_k_data['MA20'] = stock_k_data['close'].rolling(20).mean()
    
    offset = stock_k_data.index[0]
    #剔除掉向前找的20个交易日数据
    if stock_k_data.shape[0] > 20:
        stock_k_data = stock_k_data.drop([offset,offset+1])
    
    
    
    
    data = sdDataAPI.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    
    
    stockCodeList = data['ts_code']
    #000948在2019.4.12等日期的复权数据有问题
    
    stock_k_data = tushare.get_k_data('000948',start='2019-04-02',end='2019-04-15')
    
    stock_k_data2 = stock_k_data.set_index('date',inplace=False)
    
    
    
    #股票代码库
    INPUTFILE = 'D:/stockList.txt'
    STARTDATE = '1990-12-19'#2007-10-16
    ENDDATE = '2019-10-31'#2008-10-31
    
    #写入数据库的引擎
    engine = sqlalchemy.create_engine('mysql+pymysql://root:xiaoda001@localhost/tsdata?charset=utf8')
    
    #对所有股票代码，循环进行处理
    in_text = open(INPUTFILE, 'r')
    
    #循环所有股票，获取数据并写入数据库
    for line in in_text.readlines():
        stockCode = line.rstrip("\n")
        df = tushare.get_k_data(code=stockCode,start=STARTDATE, end=ENDDATE)
        #存入数据库
        df.to_sql(name='k_data_'+stockCode, con=engine, chunksize=1000, if_exists='replace', index=None)
    
    
    
    
    
    
    print(os.getcwd())
    path = sys.path[0]
    print(os.listdir())
    
    
    #需要找到开始日期前面的20个交易日那天，从那一天开始获取数据
    firstOpenDay='2014-05-19'
    '''
    cday = dt.strptime(firstOpenDay, "%Y-%m-%d").date()
    dayOffset = datetime.timedelta(1)
    cnt=0
    # 获取想要的日期的时间
    while True:
        cday = (cday - dayOffset)
        if not(tushare.is_holiday(cday.strftime('%Y-%m-%d'))):
            cnt+=1
            if cnt==20:
                break
    firstOpenDay=cday.strftime('%Y-%m-%d')
    '''
    stock_k_data = tushare.get_k_data('000001',start=firstOpenDay,end='2015-06-12')
    
    stock_k_data2 = stock_k_data.set_index('date',inplace=False)
    
    print(stock_k_data2.at['2014-05-19','open'])
    
    stock_k_data2.reset_index()
    
    
    stock_k_data['SMA_20'] = stock_k_data['close'].rolling(20).mean()
    
    offset = stock_k_data.index[0]
    stock_k_data = stock_k_data.drop([offset,offset+1,offset+2,offset+3,offset+4,offset+5,offset+6,offset+7, \
                                      offset+8,offset+9,offset+10,offset+11,offset+12,offset+13,offset+14, \
                                      offset+15,offset+16,offset+17,offset+18,offset+19])
    
    print()
    
    
    
    
    
    
    
    date_string = "2018-01-01"
    
    cday = dt.strptime(date_string, "%Y-%m-%d").date()
    
    offset = datetime.timedelta(1)
    
    # 获取想要的日期的时间
    re_date = (cday + offset).strftime('%Y-%m-%d')
    
    
    
    print(re_date)
    
    
    #d:/stockList.txt
    #股票代码库
    INPUTFILE = 'D:/stockList.txt'
    OUTPUTDIR = 'D:/outputDir/'
    
    def printHead():
        print('日期,交易类型,当天持仓账面总金额,当天持仓总手数,累计投入,累计赎回,当前持仓平均成本,当天平均价格,当前持仓盈亏,最近一次交易类型,最近一次交易价格,当前全部投入回报率')
        
    def printTradeInfo(date, dealType, avgPriceToday,holdShares,holdAvgPrice,totalInput,totalOutput,latestDealType,latestDealPrice):
        print(date, end=',')
        print(dealType, end=',')
        print(round(avgPriceToday*holdShares * 100,4), end=',')
        print(holdShares, end=',')
        print(round(totalInput*100,4), end=',')
        print(round(totalOutput*100,4), end=',')
        print(round(holdAvgPrice,4), end=',')
        print(round(avgPriceToday,4), end=',')
        currentProfit = round(avgPriceToday*holdShares*100+totalOutput*100-totalInput*100,4)
        print(currentProfit, end=',')
        print(latestDealType, end=',')
        print(round(latestDealPrice,4), end=',')
        totalProfitRate = currentProfit / (totalInput*100) * 100
        print(round(totalProfitRate,2), end='%,')
        print()
    
    
    
    
    
    
    
    
    stock_his = tushare.get_k_data('601366',start='2018-01-01',end='')
    stock_his.shape[0]
    stock_hist_data = tushare.get_hist_data(code='601366', start='2018-01-01')
    
    stock_hist_data = stock_hist_data.sort_index()
    stock_hist_data.shape[0]
    todayMA20 = stock_hist_data.at['2018-01-03','ma20']
    
    
    stock_his.set_index('date',inplace=True)
    
    
    stock_his['SMA_20'] = stock_his['close'].rolling(20).mean()
    
    
    stock_his['close_shift'] = stock_his['close'].shift(1)
    
    #stock_his.plot(subplots=True,figsize=(10,6))
    
    #stock_his[['close','SMA_20']].plot(figsize=(10,6))
    
    print()
    
    s = pandas.Series([20, 21, 12], index=['London', 'New York', 'Helsinki'])
    
    
    def add_custom_values(x, **kwargs):
        for month in kwargs:
            x += kwargs[month]
        return x
    s.apply(add_custom_values, june=30, july=20, august=25)
    
    #s.apply(numpy.log)
    
    
    
    '''
    print(type(stock_his))
    
    df = stock_his.head()
    
    print(df)
    print(df.dtypes)
    
    '''
    
    
    #print(stock_his.index)
    #type(stock_his.index)
    
    #第一行的偏移量
    #因为如果不是从当年第一个交易日开始，标号会有一个偏移量，在后续处理时，需要进行一个处理
    offset = stock_his.index[0]
    
    #stock_his.shape[0]
    
    #    print(stock_his)
    
    '''
    #print(stock_his.get_value(1, 'open', ))
    
    print(stock_his.at[0,'open'])
    
    print(stock_his.open)
    
    '''
    
    '''
    print(stock_his.iat[0,0])#查看具体位置数据
    
    print(stock_his.iloc[1])#查看某一行数据
    
    print(stock_his['open'])#查看某一列
    
    print(stock_his.shape[0])#查看行数
    
    print(stock_his.shape[1])#查看列数
    '''
    
    
    nShare = 10 #每次交易单位，默认为10手
    #{stock_his.at[0,'date']: 4098}
    
    holdShares = 0#持仓手数，1手为100股
    holdAvgPrice = 0#持仓的平均价格
    
    latestDealType = 0#最近一笔成交类型，-1表示卖出，1表示买入
    latestDealPrice = 0#最近一笔成交的价格
    
    
    
    totalInput = 0#累计总投入
    totalOutput = 0#累计总赎回
    
    
    biggestCashOccupy = 0#最大占用总金额，为totalInput-totalOutput的最大值
    
    i = 0
    
    import sys
    savedStdout = sys.stdout  #保存标准输出流
    outputFile = OUTPUTDIR + '000029' + '.csv'
    print(outputFile)
    sys.stdout = open(outputFile,'wt')
    
    printHead()
    
    while i<stock_his.shape[0]:
    #    print(stock_his.iloc[i])
    
        avgPriceToday = (stock_his.at[i+offset,'open'] + stock_his.at[i+offset,'close'])/2
        todayDate = stock_his.at[i+offset,'date']
        if i==0:
            #第一个交易日，以当日均价买入n手
            holdShares = nShare
            holdAvgPrice = avgPriceToday
            #最近一笔交易类型为买入，交易价格为当日均价
            latestDealType = 1
            latestDealPrice = avgPriceToday
            totalInput += holdShares * holdAvgPrice
    #        print(todayDate)
    #        print('完成买入交易，以%f价格买入%i手股票'%(avgPriceToday,nShare))
            printTradeInfo(todayDate, 1, avgPriceToday,holdShares,holdAvgPrice,totalInput,totalOutput,latestDealType,latestDealPrice)
            
            if 100*(totalInput - totalOutput) > biggestCashOccupy:
                biggestCashOccupy = 100*(totalInput - totalOutput)
        else:
            #不是第一个交易日
            #需要根据当前价格确定如何操作
            if avgPriceToday <= 0.85*latestDealPrice:
                #价格小于上次交易价格0.85，买入nShare/2手下取整
                buyShare = math.floor(nShare/2)
                holdAvgPrice = (holdShares*holdAvgPrice + buyShare*avgPriceToday)/(holdShares+buyShare)
                holdShares += buyShare
                
                latestDealType = 1
                latestDealPrice = avgPriceToday
                totalInput += buyShare*avgPriceToday
    #            print(todayDate)
    #            print('完成买入交易，以%f价格买入%i手股票'%(round(avgPriceToday,2),buyShare))
                printTradeInfo(todayDate, 1, avgPriceToday,holdShares,holdAvgPrice,totalInput,totalOutput,latestDealType,latestDealPrice)
                
            
                if 100*(totalInput - totalOutput) > biggestCashOccupy:
                    biggestCashOccupy = 100*(totalInput - totalOutput)
                
            elif avgPriceToday >= 1.1*holdAvgPrice and avgPriceToday >= latestDealPrice*1.1 and holdShares > 0 :
                #价格大于上次交易价格1.1倍，且大于平均持仓成本的1.1倍，卖出持仓数/2上取整手
                sellShare = math.ceil(holdShares/2)
    
                if holdShares > sellShare:
                    #尚未全部卖出，只是部分卖出
                    holdAvgPrice = (holdShares*holdAvgPrice - sellShare*avgPriceToday)/(holdShares-sellShare)
                    holdShares -= sellShare
                else:
                    #如果全部卖出，则把平均持仓价格设置为0
                    holdAvgPrice = 0
                    holdShares = 0
                    
                latestDealType = -1
                latestDealPrice = avgPriceToday
                totalOutput += sellShare*avgPriceToday
                
            
                if 100*(totalInput - totalOutput) > biggestCashOccupy:
                    biggestCashOccupy = 100*(totalInput - totalOutput)
                
    #            print(todayDate)
    #            print('完成卖出交易，以%f价格卖出%i手股票'%(round(avgPriceToday,2),sellShare))
                printTradeInfo(todayDate, -1, avgPriceToday,holdShares,holdAvgPrice,totalInput,totalOutput,latestDealType,latestDealPrice)
            else:
                #没有任何交易，打印对账信息:
                printTradeInfo(todayDate, 0, avgPriceToday,holdShares,holdAvgPrice,totalInput,totalOutput,latestDealType,latestDealPrice)
                
        i+=1
    
    '''
        #输出当前最新一天的盈亏情况
        if i==stock_his.shape[0]:
    #        print('最新盈亏情况: ')
    #        print(todayDate)
            printTradeInfo(todayDate, 0, avgPriceToday,holdShares,totalInput,totalOutput,latestDealType,latestDealPrice)
    '''
    
    
    print('最大资金占用金额: ', round(biggestCashOccupy,2))
    
    sys.stdout = savedStdout  #恢复标准输出流
    
    
    '''
    规则：
    1、以第一天中间价买入n手，如果后续某天中间价大于持仓成本的1.1倍则抛出一半
    2、价格跌破最近一次交易价格的0.85时，再次买入n/2下取整手
    3.如果持仓不为0，价格涨破平均持仓平均成本的1.1，且价格涨破上笔交易的1.1倍时，卖出持仓数/2上取整
    
    '''



