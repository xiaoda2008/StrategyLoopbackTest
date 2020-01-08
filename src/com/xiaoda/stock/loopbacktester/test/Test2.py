'''
Created on 2019年12月24日

@author: xiaoda
'''
from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlProcessor
from com.xiaoda.stock.loopbacktester.utils.StockDataUtils import StockDataProcessor
from com.xiaoda.stock.loopbacktester.strategy.trade.BuylowSellhighStrategy import BuylowSellhighStrategy
from timeit import default_timer as timer
import time
import matplotlib.pyplot as plt
from datetime import datetime as dt
from com.xiaoda.stock.loopbacktester.utils.TradeStrategyUtil import TradeStrategyProcessor
import tushare
import pandas

if __name__ == '__main__':

    


            
    pltDF=pandas.DataFrame(data=[],columns=['Date','Profit','HS300'])

    tmpDF=pandas.DataFrame({'Date':'a','Profit':'b','HS300':'c'},index=[1])


    pltDF=pltDF.append(tmpDF,ignore_index=True,sort=False)
    
    
    sdProcessor=StockDataProcessor()
    
    df=sdProcessor.getidxData('HS300','20191101','20191101')

    tushare.set_token('221f96cece132551e42922af6004a622404ae812e41a3fe175391df8')
    sdDataAPI=tushare.pro_api()

    df=sdDataAPI.index_daily(ts_code='000300.SH', start_date='20000101', end_date='20001010')

    print()


    df=sdDataAPI.index_dailybasic(ts_code='000300.SH',start_date='20190101',end_date='20191231')

    print()
    
    
    
    
    
    
    
    

    sdProcessor=StockDataProcessor()
    mysqlProcessor=MysqlProcessor()
    mysqlSession=mysqlProcessor.getMysqlSession()
        
    sql="select content from u_data_desc where content_name='data_end_dealday'"
    df=mysqlProcessor.querySql(sql)    
    
    lstDealDayInDB=df.at[0,'content']
    
    sd=sdProcessor.getCalDayByOffset(lstDealDayInDB, -365*5)
    ed=lstDealDayInDB  
    
    hs300Dict=sdProcessor.getHS300Dict()      
    
    #cfRatioDict={}

    #可以考虑多进程？
    for (stockCode,scdict) in hs300Dict.items():    
    
        sql='alter table s_dailybasic_%s drop column avg_PE_Lst_5Years;'%(stockCode[:6]) 
        MysqlProcessor.execSql(mysqlSession,sql,True)

        sql='alter table s_dailybasic_%s drop column mAvg_PE_Lst_5Years;'%(stockCode[:6]) 
        MysqlProcessor.execSql(mysqlSession,sql,True)

        sql='alter table s_dailybasic_%s drop column Percentage_PE_Lst_5Years;'%(stockCode[:6]) 
        MysqlProcessor.execSql(mysqlSession,sql,True)
           
    
    print()
    
    
    
    
    
    
    
    
    
    
    
    
    sd=sdProcessor.getCalDayByOffset(lstDealDayInDB, -365*5)
    
    sql="SELECT\
    db.trade_date trade_date,\
    db.pe pe,\
    db.pb pb,\
    db.ps ps,\
    kd.vol vol\
    FROM\
    s_dailybasic_000063 db,\
    s_kdata_000063 kd \
    WHERE\
    db.trade_date = kd.trade_date \
    AND db.trade_date > '%s' \
    ORDER BY\
    trade_date ASC;"%(sd)
    
    df=mysqlProcessor.querySql(sql)

    df.set_index('trade_date',drop=True,inplace=True)

    try:
        lastPE=df.at[lstDealDayInDB,'pe']
    except:
        print("股票在最后一个交易日停牌")
    
    df.reset_index(inplace=True)
    
    avgPE=0
    sumPE=0
    biggerCnt=0
    
    
    sumVol=0
    wSumPE=0 
    wAvgPE=0
    
    idx=0
    while idx<len(df):
        sumPE+=df.at[idx,'pe']
        if df.at[idx,'pe']>=lastPE:
            biggerCnt+=1
         
        
        sumVol+=df.at[idx,'vol']
        wSumPE+=df.at[idx,'vol']*df.at[idx,'pe']
        
        idx+=1

    avgPE=sumPE/len(df)
    wAvgPE=wSumPE/sumVol
    
    print()
    print("股票%s当前PE值为：%.2f过去5年，有%.2f%%的交易日PE值低于当前"%("000063",lastPE,(1-biggerCnt/len(df))*100),end=',')
    print("过去5年平均PE:%.2f，按交易量加权平均PE:%.2f"%(avgPE,wAvgPE))
    
    
    df.drop(columns=['vol'],inplace=True)
    
    df.set_index('trade_date',drop=True,inplace=True)
    

    df.plot()
    plt.grid(True)    
    plt.savefig('d:\\foo.png')    
    

    #plt.show()

    print()
   
    
    
    
    '''
    processor=MysqlProcessor()
    session=processor.getMysqlSession()
    
    processor.querySql("select * from u_stock_list")
    
    print()
    
    
    sdProcessor=StockDataProcessor()
    
    print()
    
    '''
    tradeStrategyProcessor=TradeStrategyProcessor()
    
    
    
    t1=time.time()
    strategy=tradeStrategyProcessor.getStrategy("BuylowSellhighStrategy")
    
    t2=time.time()
    stockOutDF=strategy.processTradeDuringPeriod('000001.SZ','20190101','20190105')
    
    t3=time.time()
    stockOutDF.to_csv('d:/test.csv',index = False,encoding='ANSI')
    t4=time.time()
    
    print("t4-t1:%s"%(t4-t1)) # 输出的时间，秒为单位
    print("t4-t2:%s"%(t4-t2)) # 输出的时间，秒为单位    
    print("t4-t3:%s"%(t4-t3)) # 输出的时间，秒为单位   