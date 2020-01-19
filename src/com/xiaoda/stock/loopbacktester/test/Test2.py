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
from mpl_toolkits.axes_grid1 import host_subplot
import mpl_toolkits.axisartist as AA
import matplotlib.pyplot as plt

import pandas as pd
import pandas_datareader.data as web
##利用最小二乘法进行线性回归，拟合CAPM模型
import statsmodels.api as sm


if __name__ == '__main__':

    sdProcessor=StockDataProcessor()

    startday='20190101'
    endday='20200101'
    
    '''
    #黄金价格
    goldDF=web.DataReader(name='GOLDAMGBD228NLBM', data_source='fred',start=startday,end=endday)

    goldDF=goldDF[goldDF['GOLDAMGBD228NLBM'].notnull()]

    goldDF=goldDF.reset_index()
    goldDF['DATE']=goldDF['DATE'].dt.strftime('%Y%m%d') 

    goldDF=goldDF.rename(columns={'DATE': 'trade_date', 'GOLDAMGBD228NLBM': 'gold_price'})
        
    goldDF.set_index('trade_date',drop=True,inplace=True)

    goldDF['pct_chg']=(goldDF['gold_price']-goldDF['gold_price'].shift(1))/goldDF['gold_price'].shift(1)

    goldDF=goldDF[goldDF['pct_chg'].notnull()]
        
    idxDF=sdProcessor.getidxData('HS300',startday,endday)    

    goldDF['pct_chg']=goldDF['pct_chg']*100

    sh_md_merge=pd.merge(pd.DataFrame(idxDF.pct_chg),pd.DataFrame(goldDF.pct_chg),\
                         left_index=True,right_index=True,how='inner')
    

    '''
    
    
    #德国股指
    #goldDF=web.DataReader(name='^GDAXI', data_source='yahoo',start=startday,end=endday)
    #苹果股价
    #cmpDF=web.DataReader(name='AAPL', data_source='yahoo',start=startday,end=endday)


    '''
    #道琼斯指数
    cmpDF=web.DataReader(name='DJIA', data_source='yahoo',start=startday,end=endday)    

    cmpDF=cmpDF[cmpDF['Adj Close'].notnull()]

    cmpDF=cmpDF.reset_index()
    cmpDF['Date']=cmpDF['Date'].dt.strftime('%Y%m%d')

    cmpDF=cmpDF.rename(columns={'Date': 'trade_date', 'Adj Close': 'close_price'})
        
    cmpDF.set_index('trade_date',drop=True,inplace=True)

    cmpDF['pct_chg']=(cmpDF['close_price']-cmpDF['close_price'].shift(1))/cmpDF['close_price'].shift(1)*100

    cmpDF=cmpDF[cmpDF['pct_chg'].notnull()]
        
    idxDF=sdProcessor.getidxData('HS300',startday,endday)

    sh_md_merge=pd.merge(pd.DataFrame(idxDF.pct_chg),pd.DataFrame(cmpDF.pct_chg),\
                         left_index=True,right_index=True,how='inner')

    '''

    tushare.set_token('221f96cece132551e42922af6004a622404ae812e41a3fe175391df8')
    sdDataAPI=tushare.pro_api()
    
    #cmpDF=sdDataAPI.fund_nav(ts_code='000218.OF')
    cmpDF=sdDataAPI.fund_nav(ts_code='519772.OF')
    cmpDF=cmpDF.reindex(index=cmpDF.index[::-1])

   
    #print(cmpDF.columns)
    #cmpDF=cmpDF.reset_index()    
    
    cmpDF=cmpDF.rename(columns={'end_date': 'trade_date'})
    
    cmpDF.set_index('trade_date',drop=True,inplace=True)

    
    cmpDF['pct_chg']=(cmpDF['accum_nav']-cmpDF['accum_nav'].shift(1))/cmpDF['accum_nav'].shift(1)*100

    cmpDF=cmpDF[cmpDF['pct_chg'].notnull()]
    idxDF=sdProcessor.getidxData('HS300',startday,endday)

    sh_md_merge=pd.merge(pd.DataFrame(idxDF.pct_chg),pd.DataFrame(cmpDF.pct_chg),\
                         left_index=True,right_index=True,how='inner')


    
    #计算日无风险利率
    Rf_annual=0.0334#以一年期的国债利率为无风险利率
    Rf_daily=(1+Rf_annual)**(1/365)-1##年利率转化为日利率
     
    #计算风险溢价:Ri-Rf
    risk_premium=sh_md_merge-Rf_daily
    #risk_premium.head()
    
    #画出两个风险溢价的散点图，查看相关性
    plt.scatter(risk_premium.values[:,0],risk_premium.values[:,1])
    plt.ylabel("Gold Daily Return")
    plt.xlabel("HS300 Index Daily Return")   
    
    md_capm=sm.OLS(risk_premium.pct_chg_y[1:],sm.add_constant(risk_premium.pct_chg_x[1:]))
    result=md_capm.fit()
    print(result.summary())
    print(result.params)
 
 
    exit(0)
   
    
    host = host_subplot(111, axes_class=AA.Axes)
    plt.subplots_adjust(right=0.75)
    
    par1 = host.twinx()
    par2 = host.twinx()
    
    offset = 100
    new_fixed_axis = par2.get_grid_helper().new_fixed_axis
    par2.axis["right"] = new_fixed_axis(loc="right",
                                        axes=par2,
                                        offset=(offset, 0))
    
    par1.axis["right"].toggle(all=True)
    par2.axis["right"].toggle(all=True)
    
    host.set_xlim(0, 2)
    host.set_ylim(0, 2)
    
    host.set_xlabel("Distance")
    host.set_ylabel("Density")
    par1.set_ylabel("Temperature")
    par2.set_ylabel("Velocity")
    
    p1, = host.plot([0, 1, 2], [0, 1, 2], label="Density")
    p2, = par1.plot([0, 1, 2], [0, 3, 2], label="Temperature")
    p3, = par2.plot([0, 1, 2], [50, 30, 15], label="Velocity")
    
    par1.set_ylim(0, 4)
    par2.set_ylim(1, 65)
    
    host.legend()
    
    host.axis["left"].label.set_color(p1.get_color())
    par1.axis["right"].label.set_color(p2.get_color())
    par2.axis["right"].label.set_color(p3.get_color())
    
    
    plt.savefig("d:/test.png")
    #plt.draw()
    #plt.show()

   


            
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