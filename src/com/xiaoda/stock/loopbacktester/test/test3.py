'''
Created on 2020年1月19日

@author: xiaoda
'''
import tushare
import sqlalchemy
from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlProcessor
import pandas
from datetime import datetime as dt
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from com.xiaoda.stock.loopbacktester.utils.StockDataUtils import StockDataProcessor

if __name__ == '__main__':
    
    pandas.set_option('display.max_rows',100)      #设置最大行数
    pandas.set_option('display.max_columns', 100)  #设置最大列数
    
    #使用TuShare pro版本    
    tushare.set_token('221f96cece132551e42922af6004a622404ae812e41a3fe175391df8')
    sdDataAPI=tushare.pro_api()


    df = sdDataAPI.fund_basic(market='O')

    print()
   
    df = sdDataAPI.fund_nav(ts_code='165509.SZ')
    
    print()
    
    
    #筛选过去三年，连续超越大盘（HS300，上证指数、创业板指数等）涨幅的基金
    #筛选过去三年，连续涨幅排名前n位的基金
    #筛选过去三年，连续收益在XX%以上的基金
    
    
    
    
    
    
    stockCode='601318.SH'
    startDay='20090101'
    endDay='20201031'
    
    shn=sdDataAPI.stk_holdernumber(ts_code=stockCode, start_date=startDay, end_date=endDay)
    
    
    shn=shn.sort_values(by='end_date')
    
    """
    mysqlProcessor=MysqlProcessor()
    
    #写入数据库的引擎
    mysqlEngine=mysqlProcessor.getMysqlEngine()
    mysqlSession=mysqlProcessor.getMysqlSession()
    
    DAYONE='19900101'

    indexDF=sdDataAPI.index_daily(ts_code='399006.SZ',start_date=DAYONE,end_date='20200606')
    """
    
    pltDF=pandas.DataFrame(data=[],columns=['Date','ShareHolderNum','StockPrice'])
    
    pandas.set_option('display.max_rows',None)
    pandas.set_option('display.max_columns',None)
    # 设置数据的显示长度，默认为50
    pandas.set_option('max_colwidth',200)
    
    sdProcessor=StockDataProcessor()
    
    stock_k_data=sdProcessor.getStockKData(stockCode,sdProcessor.getCalDayByOffset(startDay,-365),endDay,'qfq')    
    stock_k_data.set_index('trade_date',drop=True, inplace=True)
    
    for i in range(0, len(shn)):
        
        #print(shn.iloc[i]['end_date'], shn.iloc[i]['holder_num'])
        currday=shn.iloc[i]['end_date']
        
        #取最近交易日的价格
        #currday=sdProcessor.getNextDealDay(currday, True)
        
        #有可能在这一天，股票是停牌状态
        #理论上，应该在这一天往后找下一天股票实际开盘的日期
        #closePrice=stock_k_data.at[currday,'close']
        stock_k_data=stock_k_data[stock_k_data.index>currday]
        closePrice=stock_k_data.iat[0,4]
        
        tmpDF=pandas.DataFrame({'Date':shn.iloc[i]['end_date'],\
                                    'ShareHolderNum':shn.iloc[i]['holder_num'],\
                                    'StockPrice':closePrice},index=[1])

        pltDF=pltDF.append(tmpDF,ignore_index=True,sort=False)


    # 画图
    fig, ax1 = plt.subplots(figsize = (10, 5), facecolor='white')
    
    # 左轴
    #ax1.bar([dt.strptime(d,'%Y%m%d').date() for d in pltDF['Date'].to_list()],\
    #         pltDF['ShareHolderNum'].to_list(), color='g', alpha=1)
    ax1.plot([dt.strptime(d,'%Y%m%d').date() for d in pltDF['Date'].to_list()],\
             pltDF['ShareHolderNum'].to_list(), color='blue',label="ShareHolderNum")
    
    ax1.set_xlabel('ShareHolderNum')
    ax1.set_ylabel('Num')
    
    # 右轴
    ax2 = ax1.twinx()
    ax2.plot([dt.strptime(d,'%Y%m%d').date() for d in pltDF['Date'].to_list()],\
               pltDF['StockPrice'].to_list(), '-or',label="StockPrice")
    ax2.set_ylabel('Yuan')
    #ax2.set_ylim(0, 5)
    
    """
    plt.plot([dt.strptime(d,'%Y%m%d').date() for d in pltDF['Date'].to_list()],\
                         pltDF['ShareHolderNum'].to_list(),label='ShareHolderNum',c='blue')
    plt.plot([dt.strptime(d,'%Y%m%d').date() for d in pltDF['Date'].to_list()],\
                         pltDF['StockPrice'].to_list(),label='StockPrice',c='red')    
    """
                
    plt.title('Share Hold Number:%s'%(stockCode),fontsize=10)
    #设置图表标题和标题字号
    
    """
    plt.tick_params(axis='both',which='major',labelsize=8)
    #设置刻度的字号
    
    plt.xlabel('Date',fontsize=8)
    #设置x轴标签及其字号
    
    plt.ylabel('IncRate',fontsize=8)
    #设置y轴标签及其字号
   """
   
    handles1, labels1 = ax1.get_legend_handles_labels()
    handles2, labels2 = ax2.get_legend_handles_labels()
    plt.legend(handles1+handles2, labels1+labels2, loc='upper left')
    
    #plt.legend()#显示图例，如果注释改行，即使设置了图例仍然不显示
    plt.grid(True)
    
    plt.savefig('d:/'+stockCode+'.png')
    
    plt.cla()
    plt.clf()
    plt.close()


    exit()

    
    
    print()
    
     
    
    df=sdDataAPI.fund_nav(ts_code='000218.OF',start_date='20200101',end_date='20200110')
        #end_date='20200102')
    print()