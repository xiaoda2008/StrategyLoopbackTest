'''
Created on 2019年12月24日

@author: xiaoda
'''
from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlProcessor
from com.xiaoda.stock.loopbacktester.utils.StockDataUtils import StockDataProcessor
from com.xiaoda.stock.loopbacktester.strategy.trade.BuylowSellhighStrategy import BuylowSellhighStrategy
from timeit import default_timer as timer
import time

from com.xiaoda.stock.loopbacktester.utils.TradeStrategyUtil import TradeStrategyProcessor

if __name__ == '__main__':
    
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