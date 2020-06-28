'''
Created on 2020年6月28日

@author: xiaoda
'''
from com.xiaoda.stock.loopbacktester.utils.ParamUtils import OUTPUTDIR
from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlProcessor
from com.xiaoda.stock.loopbacktester.utils.StockSelectStrategyUtil import StockSelectStrategyProcessor
from com.xiaoda.stock.loopbacktester.utils.TradeStrategyUtil import TradeStrategyProcessor
from pathlib import Path
from com.xiaoda.stock.loopbacktester.LoopbackTester import compAndOutputXLSAndPlot
import pandas as pd


def getAvgProfitRateForSingleStrategy(stockSelectStrategyName,tradeSelectStrategyName,startdate,enddate):
    #stockSelectStrategy=StockSelectStrategyProcessor().getStrategy(stockSelectStrategyName)
    #tradeStrategy=TradeStrategyProcessor().getStrategy(tradeSelectStrategyName)    
    mysqlProcessor=MysqlProcessor()
    sdf=mysqlProcessor.querySql('select content from u_data_desc where content_name=\'data_end_dealday\'')
    enddate=sdf.at[0,'content']
    
    OODir=OUTPUTDIR+'/'+startdate+'-'+enddate
    irrSummaryLoc=OODir+'/'+stockSelectStrategyName+'/'+tradeSelectStrategyName+'/Summary-xirr.csv'
    myPath=Path(irrSummaryLoc)
    
    #如果该位置不存在
    #说明该区间的数据还未进行测算
    #需要进行测算
    #如果已经存在，说明已经有结果，可以直接取结果
    if not(myPath.exists()):
        compAndOutputXLSAndPlot(stockSelectStrategyName, tradeSelectStrategyName, startdate, enddate)

    
    data=pd.read_csv(irrSummaryLoc,index_col='日期',encoding='gbk')
    
    print(data)
    
    avgProfitRate=0    
    return avgProfitRate
    
    
    
if __name__ == '__main__':
    '''
    穷举各个时间，计算每一个买入日期之下，获得正收益需要的时间，以及获得20%，30%，50%收益需要的时间
    半年内获得正收益的概率，一年之内获得正收益的概率，一年半、两年获得正收益的概率
    买入后，一年、两年、三年内平均年化收益
  买入后，超越指数的概率，平均超越指数的数量
  
    '''
    
    #起始测试日期，都是1月1日，最早为2011年1月1日（再早没有创业板指数）
    startdate='2019'+'0101'
    
    #结束测试日期,都是12月31日，最晚为2019年12月31日
    enddate='2019'+'1231'
    
    #用于对照的指数
    cmpIdx='HS300'
    
    
    stockSelectStrategyName='CashCowStrategy'
    tradeSelectStrategyName='HoldStrategy'
    
    avgPR=getAvgProfitRateForSingleStrategy(stockSelectStrategyName,tradeSelectStrategyName,startdate,enddate)
    
    
    print('The average profit rate from %s to %s is %f')%(startdate,enddate,avgPR)
    
    
    
    
    
    pass