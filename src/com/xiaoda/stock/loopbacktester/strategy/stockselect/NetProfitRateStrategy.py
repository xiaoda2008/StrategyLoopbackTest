'''
Created on 2019年12月20日

@author: xiaoda
'''

import os
from com.xiaoda.stock.loopbacktester.strategy.stockselect.StrategyParent import StrategyParent
from com.xiaoda.stock.loopbacktester.utils.StockDataUtils import StockDataProcessor
from com.xiaoda.stock.loopbacktester.utils.FinanceDataUtils import FinanceDataProcessor
from com.xiaoda.stock.loopbacktester.utils.LoggingUtils import Logger


class NetProfitRateStrategy(StrategyParent):
    '''
    classdocs
    '''
    
    
    log = Logger(os.path.split(__file__)[-1].split(".")[0]+'.log',level='info')
    
    def __init__(self):
        '''
        Constructor
        '''
        self.name="NetProfitRateStrategy"
        self.finProcessor=FinanceDataProcessor()
    
    #决定对哪些股票进行投资
    def getSelectedStockList(self,sdProcessor,startdateStr):
        
        sdict=sdProcessor.getAllStockDataDict()
        
        npRatioDict={}

        #可以考虑多进程？
        for (stockCode,scdict) in sdict.items():
            
            listdate=scdict['list_date']
            
            if listdate>startdateStr:
                continue
            
            
            ic=self.finProcessor.getLatestIncomeReport(stockCode,startdateStr,True)
            #ic为之前发布的所有利润表数据
            
            #有可能数据不全，直接跳过
            if ic.empty:
                continue
            
            #需要到里面找到最后一个不是空的总资产

            try:
                #净利润
                netIncome1=ic[ic['n_income'].notnull()].reset_index(drop=True).at[0,'n_income']
                netIncome2=ic[ic['n_income'].notnull()].reset_index(drop=True).at[1,'n_income']           
                netIncome3=ic[ic['n_income'].notnull()].reset_index(drop=True).at[2,'n_income']
                netIncome4=ic[ic['n_income'].notnull()].reset_index(drop=True).at[3,'n_income']                
                
                #总收入
                totalAvenue1=ic[ic['total_revenue'].notnull()].reset_index(drop=True).at[0,'total_revenue']
                totalAvenue2=ic[ic['total_revenue'].notnull()].reset_index(drop=True).at[1,'total_revenue']
                totalAvenue3=ic[ic['total_revenue'].notnull()].reset_index(drop=True).at[2,'total_revenue']
                totalAvenue4=ic[ic['total_revenue'].notnull()].reset_index(drop=True).at[3,'total_revenue']
            except KeyError:
                
                continue

 
            if netIncome4>netIncome3 or netIncome3>netIncome2 or netIncome2>netIncome1:
                continue
            elif totalAvenue4>totalAvenue3 or totalAvenue3>totalAvenue2 or totalAvenue2>totalAvenue1:
                continue
            elif netIncome4<100000000:
                #对于净利润小于1亿直接排除
                continue
            else:
                ratio=(netIncome1+netIncome2+netIncome3+netIncome4)/(totalAvenue1+totalAvenue2+totalAvenue3++totalAvenue4)
    
            npRatioDict[stockCode]=ratio
            
            self.log.logger.info('NetProfitRateStrategy:'+stockCode+','+str(ratio))

        sortedNPRatioList=sorted(npRatioDict.items(),key=lambda x:x[1],reverse=True)

        returnStockList=[]

        for tscode, ratio in sortedNPRatioList[:30]:
            returnStockList.append(tscode)
        
        return returnStockList
