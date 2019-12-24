'''
Created on 2019年12月23日

@author: xiaoda
'''
import os
from com.xiaoda.stock.loopbacktester.strategy.stockselect.StrategyParent import StrategyParent
from com.xiaoda.stock.loopbacktester.utils.FinanceDataUtils import FinanceDataProcessor
from com.xiaoda.stock.loopbacktester.utils.LoggingUtils import Logger


class GrossProfitRateStrategy(StrategyParent):
    '''
    classdocs
    '''
    
    
    log = Logger(os.path.split(__file__)[-1].split(".")[0]+'.log',level='info')
    
    def __init__(self):
        '''
        Constructor
        '''
        self.name="GrossProfitRateStrategy"
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
                operateprofit1=ic[ic['operate_profit'].notnull()].reset_index(drop=True).at[0,'operate_profit']
                operateprofit2=ic[ic['operate_profit'].notnull()].reset_index(drop=True).at[1,'operate_profit']           
                operateprofit3=ic[ic['operate_profit'].notnull()].reset_index(drop=True).at[2,'operate_profit']
                operateprofit4=ic[ic['operate_profit'].notnull()].reset_index(drop=True).at[3,'operate_profit']                
                
                #总收入
                totalRavenue1=ic[ic['total_revenue'].notnull()].reset_index(drop=True).at[0,'total_revenue']
                totalRavenue2=ic[ic['total_revenue'].notnull()].reset_index(drop=True).at[1,'total_revenue']
                totalRavenue3=ic[ic['total_revenue'].notnull()].reset_index(drop=True).at[2,'total_revenue']
                totalRavenue4=ic[ic['total_revenue'].notnull()].reset_index(drop=True).at[3,'total_revenue']
            except KeyError:
                
                continue

 
            if operateprofit2>operateprofit1 or operateprofit3>operateprofit2 or operateprofit4>operateprofit3:
                continue
            elif totalRavenue2>totalRavenue1 or totalRavenue3>totalRavenue2 or totalRavenue4>totalRavenue3:
                continue
            elif operateprofit4<100000000:
                #对于净利润小于1亿直接排除
                continue
            else:
                ratio=(operateprofit1+operateprofit2+operateprofit3+operateprofit4)/(totalRavenue1+totalRavenue2+totalRavenue3+totalRavenue4)
    
            npRatioDict[stockCode]=ratio
            
            self.log.logger.info('GrossProfitRateStrategy:'+stockCode+','+str(ratio))

        sortedNPRatioList=sorted(npRatioDict.items(),key=lambda x:x[1],reverse=True)

        returnStockList=[]

        for tscode, ratio in sortedNPRatioList[:30]:
            returnStockList.append(tscode)
        
        return returnStockList