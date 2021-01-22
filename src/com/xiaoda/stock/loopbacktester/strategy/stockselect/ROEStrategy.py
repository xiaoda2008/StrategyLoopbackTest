'''
Created on 2019年11月18日

@author: xiaoda
'''
import os
from com.xiaoda.stock.loopbacktester.strategy.stockselect.StrategyParent import StrategyParent
from com.xiaoda.stock.loopbacktester.utils.FinanceDataUtils import FinanceDataProcessor
from com.xiaoda.stock.loopbacktester.utils.LoggingUtils import Logger
from com.xiaoda.stock.loopbacktester.strategy.utils.RiskAvoidUtil import RiskAvoidProcessor
from com.xiaoda.stock.loopbacktester.strategy.utils.StockListFilterUtil import StockListFilterProcessor



class ROEStrategy(StrategyParent):
    '''
    根据股票ROE进行选股，选择ROE排在前5%的股票进行交易
    '''
    log = Logger(os.path.split(__file__)[-1].split(".")[0]+'.log',level='info')
    
    def __init__(self):
        '''
        Constructor
        '''
        self.name="ROEStrategy"
        self.finProcessor=FinanceDataProcessor()

    #决定对哪些股票进行投资
    def getSelectedStockList(self,sdProcessor,startdateStr):

        #sdict=sdProcessor.getZZ500Dict()        
        sdict=sdProcessor.getHS300Dict()
        ROEDict={}
        
        for (stockCode,scdict) in sdict.items():
            if stockCode=="600519.SH":
                continue
                        
            listdate=scdict['list_date']
            
            if listdate>startdateStr:
                continue



            #ROE=净利润/净资产
            #从资产负债表获取净资产数据
            
            bs=self.finProcessor.getLatestBalanceSheetReport(stockCode,startdateStr,False)
            #bs为所有之前发布的所有资产负债表数据
            
            ic=self.finProcessor.getLatestIncomeReport(stockCode,startdateStr,False)
            #ic为之前发布的所有利润表数据
            
            #获取现金流量表中，现金等价物总数
            cf=self.finProcessor.getLatestCashFlowReport(stockCode,startdateStr,False)
            #cf为之前发布的所有现金流量表数据
            
            #有可能数据不全，直接跳过
            if bs.empty or ic.empty:
                continue
            
            
            #需要到里面找到最后一个不是空的总资产

            totalAssets=bs[bs['total_assets'].notnull()].reset_index(drop=True).at[0,'total_assets']
            totalLiability=bs[bs['total_liab'].notnull()].reset_index(drop=True).at[0,'total_liab']
            netAssets=totalAssets-totalLiability
            

            #需要到里面找到最后一个不是空的现金等价物数据
            nincome=ic[ic['n_income'].notnull()].reset_index(drop=True).at[0,'n_income']


            #防暴雷、防财务造假逻辑
            if RiskAvoidProcessor.getRiskAvoidFlg(stockCode, ic, bs, cf, sdProcessor)==True:
                continue
             
        
            if bs.empty or ic.empty:
                #ROE=0
                continue
            else:
                ROE=nincome/netAssets
    
            ROEDict[stockCode]=ROE
            
            self.log.logger.info('ROEStrategy:'+stockCode+','+str(ROE))

        sortedROEList=sorted(ROEDict.items(),key=lambda x:x[1],reverse=True)

        tmpStockList=[]

        for tscode, ROE in sortedROEList[:30]:
            tmpStockList.append(tscode)


        #删选以避免某一行业占比过高
        returnStockList=StockListFilterProcessor.filterStockList(tmpStockList, sdProcessor)        

        #选出的股票不要少于10支
        if len(returnStockList)<10:
            tmpStockList=[]
            for tscode, ratio in sortedROEList[:50]:
                tmpStockList.append(tscode)
            
            #删选以避免某一行业占比过高
            returnStockList=StockListFilterProcessor.filterStockList(tmpStockList, sdProcessor)      
 
             
        return returnStockList