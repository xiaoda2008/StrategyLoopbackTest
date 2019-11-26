'''
Created on 2019年11月18日

@author: xiaoda
'''
from com.xiaoda.stock.loopbacktester.strategy.stockselect.StrategyParent import StrategyParent
from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlProcessor
from com.xiaoda.stock.loopbacktester.utils.StockDataUtils import StockDataProcessor
from com.xiaoda.stock.loopbacktester.utils.FinanceDataUtils import FinanceDataProcessor

class ROEStrategy(StrategyParent):
    '''
    根据股票ROE进行选股，选择ROE排在前5%的股票进行交易
    '''


    def __init__(self):
        '''
        Constructor
        '''
        self.name="ROEStrategy"


    #决定对哪些股票进行投资
    def getSelectedStockList(self,startdateStr):
        
        sdict=StockDataProcessor.getAllStockDataDict()
        
        ROEDict={}

        for (stockCode,listdate) in sdict.items():

            if listdate>startdateStr:
                continue

            #ROE=净利润/净资产
            #从资产负债表获取净资产数据
            
            bs=FinanceDataProcessor.getLatestStockBalanceSheetReport(stockCode,startdateStr)
            #bs为所有之前发布的所有资产负债表数据
            #需要到里面找到最后一个不是空的总资产

            totalAssets=bs[bs['total_assets'].notnull()].reset_index(drop=True).at[0,'total_assets']
            totalLiability=bs[bs['total_liab'].notnull()].reset_index(drop=True).at[0,'total_liab']
            netAssets=totalAssets-totalLiability
            
    
            #获取现金流量表中，现金等价物总数
            ic=FinanceDataProcessor.getLatestIncomeReport(stockCode,startdateStr)
            #cf为之前发布的所有利润表数据
            #需要到里面找到最后一个不是空的现金等价物数据
            ebit=ic[ic['ebit'].notnull()].reset_index(drop=True).at[0,'ebit']

        
            if bs.empty or ic.empty:
                ROE=0
            else:
                ROE=ebit/netAssets
    
            ROEDict[stockCode]=ROE
            
            self.log.logger.info('ROEStrategy:'+stockCode+','+str(ROE))

        sortedROEList=sorted(ROEDict.items(),key=lambda x:x[1],reverse=True)

        returnStockList=[]

        for tscode, ROE in sortedROEList[:30]:
            returnStockList.append(tscode)
        
        return returnStockList     