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
    def getSelectedStockList(self,dateStr):
        
        stockList=StockDataProcessor.getAllStockList()
        
        ROEDict={}

        for stockCode in stockList:

            bs=FinanceDataProcessor.getLatestStockBalanceSheet(stockCode,dateStr)
            #获取资产负债表，总资产-总负债
            bs.at[0,'total_assets']-bs.at[0,'total_liab']
    
            #获取现金流量表中，净利润
            cf=FinanceDataProcessor.getLatestStockCashFlow(stockCode,dateStr)
            cf.at[0,'net_profit']
        
            if bs.empty or cf.empty:
                roe=0
            else:
                roe=float(cf.at[0,'net_profit'])/(float(bs.at[0,'total_assets'])-float(bs.at[0,'total_liab']))
    
            ROEDict[stockCode]=roe
            
            #print('CashCowStrategy:'+stockCode+','+str(ratio))

        sortedROEList=sorted(ROEDict.items(),key=lambda x:x[1],reverse=True)

        returnStockList=[]

        for tscode, ratio in sortedROEList[:100]:    
            returnStockList.append(tscode)
        
        return returnStockList       