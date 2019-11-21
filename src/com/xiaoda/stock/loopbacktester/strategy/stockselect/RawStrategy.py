'''
Created on 2019年11月12日

@author: picc
'''
from com.xiaoda.stock.loopbacktester.strategy.stockselect.StrategyParent import StrategyParent
from com.xiaoda.stock.loopbacktester.utils.StockDataUtils import StockDataProcessor

class RawStrategy(StrategyParent):
    '''
    classdocs
    '''
    
    def __init__(self):
        '''
        Constructor
        '''
        self.name="RawStrategy"

    
    #决定对哪些股票进行投资
    def getSelectedStockList(self,startdateStr):
        
        #在MysqlProcessor获取股票列表时
        #已经剔除掉了退市股和ST、*ST等
        '''
        returnStockList=[]
        
        sdf=MysqlProcessor.getStockList()
        #sdf = sdDataAPI.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')

        stockDict={}

        for idx in sdf.index:
            stockDict[sdf.at[idx,'ts_code']]=sdf.at[idx,'name']
            
        for stockCode,stockName in stockDict.items():
            if 'ST' in stockName or '退' in stockName:
                #print(stockCode,'为ST股或即将退市股，剔除')
                continue
            else:
                returnStockList.append(stockCode)
        '''
        
        sdict=StockDataProcessor.getAllStockDataDict()
        
        returnStockList=[]
        
        
        for (stockCode,listdate) in sdict.items():
            if listdate>startdateStr:
                continue
            returnStockList.append(stockCode)
            
        return returnStockList
    
