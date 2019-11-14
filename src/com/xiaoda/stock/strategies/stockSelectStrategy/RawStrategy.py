'''
Created on 2019年11月12日

@author: picc
'''
from com.xiaoda.stock.strategies.stockSelectStrategy.StrategyParent import StrategyParent
from datetime import datetime as dt
import time
from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlProcessor

class RawStrategy(StrategyParent):
    '''
    classdocs
    '''

    
    #决定对哪些股票进行投资
    def getSelectedStockList(self):
        
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
        
        return returnStockList