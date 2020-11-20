'''
Created on 2019年12月22日

@author: xiaoda
'''
from com.xiaoda.stock.loopbacktester.strategy.stockselect.CashCowStrategy import CashCowStrategy
from com.xiaoda.stock.loopbacktester.strategy.stockselect.CCPlusNPRStrategy import CCPlusNPRStrategy
from com.xiaoda.stock.loopbacktester.strategy.stockselect.NetProfitRateStrategy import NetProfitRateStrategy
from com.xiaoda.stock.loopbacktester.strategy.stockselect.RawStrategy import RawStrategy
from com.xiaoda.stock.loopbacktester.strategy.stockselect.ROEStrategy import ROEStrategy
from com.xiaoda.stock.loopbacktester.strategy.stockselect.GrossProfitRateStrategy import GrossProfitRateStrategy
from com.xiaoda.stock.loopbacktester.strategy.stockselect.SelfSelectedStocksStrategy import SelfSelectedStocksStrategy
from com.xiaoda.stock.loopbacktester.strategy.stockselect.LowValuationStrategy import LowValuationStrategy
from com.xiaoda.stock.loopbacktester.strategy.stockselect.ROICStrategy import ROICStrategy

class StockSelectStrategyProcessor(object):
    '''
    classdocs
    '''


    def __init__(self):
        '''
        Constructor
        '''
        self.rawStrategy=RawStrategy()
        self.roeStrategy=ROEStrategy()
        self.roicStrategy=ROICStrategy()
        self.ccStrategy=CashCowStrategy()
        self.nprStrategy=NetProfitRateStrategy()
        self.ccplusNPRStrategy=CCPlusNPRStrategy()
        self.gprStrategy=GrossProfitRateStrategy()
        self.sssStrategy=SelfSelectedStocksStrategy()
        self.lvStrategy=LowValuationStrategy()
    
    def getStrategy(self,strategyName):
        if strategyName==self.rawStrategy.getStrategyName():
            return self.rawStrategy
        elif strategyName==self.roeStrategy.getStrategyName():
            return self.roeStrategy
        elif strategyName==self.roicStrategy.getStrategyName():
            return self.roicStrategy
        elif strategyName==self.ccStrategy.getStrategyName():
            return self.ccStrategy
        elif strategyName==self.nprStrategy.getStrategyName():
            return self.nprStrategy
        elif strategyName==self.ccplusNPRStrategy.getStrategyName():
            return self.ccplusNPRStrategy
        elif strategyName==self.gprStrategy.getStrategyName():
            return self.gprStrategy
        elif strategyName==self.sssStrategy.getStrategyName():
            return self.sssStrategy
        elif strategyName==self.lvStrategy.getStrategyName():
            return self.lvStrategy        
        else:
            return None
        
        
        
        
        