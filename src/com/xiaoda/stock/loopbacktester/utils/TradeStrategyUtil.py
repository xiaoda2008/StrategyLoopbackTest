'''
Created on 2019年12月19日

@author: xiaoda
'''
from com.xiaoda.stock.loopbacktester.strategy.trade.BuylowSellhighStrategy import BuylowSellhighStrategy
from com.xiaoda.stock.loopbacktester.strategy.trade.FloatingBuylowSellhighStrategy import FloatingBuylowSellhighStrategy
from com.xiaoda.stock.loopbacktester.strategy.trade.MultiStepStrategy import MultiStepStrategy
from com.xiaoda.stock.loopbacktester.strategy.trade.FloatingMultiStepStrategy import FloatingMultiStepStrategy
from com.xiaoda.stock.loopbacktester.strategy.trade.HoldStrategy import HoldStrategy
from com.xiaoda.stock.loopbacktester.strategy.trade.SMAStrategy import SMAStrategy
from com.xiaoda.stock.loopbacktester.strategy.trade.BLSHPlusMAStrategy import BLSHPlusMAStrategy

class TradeStrategyProcessor(object):
    '''
    classdocs
    '''


    def __init__(self):
        '''
        Constructor
        '''
        self.blshStrategy=BuylowSellhighStrategy()
        self.fblshStrategy=FloatingBuylowSellhighStrategy()
        self.msStrategy=MultiStepStrategy()
        self.fmsStrategy=FloatingMultiStepStrategy()
        self.hStrategy=HoldStrategy()
        self.SMAStrategy=SMAStrategy()
        self.blshPlusMAStrategy=BLSHPlusMAStrategy()
    
    
    def getStrategy(self,strategyName):
        if strategyName==self.blshStrategy.getStrategyName():
            return self.blshStrategy
        elif strategyName==self.fblshStrategy.getStrategyName():
            return self.fblshStrategy
        elif strategyName==self.msStrategy.getStrategyName():
            return self.msStrategy
        elif strategyName==self.fmsStrategy.getStrategyName():
            return self.fmsStrategy
        elif strategyName==self.hStrategy.getStrategyName():
            return self.hStrategy
        elif strategyName==self.SMAStrategy.getStrategyName():
            return self.SMAStrategy
        elif strategyName==self.blshPlusMAStrategy.getStrategyName():
            return self.blshPlusMAStrategy
        else:
            return None