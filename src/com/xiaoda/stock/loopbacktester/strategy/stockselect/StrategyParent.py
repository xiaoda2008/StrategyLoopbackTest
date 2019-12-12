'''
Created on 2019年10月28日

@author: picc
'''

class StrategyParent:
    
    def __init__(self,name):
        self.name=name
    
    def getStrategyName(self):
        return self.name
    
    
    #可以考虑将买入/卖出进行合并，返回值直接使用正负号代表买入/卖出
    
    
    #决定对哪些股票进行投资
    def getSelectedStockList(self,sdProcessor,dateStr):
        return 0
