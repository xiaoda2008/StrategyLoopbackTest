'''
Created on 2019年10月30日

@author: picc
'''
import math
from com.xiaoda.stock.strategies.StrategyParent import StrategyParent
from com.xiaoda.stock.loopbacktester.utils.ParamUtils import *

class SimplePlusSMAStrategy(StrategyParent):
    '''
    将基础的策略与SMA策略相结合
    在涨跌幅达到一定程度，且MA20满足一定条件的情况下才进行买卖
    '''


