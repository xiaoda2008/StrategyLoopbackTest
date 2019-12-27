'''
Created on 2019年12月26日

@author: xiaoda
'''
#对每支个股分析其当前估值
#相对过去三年处于什么水平
#可以形成一个曲线，画出来
from com.xiaoda.stock.loopbacktester.utils.StockDataUtils import StockDataProcessor
from com.xiaoda.stock.loopbacktester.utils.FinanceDataUtils import FinanceDataProcessor
from com.xiaoda.stock.loopbacktester.test.TestPy import todayDate
from datetime import datetime as dt

todayDt=dt.now().strftime('%Y%m%d')

threeYearBfDt=StockDataProcessor.getCalDayByOffset(todayDt,-1095)

#fourYearBfDt=StockDataProcessor.getCalDayByOffset(todayDt,-1460)

bs=FinanceDataProcessor().getLatestBalanceSheetReport('000001.SZ',todayDate)

ic=FinanceDataProcessor().getLatestIncomeReport('000001.SZ',todayDate)

cf=FinanceDataProcessor().getLatestCashFlowReport('000001.SZ',todayDate)


#以日为单位，PE
pettmDF=pd.DataFrame(columns=('日期','PE-TTM'))
#以日为单位，PB
pbttmDF=pd.DataFrame(columns=('日期','PB-TTM'))



#以年度为单位？
nprDF=pd.DataFrame(columns=('日期','净利润率'))
#以年度为单位？
gprDF=pd.DataFrame(columns=('日期','毛利润率'))
#以年度为单位?
revenueDF=pd.DataFrame(columns=('日期','营业收入'))

#输出两个：
#一个csv，保存数据
#一个图片，画图输出


currday=StockDataProcessor.getNextDealDay(threeYearBfDt,True)

while currday<todayDt:
    
    #计算当天股价对应的PE值
    #收盘总市值：
    totalMarketVal=
    
    
    currday=StockDataProcessor.getNextDealDay()



#指数市盈率？

当前PE-TTM
过去三年平均PE-TTM，按天平均?



#指数市净率？
当前PB-TTM
过去三年平均PB-TTM，按天平均？



#并对公司的盈利能力、利润总额、收入总额、毛利率、净利率等
#相对过去三年处于什么水平

当前盈利能力：净利润率、毛利润率

营业利润、利润总额、营业收入的相对变化







#实现对个股的相对估值水平的评判



