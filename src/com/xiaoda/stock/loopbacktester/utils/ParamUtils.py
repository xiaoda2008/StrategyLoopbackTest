'''
Created on 2019年10月28日

@author: picc
'''


#股票代码库
#INPUTFILE = 'D:/stockList.txt'
OUTPUTDIR = 'D:/outputDir'


#换成tushare pro之后，不能间隔超过4000个交易日（约18年）
#STARTDATE = '20190701'#20071016
#ENDDATE = '20191126'#20081031

#1、单边下跌：
#20071016-20081031
#20180125-20190103

#2、单边上涨
#20190104-20190408



#LOGGINGDIR = 'D:/outputDir'

#mysql连接地址
mysqlURL = 'mysql+pymysql://ts:ts@localhost/tsdb?charset=utf8mb4'

#上涨与下跌幅度的参数
UPRATE = 0.1
DOWNRATE = 0.15

nShare = 3 #第一次买入的数量，默认为10手，部分策略中，后续买入的数量，与该参数也相关

#券商佣金费率
SecChargeRate = 0.00025


#标记是否进行止损
profitStop=True
