'''
Created on 2019年10月28日

@author: picc
'''
#股票代码库
#INPUTFILE = 'D:/stockList.txt'
OUTPUTDIR = 'D:/outputDir'


#换成tushare pro之后，不能间隔超过4000个交易日（约18年）
STARTDATE = '20190104'#20071016
ENDDATE = '20190408'#20081031



LOGGINGDIR = 'D:/outputDir'

#mysql连接地址
mysqlURL = 'mysql+pymysql://root:xiaoda001@localhost/tsdata?charset=utf8'

#上涨与下跌幅度的参数
UPRATE = 0.1
DOWNRATE = 0.15

nShare = 3 #第一次买入的数量，默认为10手，部分策略中，后续买入的数量，与该参数也相关

#券商佣金费率
SecChargeRate = 0.00025
