'''
Created on 2019年10月28日

@author: picc
'''
#股票代码库
INPUTFILE = 'D:/stockList.txt'
OUTPUTDIR = 'D:/outputDir'
STARTDATE = '2019-01-04'
ENDDATE = '2019-04-08'


#上涨与下跌幅度的参数
UPRATE = 0.1
DOWNRATE = 0.15

nShare = 10 #第一次买入的数量，默认为10手，部分策略中，后续买入的数量，与该参数也相关