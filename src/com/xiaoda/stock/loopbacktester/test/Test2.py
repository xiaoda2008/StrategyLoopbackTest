'''
Created on 2019年12月24日

@author: xiaoda
'''
from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlProcessor
from com.xiaoda.stock.loopbacktester.utils.StockDataUtils import StockDataProcessor

if __name__ == '__main__':
    processor=MysqlProcessor()
    session=processor.getMysqlSession()
    
    processor.querySql("select * from u_stock_list")
    
    print()
    
    
    sdProcessor=StockDataProcessor()
    
    print()