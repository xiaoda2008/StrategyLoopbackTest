'''
Created on 2020年1月19日

@author: xiaoda
'''
import tushare
import sqlalchemy
from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlProcessor

if __name__ == '__main__':
    #使用TuShare pro版本    
    tushare.set_token('221f96cece132551e42922af6004a622404ae812e41a3fe175391df8')
    sdDataAPI=tushare.pro_api()
   
    mysqlProcessor=MysqlProcessor()
    
    #写入数据库的引擎
    mysqlEngine=mysqlProcessor.getMysqlEngine()
    mysqlSession=mysqlProcessor.getMysqlSession()
    
    DAYONE='19900101'

    indexDF=sdDataAPI.index_daily(ts_code='399006.SZ',start_date=DAYONE,end_date='20200606')
    
    print()
 
    try:
        #将指数数据存入数据库表中
        indexDF.to_sql(name='u_idx_cyb',con=mysqlEngine,chunksize=1000,if_exists='append',index=None)
    except Exception as e:
        if type(e)==sqlalchemy.exc.IntegrityError:
            pass
        else:
            raise e    

 
 
    
    df=sdDataAPI.fund_nav(ts_code='000218.OF',start_date='20200101',end_date='20200110')
        #end_date='20200102')
    print()