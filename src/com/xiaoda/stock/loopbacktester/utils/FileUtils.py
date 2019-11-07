'''
Created on 2019年11月6日

@author: picc
'''
import csv
import pandas

class FileProcessor(object):
    '''
    从各个股票的交易历史中，抽取出每天的资金占用
    用于计算实际的策略IRR
    '''


    def __init__(self, params):
        '''
        Constructor
        '''
    
    @staticmethod
    def readFile(fileStr):
        '''
            从文件中读取出内容，返回DataFrame
        '''
        tmp_lst = []
        with open(fileStr, 'r') as f:
            reader=csv.reader(f)
            for row in reader:
                tmp_lst.append(row)
        df = pandas.DataFrame(tmp_lst[1:], columns=tmp_lst[0]) 
        return df
    
    
    