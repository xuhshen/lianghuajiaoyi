'''
Created on 2019年3月14日

@author: 04yyl

策略思想：双均线策略 ，金叉买入，死叉卖出
'''

from strategy.base import data
from tools import Draw
import numpy as np
import pandas as pd
import os
import talib
class data(data):
    def __init__(self,heartbeat=True,isstock=True):
        self.output =  "{}/{}".format("../output",os.path.basename(__file__)) #存放输出结果的目录
        super(data,self).__init__(heartbeat=heartbeat,isstock=isstock)
        self.createdir(self.output) #创建输出目录
    
    def handeldata(self,df,args=[]):
        '''计算args中均线的移动均值
        '''
        for i in args:# 
            pres = str(i)+"_"
            df.loc[:,pres+"ma"] = df["close"].ewm(adjust=False,span=i).mean()
        return df
    
    def backtest(self,df,args=[5,10],product="RBL8"):
        
        #计算信号点
        df.loc[:,"buy"] = df[str(args[1])+"_ma"]<df[str(args[0])+"_ma"]  #短均线再长均线上方，持有
        df.loc[:,"sell"] = df[str(args[1])+"_ma"]>=df[str(args[0])+"_ma"] #长均线在短均线上方，空仓
        
        #计算持仓收益
        df.loc[df["buy"]>0,"P"] = df["close"]/df["close"].shift(1)
        df.loc[df["sell"]>0,"P"] = 1
        
        df.loc[(df["buy"]>0)&(df["buy"].shift(1)==0),"P"] = 1 #处理买点收益计算
        df.loc[(df["sell"]>0)&(df["sell"].shift(1)==0),"P"] = df["close"]/df["close"].shift(1) #处理卖点收益计算
        
        df.loc[:,"P"] = df["P"].cumprod() # 收益进行复利计算，这里按照固定手数进行回测
        df.loc[:,"change"] = df["buy"] != df["buy"].shift(1)   #结算交易次数，用来评估手续费 
        
        print(product,args,df[df["change"]>0].shape[0],df.iloc[-1]["P"])
        self.result.append([args,df[df["change"]>0].shape[0],df.iloc[-1]["P"]] )
        
        return df
    
    def run(self,product="000001",market=47,rg=[(5,200,5),(10,400,10)]):
        '''rg:扫参区间和步长
        '''
        df = self.getdata(product,market,number=self.number)
        #为了减少重复计算，扫参的时候会一次性计算所有均线值
        arg_s = [i for i in range(rg[0][0],rg[0][1],rg[0][2])] #短均线
        arg_l = [i for i in range(rg[0][0],rg[0][1],rg[0][2])] #长均线
        df = self.handeldata(df,args=list(set(arg_s+arg_l))) 
        
        for s in arg_s:
            for l in arg_l:
                if s>=l:continue 
                self.backtest(df.copy(),args=[s,l],product=product) #防止df被污染，这里每次循环都要重新获取原始df
                    
        select_rst = pd.DataFrame(self.result,columns=["args","number","{}_profit".format(product)]) 
        select_rst.to_csv("{}/args_{}.csv".format(self.output,product)) #保存扫参结果
        
    def merge(self,dct={},fname="total_result.html"):
        '''
        '''
        
        draw = Draw(self.output+"/"+fname)
        datelist = pd.date_range(start="2005-05-01",end="2019-09-28",freq="5min")
        b = [] #处理时间顺序，夜盘时间标记的第二天的时间
        for i in range(int(len(datelist)/288)):
            b.extend(datelist[i*288+200:i*288+288])
            b.extend(datelist[i*288:i*288+200])
        result = pd.DataFrame(np.zeros([len(b),1]),index=b,columns=["total",] )
        
        result.index = result.index.strftime('%Y-%m-%d %H:%M')
        keys = []
        for p,args in dct.items():
            df1 = self.getdata(p,number=self.number)
                
            draw_n = Draw("{}/{}.html".format(self.output,p))
            for v in args:
                df = self.handeldata(df1.copy(),v)
                rst = self.backtest(df,v,p)
                print(rst)
                k = "{}_{}".format(p,v)
                result.loc[result.index.isin(rst.index),k] = rst["P"]
                
                keys.append(k)
                rst["P"].fillna(1,inplace=True)
                draw_n.add(k,rst.index,{"profit":rst["P"].values})
            draw_n.draw()
            
        result.fillna(0,inplace=True)
        result.loc[:,"total"] = result[keys].sum(axis=1)/len(keys)
        
        result = result[result["total"]!=0]
        
        draw.add("profit",result.index,{"profit":result["total"].values})
        draw.draw()
        
    def train(self,products=[],number=1000,datatype=4,rg=[(5,200,5),(10,400,10)]):
        '''number:数据抓取数量
           datatype:数据周期
           rg: 参数寻优区间和步长
        '''
        self.connect()
        self.number = number
        self.datatype = datatype
        
        if not products:
            products = ["000001",] #需要回测的股票代码
            
        for p in products:
            self.result = [] #每个股票回测的时候，清空下回测结果
            self.run(p,rg=rg)
    
    def test(self,number=2000,datatype=4):
        self.connect()
        self.datatype = datatype #0:5min数据 ,1:15min数据
        self.number = number
   
        #备注:部分品种前面几年没有交易或者没有成交量，会导致前面几年的净值曲线变差
        if datatype==4:
            dct = {
                    "000001":[(5,40)],
                    }
    
        self.merge(dct=dct,fname="total_result_all.html")
        
if __name__=="__main__":
    d = data()
#     d.train()
    d.test()




