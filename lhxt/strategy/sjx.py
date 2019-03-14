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
    def __init__(self,heartbeat=True,isstock=False):
        self.output =  "{}/{}".format("../output",os.path.basename(__file__))
        super(data,self).__init__(heartbeat=heartbeat,isstock=isstock)
        self.createdir(self.output) #创建输出目录
    
    def handeldata(self,df,args=[3,12,50,200],macd_f=12,macd_s=26,macd_m=9):
        df.loc[:,"number"] = range(df.shape[0]) 
        for i in args:#5 15 60 D
            df.loc[:,str(i)+"ma"] = df["close"].ewm(adjust=False,span=i*20).mean()
            
            pres = str(i)+"_"
            #计算各周期初始macd
            df.loc[::i,pres+"DIF"] ,df.loc[::i,pres+"DEA"],df.loc[::i,pres+"MACD"] = talib.MACD(df.close.values[::i],
                                                                                     fastperiod=macd_f,
                                                                                     slowperiod=macd_s,
                                                                                     signalperiod=macd_m) 
            #计算各个采样点的快线
            df.loc[:,pres+"close_delter"] = 2/(macd_f+1)*df["close"]-2/(macd_s+1)*df["close"]
            df.loc[::i,pres+"close_mark_delter"] = df[pres+"close_delter"][::i]
            df.fillna(method="ffill",inplace=True)
            df.loc[:,pres+"DIF"] += df[pres+"close_delter"] - df[pres+"close_mark_delter"]  #计算快线
            
            #计算各采样点慢线
            df.loc[:,pres+"DIF_delter"] = 2/(macd_m+1)*df[pres+"DIF"]
            df.loc[::i,pres+"DIF_mark_delter"] = df[pres+"DIF_delter"][::i]
            df.fillna(method="ffill",inplace=True)
            df.loc[:,pres+"DEA"] += df[pres+"DIF_delter"] - df[pres+"DIF_mark_delter"] #计算慢线
            df.loc[:,pres+"MACD"] = df[pres+"DIF"] - df[pres+"DEA"] #计算差值
            
            #标记各周期每个整点的上一次最新值
            df.loc[::i,pres+"DIF_LAST"] = df[pres+"DIF"][::i].shift(1)
            df.loc[::i,pres+"DEA_LAST"] = df[pres+"DEA"][::i].shift(1)
            df.loc[::i,pres+"MACD_LAST"] = df[pres+"MACD"][::i].shift(1)
            df.fillna(method="ffill",inplace=True)
            
            
        df.fillna(method="ffill",inplace=True)    
        return df
    
    def backtest(self,df,args=[3,12,50,200],product="RBL8"):
        df = self.handeldata(df,args)
        filt0 = df[str(args[0])+"_MACD"]>0
        filt1 = df[str(args[1])+"_MACD"]>0
        filt2 = df[str(args[2])+"_MACD"]>0
        filt3 = df[str(args[3])+"_MACD"]>0
        
        
        filt = filt0&filt1&filt2&filt3 #1,1,1,1
        
        df.loc[df["upfilt"]>0,"P"] = df["close"]-df["close"].shift(1)
        df.loc[df["upfilt"]<0,"P"] = df["close"].shift(1)-df["close"]
        
        df.loc[(df["upfilt"]>0)&(df["upfilt"].shift(1)==0),"P"] = 0
        df.loc[(df["upfilt"]<0)&(df["upfilt"].shift(1)==0),"P"] = 0 
        
        df.loc[(df["upfilt"]>=0)&(df["upfilt"].shift(1)<0),"P"] = df["close"].shift(1)-df["close"]
        
        df.loc[(df["upfilt"]<=0)&(df["upfilt"].shift(1)>0),"P"] = df["close"]-df["close"].shift(1)
        
        df.loc[:,"P"] = df["P"]*df["weight"].shift(1)
        
        df.loc[:,"change"] = df["upfilt"] != df["upfilt"].shift(1)   
        
        print(product,args,df[df["change"]>0].shape[0],df["P"].sum())
        self.result.append([args,df[df["change"]>0].shape[0],df["P"].sum()] )
        
        return df
    
    def run(self,product="ICL8",market=47,rg=[(4,8,1),(5,40,2),(20,450,5)]):
        df = self.getdata(product,market,number=self.number)
        df1 = df.copy()
        df = self.backtest(df,[3,12,50,200],product)
        
                    
        select_rst = pd.DataFrame(self.result,columns=["args","number","{}_profit".format(product)])
        select_rst.to_csv("{}/args_{}.csv".format(self.output,product))
        
    def merge(self,dct={},fname="total_result.html"):
        '''dct:测试对象列表
            example：dct = {
                "RBL9":[(5,35,125),(6,35,105),(7,35,125)],
                }
        '''
        
        draw = Draw(self.output+"/"+fname)
        datelist = pd.date_range(start="2013-05-01",end="2019-09-28",freq="5min")
        b = [] #处理时间顺序，夜盘时间标记的第二天的时间
        for i in range(int(len(datelist)/288)):
            b.extend(datelist[i*288+200:i*288+288])
            b.extend(datelist[i*288:i*288+200])
        result = pd.DataFrame(np.zeros([len(b),1]),index=b,columns=["total",] )
        
        result.index = result.index.strftime('%Y-%m-%d %H:%M')
        keys = []
        for p,args in dct.items():
            if self.isstock:
                df1 = self.getdata(p,number=self.number)
            else:
                print(p)
                mk = self.market[self.market["desc"]==p]["market"].values[0]
                df1 = self.getdata(p,mk,number=self.number)
            draw_n = Draw("{}/{}.html".format(self.output,p))
            for v in args:
                df = df1.copy()
                rst = self.backtest(df,v[0],v[1],v[2],p)
    #             rst.index = pd.to_datetime(rst.index) #索引用时间序列画图会异常
                k = "{}_{}".format(p,v)
                result.loc[result.index.isin(rst.index),k] = rst["P"]/rst["close"].shift(1) #百分比计算
#                 result.loc[result.index.isin(rst.index),k] = rst["P"]*PERMONEY[p[:-4]+"L9"] # 一手回测
                
                keys.append(k)
                rst["P"].fillna(0,inplace=True)
                draw_n.add(k,rst.index,{"profit":rst["P"].cumsum().values})
            draw_n.draw()
            
        result.fillna(0,inplace=True)
        result.loc[:,"total"] = result[keys].sum(axis=1)/len(keys)
        
        result = result[result["total"]!=0]
        
        draw.add("profit",result.index,{"profit":result["total"].cumsum().values})
        draw.add("profit_small",result.index[-600:],{"profit":result["total"].cumsum().values[-600:]})
        draw.draw()
        
    def train(self,products=[],number=10000,datatype=1,rg=[(1,4,1),(9,15,1),(40,60,2),(160,240,5)]):
        '''number:数据抓取数量
           datatype:数据周期
           rg: 参数寻优区间和步长
        '''
        self.connect()
        self.setmarket()
        self.number = number
        self.datatype = datatype
        
        if not products:
            products = ["RBL9","PPL9","SML9","SFL9","MAL9",
                        "ZCL9","CFL9","JML9","SNL9",
                        "NIL9","SCL9","TAL9","BUL9",
                        "JDL9","RUL9","ML9","HCL9",
                        "JL9","BL9","CSL9",
                        "IL9","AL9","CL9","VL9",
                        "RML9","PL9"]
        for p in products:
            mk = self.market[self.market["desc"]==p]["market"].values[0]
            print(p,mk)
            self.result = []
            self.run(p,mk,rg=rg)
    
    def test(self,number=30000,datatype=1):
        self.connect()
        self.setmarket()
        self.datatype = datatype #0:5min数据 ,1:15min数据
        self.number = number
   
        #备注:部分品种前面几年没有交易或者没有成交量，会导致前面几年的净值曲线变差
        if datatype==1:
            dct = {
                    "PPL9":[(2,18,21)],
                    }
        elif datatype==0:
            dct = {
                    "HCL9":[(2,12,63)],"NIL9":[(3,12,51)],"PBL9":[(2,34,51)],
                    "RBL9":[(2,12,57)],"RUL9":[(3,24,57)],"APL9":[(2,34,114)],
                    "CFL9":[(5,24,60)],"SFL9":[(4,18,84)],"SML9":[(4,18,66)],
                    "JDL9":[(3,20,57)],"PPL9":[(4,22,45)],"VL9" :[(4,18,51)],
                    "CSL9":[(2,28,51)],"CL9" :[(5,10,51)],"JL9" :[(5,44,111)],
                    "BUL9":[(4,30,90)],"IL9" :[(4,18,48)],"ALL9":[(3,42,66)]
                    }
    
        self.merge(dct=dct,fname="total_result_all.html")
        
if __name__=="__main__":
    d = data()
    d.train()
    d.test()




