from pytdx.hq import TdxHq_API
from pytdx.exhq import TdxExHq_API
import talib
import pandas as pd
import os,re 
from config import FILE_INCON,FILE_TDXHY,FILE_TDXZS,HY_WEIGHT,PERMONEY,MIN_STOCK_DB
from db import MongoDB
import gevent
from gevent import monkey;monkey.patch_all()
import tushare as ts
import datetime
        
class data(object):
    def __init__(self,heartbeat=True,isstock=False):
        self.api = None
        self.isstock = isstock
        self.heartbeat = heartbeat
        self.number = 80000
        self.market = None
        self.datatype = 0
        self.result = []
        self.TDX_IP_SETS = ["119.147.86.168",'119.97.185.5''202.103.36.71','139,196,185,253','61.152.107.171',]
#         218.75.74.103:7721  60.12.15.21:7721
        
        self.TDX_IP_SETS_STOCK = ['119.147.164.60','218.75.126.9', '115.238.90.165',
                 '124.160.88.183', '60.12.136.250', '218.108.98.244', '218.108.47.69',
                 '14.17.75.71', '180.153.39.51']
        
        self.file_incon = FILE_INCON
        self.file_tdxhy = FILE_TDXHY
        self.file_tdxzs = FILE_TDXZS
        self.weight = {}

    def _get_incon(self,):
        '''获取行业分类代码
        '''
        f= open(self.file_incon, "rb")
        data = f.read()
        strings = data.decode("gbk", 'ignore').rstrip("\x00").replace("\r\n","\n")
        data = strings.split("######") 
        rst = {}
        for hystr in data:
            key = re.findall(r'#.*',hystr)
            if key == ['#TDXNHY']:
                hylst = hystr.replace("#TDXNHY","").strip("\n").split("\n")
                for item in hylst:
                    k,v = item.split("|")
                    rst[k] = [v]
        return rst

    def _get_tdxhy(self,islocal=True):
        '''获取股票和行业对应列表
        '''
        if islocal:
            stocklist = HY_WEIGHT.keys()
        else:
            stocklist = list(ts.get_stock_basics().index)  #获取全市场股票代码
        
        rst = self._get_incon()
        f= open(self.file_tdxhy, "rb")
        data = f.read().decode("gbk", 'ignore').rstrip("\x00").replace("\r\n","\n").strip("\n").split("\n")
                
        for i in data:
            _,code,tdxhy,_,_ = i.split("|")
            if tdxhy != "T00" and code in stocklist:
                rst[tdxhy].append(code)
        return rst

    def _get_tdxzs(self,islocal=True):
        '''生成通达性版块代码对应股票列表
        '''
        dct = {}
        rst = self._get_tdxhy(islocal=islocal)
        f= open(self.file_tdxzs, "rb")
        data = f.read().decode("gbk", 'ignore').rstrip("\x00").replace("\r\n","\n").strip("\n").split("\n")
        for i in data:
            name,code,_,_,_,hy = i.split("|")
            code = int(code)
            if 880301<=code and 880497>=code and hy in rst.keys() :
                k = hy[:5]
                if not dct.__contains__(k):
                    dct[k] = {"name":"","code":"","stocklist":[]}
                if k==hy: 
                    dct[k]["name"] = name
                    dct[k]["code"] = code
                dct[k]["stocklist"].extend(rst[hy][1:])
        return dct

    def get_tdxhy_list(self,islocal=True):
        '''获取通达信行业板块指数对应的股票列表
        '''
        return self._get_tdxzs(islocal)
    
    def get_weight(self,htlist={},islocal=True):
        '''获取行业板块个股权重，流动市值为权重系数
                   备注：回测是为方便处理，以最后一天的权重系数作为历史上的权重
        '''
        if islocal:
            self.weight = HY_WEIGHT
        else:
            if not htlist:
                htlist = self.get_tdxhy_list(islocal)
            tasks = []
            for v in htlist.values():
                tasks.append(gevent.spawn(self.get_latest_ltsz,v["stocklist"]))
            gevent.joinall(tasks)
            
        return self.weight
 
    def get_latest_ltsz(self,stocks=[]):
        '''获取最新流通市值,千万为单位，取整
        '''
        unit = 10000000
        for code in stocks:
            mk = self._select_market_code(code)
            print(mk,code)
            try:
                ltgb = self.api.get_finance_info(mk,code)["liutongguben"]
                price = self.api.get_security_bars(4,mk,code,0,1)[0]["close"]
                ltsz = int(ltgb*price/unit)
                self.weight[code] = ltsz
            except:
                print("*****",code)
        return self.weight

    @property
    def mdb(self):
        '''设置数据库连接
        '''
        if not hasattr(self,"_db"):
            self._db = MongoDB()
        return self._db
    
    @property
    def starttime(self):
        '''采样起始时间
        '''
        if not hasattr(self, "_starttime"):
            self._starttime = "2006-01-01"
        return self._starttime
    @starttime.setter
    def starttime(self,value):
        self._starttime = value
        return self._starttime
    
    @property
    def endtime(self):
        '''采样结束时间
        '''
        if not hasattr(self, "_endtime"):
            self._endtime = str(datetime.date.today())
        return self._endtime
    @endtime.setter
    def endtime(self,value):
        self._endtime = value
        return self._endtime
    
    def getdata_m(self,collection,db=MIN_STOCK_DB,project={"open":1,"high":1,"low":1,"close":1,"datetime":1,"vol":1,"_id":0}):
        '''从数据库获取分钟数据
        '''
        filt = {"datetime":{"$gte":self.starttime,"$lte":self.endtime}}
        data = [i for i in self.mdb._dbclient(db)[collection].find(filt,project)]
        return pd.DataFrame(data)
    
    def getcollections(self,db=MIN_STOCK_DB):
        data = self.mdb.getallcollections(db)
        return data
    
    def clean_m(self,df,field="datetime"):
        '''数据库数据有些格式有问题，比如，有些11.30有数据，有些没有。有些13.00有数据，有些没有
                  导致用分钟数据生成30分钟数据时出现异常
        '''
        df[field] = df[field].str.replace("13:00","11:30")
        
        df.set_index(field,inplace=True,drop=False)
        df.index = pd.DatetimeIndex(df.index) 
        df.sort_index(inplace=True)
        df["time"] = pd.date_range('1/1/2011 00-01-00', periods=df.shape[0],freq='T')
        return df


    def createdir(self,path):
        '''创建输出目录
        '''
        if not os.path.exists(path):
            os.makedirs(path) 
        return 
    
    def setmarket(self):
        '''设置商品市场代码
        '''
        market = []
        for i in range(100):
            market += self.api.get_instrument_info(i*500, 500)
        self.market = self.api.to_df(market)
        return self.market
    
    def connect(self):
        if self.isstock:
            self.api = TdxHq_API(heartbeat=self.heartbeat)
            port = 7709
            TDX_IP_SETS = self.TDX_IP_SETS_STOCK
        else:    
            self.api = TdxExHq_API(heartbeat=self.heartbeat)
            port = 7727
            TDX_IP_SETS = self.TDX_IP_SETS
            
        for ip in TDX_IP_SETS:
            try:
                if self.api.connect(ip, port):
                    return 
            except:
                pass
    
    def disconnect(self):
        self.api.disconnect()
    
    def getdata(self,product="ICL8",market=47,number=80000,pn=400):
        if product[0] in ["0","3","6"]:
            info = self.fetch_get_stock_xdxr(product)
            data = self.getdata_stock(product,number=number,pn=pn)
            data.drop(data[data["close"]<=0].index)
            df = self.qfq(data,info)
        elif product[0] in ["8",]:
            df = self.getdata_block_index(product,market,number=number)
        else:
            df = self.getdata_future(product,market,number=number)
        return df
    
    def getdata_block_index(self,code="000001",market = 1,number=30000,pn=500):
        data = []
        start = False
        for i in range(int(number/pn)+1):
            temp = self.api.get_index_bars(self.datatype, market, code, (int(number/pn)-i)*pn,pn)
            if temp and len(temp) >0:
                start = True
            if start and (not temp or len(temp)<pn):
                for _ in range(3):
                    temp = self.api.get_index_bars(self.datatype, market, code, (int(number/pn)-i)*pn,pn)
                    if len(temp)<pn:
                        print(111111111111,pn-len(temp))
                    else:
                        break   
            try: 
                data += temp
            except:
                self.connect()    
        df = self.api.to_df(data)[["open","close","high","low","datetime"]]
        df.set_index("datetime",inplace=True,drop=False)
        return df
    
    def getdata_future(self,product="ICL8",market=47,number=80000,pn=400):
        data = []
        start = False
        for i in range(int(number/pn)+1):
            temp = self.api.get_instrument_bars(self.datatype, market, product, (int(number/pn)-i)*pn,pn)
            if temp and len(temp) >0:
                start = True
            
            if start and (not temp or len(temp)<pn):
                for _ in range(3):
                    temp = self.api.get_instrument_bars(self.datatype, market, product, (int(number/pn)-i)*pn,pn)
                    try:
                        if len(temp)<pn:
                            print(111111111111,pn-len(temp))
                        else:
                            break
                    except:
                        self.connect()     
            try: 
                data += temp
            except:
                self.connect()
        df = self.api.to_df(data)[["open","close","high","low","datetime"]]
        df.set_index("datetime",inplace=True,drop=False)
        return df
    
    def set_main_rate(self,df_m,product="RBL8",f="MainContract.csv"):
        '''商品主月除权
        '''
        df = pd.read_csv(f,encoding="gb2312")
        df_p = df[df["ContractCode"]==product[:-2]]
        lstr = " 15:00"
        df_p = df_p.assign(datetime=df_p['EndDate'].apply(lambda x: str(x)[0:10]+lstr)) 
        df_p.set_index("datetime",inplace=True)
        df_p.fillna(0,inplace=True)
        df_m.loc[:,"date"] = df_m["datetime"].apply(lambda x: str(x)[0:10]) 
        
        filt = df_m.index.isin(df_p.index)
        df_m.loc[filt,"OpenPrice"] = df_p["OpenPrice"]
        df_m.loc[filt,"Term"] = df_p["Term"]
        
        df_m["OpenPrice"].fillna(method="bfill",inplace=True)
        df_m["Term"].fillna(method="bfill",inplace=True)
        df_m.dropna(inplace=True)
        
        filt = (df_m["OpenPrice"]!=df_m["open"])&\
                (df_m["date"]>df_m["date"].shift(1))&\
                (abs(1-df_m["open"]/df_m["OpenPrice"])>0.008)&\
                (df_m["OpenPrice"]>0)
                
        df_m.loc[filt,"change"] = 1
        df_m.loc[:,"adj"] = 1
        
        rst = df_m[df_m["change"]>0]
        rst = (rst["Term"]!=rst["Term"].shift(1))
        
        filt = df_m.index.isin(rst[rst>0].index )
        df_m.loc[filt,"adj"] = df_m["open"]/df_m["OpenPrice"]
        
        df_m.loc[:,'adj'] = df_m["adj"].shift(-1)
        df_m.loc[:,'adj'] = df_m["adj"][::-1].cumprod()
        
        print(df_m[(df_m["change"]>0)|(df_m["change"].shift(-1)>0)][["close","adj","open","OpenPrice","Term"]] )
        
        df_m.loc[:,'open'] = df_m['open'] * df_m['adj']
        df_m.loc[:,'high'] = df_m['high'] * df_m['adj']
        df_m.loc[:,'low'] = df_m['low'] * df_m['adj']
        df_m.loc[:,'close'] = df_m['close'] * df_m['adj']
        
        return df_m
    
    def getdata_stock(self,code="000001",number=30000,pn=500):
        
        market = self._select_market_code(code)
        data = []
        for i in range(int(number/pn)+1):
            data += self.api.get_security_bars(self.datatype, market, code, (int(number/pn)-i)*pn,pn)
            
        df = self.api.to_df(data)[["open","close","high","low","datetime"]]
        df.set_index("datetime",inplace=True,drop=False)
        return df
    
    def _select_market_code(self,code):
        code = str(code)
        if code[0] in ['5','6','9'] or code[:3] in ["009","126","110","201","202","203","204"]:
            return 1
        return 0
    
    def fetch_get_stock_xdxr(self,code):
        '除权除息'
        market_code = self._select_market_code(code)
        category = {
            '1': '除权除息', '2': '送配股上市', '3': '非流通股上市', '4': '未知股本变动', '5': '股本变化',
            '6': '增发新股', '7': '股份回购', '8': '增发新股上市', '9': '转配股上市', '10': '可转债上市',
            '11': '扩缩股', '12': '非流通股缩股', '13':  '送认购权证', '14': '送认沽权证'}
        data = self.api.to_df(self.api.get_xdxr_info(market_code, code))
        if len(data) >=1:
            data = data\
                .assign(date=pd.to_datetime(data[['year', 'month', 'day']]))\
                .drop(['year', 'month', 'day'], axis=1)\
                .assign(category_meaning=data['category'].apply(lambda x: category[str(x)]))\
                .assign(code=str(code))\
                .rename(index=str, columns={'panhouliutong': 'liquidity_after',
                                            'panqianliutong': 'liquidity_before', 'houzongguben': 'shares_after',
                                            'qianzongguben': 'shares_before'})\
                .set_index('date', drop=False, inplace=False)
            if self.datatype == 0 :
                lstr = " 09:35"
            elif self.datatype == 1 :
                lstr = " 09:45" 
            elif self.datatype == 2 :
                lstr = " 10:00"
            elif self.datatype == 3 :
                lstr = " 10:30"
            elif self.datatype == 4 :
                lstr = " 15:00"
            elif self.datatype == 7 :
                lstr = " 09:31"
            return data.assign(date=data['date'].apply(lambda x: str(x)[0:10]+lstr)) 
        else:
            return None
    
    def qfq(self,data,xdxr_data):
        '''data: 除权前数据
           info：除权信息 
        '''
        start = data.index[0]
        if xdxr_data is not None:
            info = xdxr_data[xdxr_data["category"] == 1 ]
            info.set_index("date",inplace=True)
            df = pd.concat([data, info[['fenhong', 'peigu', 'peigujia',
                                              'songzhuangu']]], axis=1).fillna(0)
            df['preclose'] = (df['close'].shift(1) * 10 - df['fenhong'] + df['peigu']
                                        * df['peigujia']) / (10 + df['peigu'] + df['songzhuangu'])
            df['adj'] = (df['preclose'].shift(-1) /
                           df['close']).fillna(1)[::-1].cumprod()
            
            df['open'] = df['open'] * df['adj']
            df['high'] = df['high'] * df['adj']
            df['low'] = df['low'] * df['adj']
            df['close'] = df['close'] * df['adj']
            df['preclose'] = df['preclose'] * df['adj']
        else:
            df['preclose'] = df['close'].shift(1)
            df['adj'] = 1
        return  df[start:]
    
    def macdhandle(self,df,p5=5,p15=15,p30=30,p60=60,p240=240,pweek=1200,macd_f=12,macd_s=26,macd_m=9):
        df.loc[:,"number"] = range(df.shape[0]) 
        for i in [p5,p15,p30,p60,p240,pweek]:
            pres = str(i)+"_"
            #计算各周期初始macd
            df.loc[::i,pres+"fEMA_mark"] = df["close"][::i].ewm(adjust=False,span=macd_f).mean()
            df.loc[::i,pres+"sEMA_mark"] = df["close"][::i].ewm(adjust=False,span=macd_s).mean()
            df.loc[::i,pres+"DIFF_mark"] = df[pres+"fEMA_mark"] - df[pres+"sEMA_mark"]
            df[pres+"fEMA_mark"].fillna(method="ffill",inplace=True)
            df[pres+"sEMA_mark"].fillna(method="ffill",inplace=True)
            df[pres+"DIFF_mark"].fillna(method="ffill",inplace=True)
            
            df[pres+"fEMA"] = (df[pres+"fEMA_mark"]*(macd_f-1)+df["close"]*2)/(macd_f+1)
            df[pres+"sEMA"] = (df[pres+"sEMA_mark"]*(macd_s-1)+df["close"]*2)/(macd_s+1)
            df.loc[::i,pres+"fEMA"] = df[pres+"fEMA_mark"]
            df.loc[::i,pres+"sEMA"] = df[pres+"sEMA_mark"]
            
            df.loc[:,pres+"DIFF"] = df[pres+"fEMA"] - df[pres+"sEMA"]
            
            df.loc[::i,pres+"DEA_mark"] = df[pres+"DIFF"][::i].ewm(adjust=False,span=macd_m).mean()
            df[pres+"DEA_mark"].fillna(method="ffill",inplace=True)
            
            df[pres+"DEA"] = (df[pres+"DEA_mark"]*(macd_m-1)+df[pres+"DIFF"]*2)/(macd_m+1)
            df.loc[::i,pres+"DEA"] = df[pres+"DEA_mark"]
            
            df.loc[:,pres+"MACD"] = 2*(df[pres+"DIFF"]-df[pres+"DEA"])
        
        df.loc[:,"DIF"] ,df.loc[:,"DEA"],df.loc[:,"MACD"] = talib.MACD(df.close.values)  
        df.dropna(inplace=True) #丢弃前面NA数据
        return df
    
    def getblockstock(self,block="沪深300"):
        '''股票版块对应股票列表
        '''
        df = self.api.to_df(self.api.get_and_parse_block_info("block.dat")) 
        stocks = list(df[df["blockname"]==block]["code"])
        return stocks
        
        
if __name__=="__main__":
    d = data(heartbeat=True)
    d.connect()
    market = []
    for i in range(100):
        market += d.api.get_instrument_info(i*500, 500)
    market = d.api.to_df(market)
     
    d.best_s,d.best_m,d.best_l = (7,37,110)
    d.datatype = 1 #0:5min数据 ,1:15min数据
    d.number = 10000
    select = True
#     d.merge(market)
     
    products = ["BUL9","CUL9","FUL9","HCL9","NIL9","PBL9","RBL9","RUL9",
                "SNL9","ZNL9","APL9","CFL9","FGL9","MAL9","OIL9","RML9","SFL9","SML9","SRL9",
                "JDL9","JML9","LL9","ML9","PL9","PPL9","VL9","YL9","AGL9","AL9","JL9"]   
    products = list(PERMONEY.keys())
    select_product = []
    if len(select_product) >= 1:
        select = False
        products = select_product
     
    rg=[(1,5,1),(1,30,2),(3,200,4)]     
    for p in products:
        mk = market[market["desc"]==p]["market"].values[0]
        print(p,mk)
        d.result = []
#         df = d.getdata(p,mk,number=d.number)
#         df = d.set_main_rate(df,product=p)
#         d.fanzhuan(df,)
#         d.macdhandle(df)
        d.run(p,mk,select,folder="result_new",rg=rg)
     

#rb: 7 35 125    
#j:6,11,50 
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    


