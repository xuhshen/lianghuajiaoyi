from pymongo import MongoClient 
from config import DAY_STOCK_DB,LOCAL_SERVER_IP,LOCAL_MONGODB_PORT,MARKET_DB,XDXR_DB,logger

class MongoDB(object):
    def __init__(self,ip=LOCAL_SERVER_IP, 
                     port=LOCAL_MONGODB_PORT, 
                     user_name=None, 
                     pwd=None,
                     authdb=None):
        self.__server_ip = ip
        self.__server_port = port
        self.__user_name = user_name
        self.__pwd = pwd
        self.__authdb = authdb

        self.client = self.connect()
    
    @property    
    def info(self):
        info = "ip={}:{},user_name={},pwd={},authdb={}".format(self.__server_ip,self.__server_port,\
            self.__user_name,self.__pwd,self.__authdb)
        return info
            
    def connect(self):
        '''建立数据库的连接
        '''
        _db_session = MongoClient(self.__server_ip, self.__server_port)
        if  self.__user_name:        
            eval("_db_session.{}".format(self.__authdb)).authenticate(self.__user_name,self.__pwd)      
        
        logger.info("connected to {}:{}".format(self.__server_ip,self.__server_port))
        return _db_session

    def disconnect(self):
        '''断开数据库连接        
        '''
        self._db_session.close()
        return True
    
    def _dbclient(self,db):
        '''返回某个特定数据库的对象
        '''
        return eval("self.client.{}".format(db))
    
    def ensure_index(self,db,idx="date",unique=True):
        for collection in self.getallcollections(db):
            logger.info("set index {}  for {} ".format(idx,collection))
            try:
                self._dbclient(db)[collection].ensure_index(idx, unique=unique)
            except:
                logger.warning("set index {}  for {} failed! Please droping the collections ".format(idx,collection))
#                 self.dropcollection(collection,db)
    
    def get_last_record(self,db,collection,sortkey=[("date",-1),]):
        rst = [i for i in self._dbclient(db)[collection].find().sort(sortkey).limit(1)]
        return rst
    
    def dropcollection(self,collection,db=DAY_STOCK_DB):
        '''删除某个数据库的一个collection
        '''
        logger.info("warning: %s is droped" %collection)
        return self._dbclient(db)[collection].drop()

    def getallcollections(self,db=DAY_STOCK_DB):  
        '''获取某个db的所有collection
        ''' 
        cls = [i for i in self._dbclient(db).collection_names()]
        return cls    
    
    def saveblockinfo(self,collection,df,fields,db=MARKET_DB):
        '''保存市场强弱指标
        '''
        bulk = self._dbclient(db)[collection].initialize_ordered_bulk_op()
        data = df[fields].values.tolist()
        for item in data:
            d = {k:v for k,v in zip(fields,item)}
            bulk.find({"date":d["date"]}).upsert().update({"$set":d})
        if len(data)>0: bulk.execute()
        
    def savexdxrinfo(self,collection,df,fields,db=XDXR_DB):
        '''保存更新除权除息信息
        '''
        bulk = self._dbclient(db)[collection].initialize_ordered_bulk_op()
        data = df[fields].values.tolist()
        for item in data:
            d = {k:v for k,v in zip(fields,item) if v != 0}
            bulk.find({"date":d["date"]}).upsert().update({"$set":d})
        if len(data)>0: bulk.execute()
    
    def loadblockinfo(self,collection,db=MARKET_DB,project={"date":1,"MA60":1,"MA10":1,"MA20":1,"rate":1,"append":1,"up":1,"_id":0}):
        '''获取市场强弱信息
        '''
        rst = [i for i in self._dbclient(db)[collection].find({},project)]
        return rst
    
    def loadxdxrinfo(self,collection,db=XDXR_DB,filt={},project={"date":1,'fenhong':1, 'peigu':1, 'peigujia':1,
                                          'songzhuangu':1,"_id":0},sort=[("date",-1)],limit=100):
        '''获取市场除权除息信息
        '''
        rst = [i for i in self._dbclient(db)[collection].find(filt,project,sort=sort).limit(limit)]
        return rst    
        
if __name__ == '__main__':
    m = MongoDB()


















