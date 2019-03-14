import logging
import os

def getlogger():
    
    basedir = os.path.dirname(os.path.realpath(__file__))
    path = basedir+"/../log"
    logfile = os.path.join(path,"sys.log")
    
    if not os.path.exists(path):
        os.makedirs(path)
    
    logger = logging.getLogger('deeplqt')
    logger.setLevel(logging.DEBUG)
    
    fh = logging.FileHandler(logfile)
    fh.setLevel(logging.DEBUG)
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)
    
    
    logger.addHandler(fh)
    logger.addHandler(ch)
    # logger.debug('Debug')
    # logger.info('Info')
    return logger

