# -*- coding: utf-8 -*-

import logging
import logging.handlers
import sys

def createLogger(name='distgear', logpath='.', debug=False, std_to_log=False):
    logger = logging.getLogger(name)
    loglevel = logging.DEBUG if debug else logging.INFO
    logger.setLevel(loglevel)
    path = logpath
    logfile = path+'/'+name+'.log'
    handler = logging.handlers.TimedRotatingFileHandler(logfile, when="midnight", backupCount=10, encoding='utf-8')
    #handler = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter("%(asctime)s %(levelname)-8s %(module)s %(funcName)s [%(lineno)d] %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    if std_to_log:
        sys.stdout = RedirectLogger(logger, logging.INFO)
        sys.stderr = RedirectLogger(logger, logging.ERROR)
    return logger

class RedirectLogger(object):
    def __init__(self, logger, level):
        self.logger = logger
        self.level = level
    
    def write(self, message):
        #message = str(message)
        if message.rstrip() != "":
            self.logger.log(self.level, message.rstrip())

    def flush(self):
        for handler in self.logger.handlers:
            handler.flush()

