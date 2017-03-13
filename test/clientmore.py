# -*- coding: utf-8 -*-

import requests
import json
import threading

def myevent():
    r = requests.post('http://localhost:8000/', data = json.dumps({ 
                            'event':'myevent', 
                            'parameters':{  
                                            'para1':'one',
                                            'para2':'two'
                                            } 
                            }))
    print(r.text)

def test():
    print('test')

threads = []
for i in range(100):
    thread = threading.Thread(target = myevent, args = ())
    thread.setDaemon(True)
    thread.start()
    threads.append(thread)

for thread in threads:
    thread.join()
