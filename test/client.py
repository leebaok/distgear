# -*- coding: utf-8 -*-

import requests
import json

print('----- Event : myevent -----')
r = requests.post('http://localhost:8000/', data = json.dumps({ 
                            'event':'myevent', 
                            'parameters':{  
                                            'para1':'one',
                                            'para2':'two'
                                            } 
                            }))
print(r.text)

print('----- Event : invalid -----')
r = requests.post('http://localhost:8000/', data = json.dumps({ 
                            'event':'invalid', 
                            'parameters':{  
                                            'para1':'one',
                                            'para2':'two'
                                            } 
                            }))
print(r.text)
