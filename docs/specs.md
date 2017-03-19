# some specifications for distgear users

## register event handler and action handler
* Master -- event handler :
    * master.handleEvent('event') : register handler
    * handler(event, master) : handler format, return json: {'status':'fail/success', 'result':result}
* Worker -- action handler
    * worker.doAction('action'), worker.undoAction('action')
    * handler(paras) : return json: {'status':'fail/success', 'result':result}

## request and action format
* request flow :
```
request:{'event':event, 'parameters':paras}
        | Master._http_handler
Event(event, paras)
        | EventHandler
{'status':'fail/success', 'result':result}
```
* action flow :
```
action : {'action':action, 'parameters':paras, 'actionid':id}
        | Worker._run_action
Action(paras)
        |
{'status':'fail/success', 'result':result}
```

## Event commands
```
commands = {
			'a':('node-1', 'act-1', 'para-1', []),
			'b':('node-2', 'act-2', 'para-2', []),
			'c':('node-3', 'act-3', 'para-3', ['a', 'b']),
			'd':('node-4', 'act-4', 'para-4', ['c']),
			'e':('node-5', 'act-5', 'para-5', []),
			'f':('node-6', 'act-6', 'para-6', ['d', 'e']),
		}
```
