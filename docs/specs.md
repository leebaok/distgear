# some specifications for distgear users

## register event handler and action handler
* Master -- event handler :
    * master.handleEvent('event') : register handler
    * handler(event, master) : handler format, return json: {'status':'fail/success', 'result':result}
* Worker -- action handler
    * worker.doAction('action'), worker.undoAction('action')
    * handler(paras) : return json: {'status':'fail/success', 'result':result}
* request flow :


    request:{'event':event, 'parameters':paras}
        | Master._http_handler
    Event(event, paras)
        | EventHandler
    {'status':'fail/success', 'result':result}

* action flow :


    action : {'action':action, 'parameters':paras, 'actionid':id}
        | Worker._run_action
    Action(paras)
        |
    {'status':'fail/success', 'result':result}

* Event commands -- ToDo
