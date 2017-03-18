# DistGear

## Introduction
DistGear is an event processing component for distributed environment.
When you develop a distributed system, you need to control nodes and manage jobs. 
DistGear is to seperate the two work.
You just need to think and write your job working code and
leave messaging, heartbeat and etc to DistGear.

DistGear is the gears for distributed environment. You code the
event handler and DistGear drives your events.

## Structure

              Master                          Worker
         +--------------+               +-------------+
         |    Handler   |               |   Handler   |    
         +--------------+               +-------------+
         |             PUB ---+------> SUB            |
    --> HTTP   Loop     |     |         |    Loop     |    
         |            PULL <--|----+-- PUSH           |
         +--------------+     |    |    +-------------+
                              |    |
                              |    |     
                              |    |    +-------------+
                              |    |    |   Handler   |
                              |    |    +-------------+
                              +----|-> SUB            |
                                   |    |    Loop     |
                                   +-- PUSH           |
                                        +-------------+

Handler is defined by You. Others are DistGear's job.
