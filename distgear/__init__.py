# -*- coding: utf-8 -*-
"""
    DistGear Module
    ~~~~~~~~~~~~~~~~~~~~~~~~~

         PrimaryMaster          SecondaryMaster             Worker
         +-----------+           +-----------+           +-----------+
         |   Event   |           |   Event   |           |  Action   |
         |  Handler  |           |  Handler  |           |  Handler  |
         +-----------+           +-----------+           +-----------+
         |          PUB ----+-> SUB         PUB ----+-> SUB          |
   --> HTTP   Loop   |      |    |    Loop   |      |    |    Loop   |
         |         PULL <-+-|-- PUSH       PULL <-+-|-- PUSH         |
         +-----------+    | |    +-----------+    | |    +-----------+
                          | |                     | |
                          | |        Worker       | |       Worker
                          | |    +-----------+    | |    +-----------+
                          | |    |  Action   |    | |    |  Action   |
                          | |    |  Handler  |    | |    |  Handler  |
                          | |    +-----------+    | |    +-----------+
                          | +-> SUB          |    | +-> SUB          |
                          |      |    Loop   |    |      |    Loop   |
                          +---- PUSH         |    +---- PUSH         |
                                 +-----------+           +-----------+

    Author: Bao Li
""" 

__version__ = '0.1-dev'

from .master import PrimaryMaster, SecondaryMaster
from .worker import Worker

__all__ = ( master.__all__ +
            worker.__all__ )

