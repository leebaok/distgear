"""
DistGear
"""

import re
import ast 
from setuptools import setup

version_re = re.compile(r'__version__\s+=\s+(.*)')

with open('distgear/__init__.py', 'rb') as f:
    version = str(ast.literal_eval(version_re.search(
        f.read().decode('utf-8')).group(1)))

setup(
    name = 'DistGear',
    version = version,
    url = 'https://github.com/leebaok/distgear/',
    license = 'BSD',
    author = 'Bao Li',
    #author_email = '',
    description = 'some components to process distributed events', 
    packages = ['distgear'],
    include_package_data = True,
    install_requires = [
        'aiohttp>=1.2',
        'pyzmq>=15.0',
    ],
    classifiers = [
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: POSIX',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: System :: Distributed Computing',
        'Topic :: System :: Clustering',
    ],
        
)

