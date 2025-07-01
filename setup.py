#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import re

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

# ensure supported Python version
if sys.version_info < (3, 7):
    raise EnvironmentError("BACpypes requires Python 3.7 or newer")

# single source directory
source_folder = 'py34'
if not os.path.exists(source_folder):
    raise EnvironmentError(
        "broken distribution, looking for " + repr(source_folder) + " in " + os.getcwd()
    )

# load in the project metadata
init_py = open(os.path.join(source_folder, 'bacpypes', '__init__.py')).read()
metadata = dict(re.findall("__([a-z]+)__ = '([^']+)'", init_py))

requirements = [
    # no external requirements
]

setup_requirements = [
    'pytest-runner',
    ]

test_requirements = [
    'pytest',
    'bacpypes',
]

setup(
    name="bacpypes",
    version=metadata['version'],
    description="BACnet Communications Library",
    long_description="BACpypes provides a BACnet application layer and network layer written in Python for daemons, scripting, and graphical interfaces.",
    author=metadata['author'],
    author_email=metadata['email'],
    url="https://github.com/JoelBender/bacpypes",
    packages=[
        'bacpypes',
        'bacpypes.local',
        'bacpypes.service',
    ],
    package_dir={
        '': os.path.join(source_folder, ''),
    },
    include_package_data=True,
    install_requires=requirements,
    license="MIT",
    zip_safe=False,
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
    python_requires='>=3.7',

    setup_requires=setup_requirements,

    test_suite='tests',
    tests_require=test_requirements,
)
