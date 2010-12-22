from distutils.core import setup

import py2exe

setup(
    console=['DBFreezy.py'],

    options={ "py2exe": { "includes": "decimal, datetime" } },
)
