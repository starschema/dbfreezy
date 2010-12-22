
rmdir /S DBFreezy-32 DBFreezy-64

rmdir /Q /S build dist

c:\Python27\python.exe setup.py py2exe
copy exampleConfig dist
ren dist DBFreezy-32

rmdir /Q /S build dist

c:\Python27-64\python.exe setup.py py2exe
copy exampleConfig dist
ren dist DBFreezy-64

rmdir /Q /S build dist
