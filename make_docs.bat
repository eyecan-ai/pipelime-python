del /s /f /q docs\_static\generated\*.*
rem sphinx-apidoc -o docs\api\generated pipelime
python docs\pl_help.py
call docs\make.bat clean
call docs\make.bat html
if exist docs\_build\html\index.html start docs\_build\html\index.html
