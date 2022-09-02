call docs\make.bat clean
call docs\make.bat html
if exist docs\_build\html\index.html start docs\_build\html\index.html
