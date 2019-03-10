@echo off
:: Set python file name
set pfn=MangoCalls.py
:: Additional args
set arg=

set script_path=%~dp0

call %script_path%..\set_tasks_env.bat


echo ------------------------------------------------------------------------------------------------------ %date% %time% >>%log_path%\%pfn%.log
cd %log_py_path%
python.exe %script_path%%pfn% --cfg=%script_path%..\OLAP.cfg %arg% >> %log_path%\%pfn%.log 2>&1
if ERRORLEVEL 1 (

 echo External error. Error description look in %log_path_web%%pfn%.log
 
 exit /b 1

)
