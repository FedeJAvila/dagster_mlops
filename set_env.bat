@echo off

set ENV_FILE=environments/local

if exist %ENV_FILE% (
    for /f "tokens=1,* delims==" %%a in (%ENV_FILE%) do (
        set %%a=%%b
    )
    echo Environment variables set from %ENV_FILE%
) else (
    echo %ENV_FILE% not found.
)
