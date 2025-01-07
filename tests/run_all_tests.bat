@echo off

:: Navigate to the directory containing the batch file
cd /d "%~dp0"

:: Find and run all Python files recursively in subdirectories
for /r %%f in (*.py) do (
    echo Running %%f...
    python %%f
    echo.
)

:: Display a completion message
echo All tests executed.
pause
