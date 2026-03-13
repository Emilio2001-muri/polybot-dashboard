@echo off
echo ============================================
echo  PolyBot Data Pusher - Supabase
echo ============================================
echo.
cd /d "%~dp0\.."
call venv\Scripts\activate.bat
cd vercel\local
python pusher.py
pause
