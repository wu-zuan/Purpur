@echo off
setlocal enabledelayedexpansion

echo ===================================================
echo   Purpur Project Build and Cleanup Tool
echo ===================================================

:: 1. Stop Gradle Daemon
echo [*] Stopping Gradle Daemon to release file locks...
call .\gradlew.bat --stop
timeout /t 3 /nobreak > nul

:: 2. Clean rejected patches
if exist "purpur-server\paper-patches\rejected" (
    echo [*] Clearing rejected patches...
    rd /s /q "purpur-server\paper-patches\rejected"
)

:: 3. Clean caches
echo [*] Cleaning caches...
call .\gradlew.bat cleanCache clean
if %errorlevel% neq 0 (
    echo [!] Gradle clean failed. Attempting to force-kill java process...
    powershell -Command "Stop-Process -Name java -Force -ErrorAction SilentlyContinue"
    timeout /t 2 /nobreak > nul
    
    call .\gradlew.bat cleanCache clean
    if %errorlevel% neq 0 (
        echo [x] Error: Could not release file locks. Please close your IDE and try again!
        pause
        exit /b 1
    )
)

:: 4. Apply Patches
echo [*] Applying patches...
call .\gradlew.bat applyAllPatches
if %errorlevel% neq 0 (
    echo [x] Error: applyAllPatches failed!
    pause
    exit /b 1
)

:: 5. Create executable Server Jar
echo [*] Compiling executable Server Jar...
call .\gradlew.bat createMojmapBundlerJar
if %errorlevel% neq 0 (
    echo [x] Error: Build failed!
    pause
    exit /b 1
)

echo ===================================================
echo [v] Success! Compiled Server Jar is located in:
echo     purpur-server\build\libs\
echo ===================================================
pause

