@echo off
REM Quick start toxiproxy on Windows host for local development
REM Run this script before running kp-chaos-runner

echo Starting Toxiproxy on host...

REM Check if toxiproxy is available
where toxiproxy-server >nul 2>&1
if %errorlevel% neq 0 (
    echo toxiproxy-server not found. Install it:
    echo   scoop install toxiproxy
    echo   OR download from https://github.com/Shopify/toxiproxy/releases
    pause
    exit /b 1
)

echo Starting toxiproxy-server on 0.0.0.0:8474...
start /B toxiproxy-server -host 0.0.0.0

REM Wait for it to start
timeout /t 2 /nobreak >nul

echo Creating proxies...
toxiproxy-cli create kafka-primary -l 0.0.0.0:8666 -u localhost:9092
toxiproxy-cli create localstack -l 0.0.0.0:8668 -u localhost:4566
toxiproxy-cli create minio -l 0.0.0.0:8669 -u localhost:9000

echo.
echo Toxiproxy running on localhost:8474
echo Proxies created:
echo   - kafka-primary: localhost:8666 -> localhost:9092
echo   - localstack: localhost:8668 -> localhost:4566
echo   - minio: localhost:8669 -> localhost:9000
echo.
echo Press Ctrl+C to stop
pause