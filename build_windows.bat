@echo off
REM Windows 构建脚本 - 通过 vcvarsall 设置 MSVC 环境后运行 make
REM 用法: build_windows.bat

REM 设置 MSVC 环境
call "C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvarsall.bat" x64
if errorlevel 1 (
    echo FAILED to set MSVC environment
    exit /b 1
)

REM 设置 Rust 路径
set PATH=%USERPROFILE%\.cargo\bin;%PATH%

REM 显示环境
echo.
echo ================================================
echo  nats_rw Windows Build
echo ================================================
echo  cl:     OK
echo  cmake:  %CMAKE_VERSION%
echo  rustc:  
rustc --version
echo  cargo:  
cargo --version
echo  ninja:  
ninja --version 2>nul
echo ================================================
echo.

REM 清理旧构建
if exist build\release rmdir /s /q build\release

REM 构建
set PATH=%PATH%;C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin;C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\Common7\IDE\CommonExtensions\Microsoft\CMake\Ninja
make release 2>&1

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ================================================
    echo  BUILD PASSED
    echo ================================================
    dir build\release\extension\nats_rw\nats_rw.duckdb_extension 2>nul
    echo.
    echo Running test...
    build\release\test\unittest.exe test\sql\nats_rw.test
) else (
    echo.
    echo ================================================
    echo  BUILD FAILED with exit code %ERRORLEVEL%
    echo ================================================
)
