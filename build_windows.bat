@echo off
REM Windows build script - uses cmake directly (no make on Windows).
REM Replicates extension-ci-tools Makefile BUILD_FLAGS for `make release`.
REM Usage: build_windows.bat

call "C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvarsall.bat" x64
if errorlevel 1 (
    echo FAILED to set MSVC environment
    exit /b 1
)

set PATH=%USERPROFILE%\.cargo\bin;%PATH%;C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin;C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\Common7\IDE\CommonExtensions\Microsoft\CMake\Ninja

set PROJ_DIR=%~dp0
set CMAKE_EXE="C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin\cmake.exe"

echo.
echo ================================================
echo  nats_rw Windows Build
echo ================================================
rustc --version
cargo --version
%CMAKE_EXE% --version | findstr /C:"cmake version"
ninja --version
echo ================================================
echo.

REM Configure (flags mirror extension-ci-tools duckdb_extension.Makefile BUILD_FLAGS)
%CMAKE_EXE% -G Ninja ^
  -DCMAKE_BUILD_TYPE=Release ^
  -DDUCKDB_EXTENSION_CONFIGS="%PROJ_DIR%extension_config.cmake" ^
  -DUNITTEST_ROOT_DIRECTORY="%PROJ_DIR:~0,-1%" ^
  -DENABLE_UNITTEST_CPP_TESTS=FALSE ^
  -DENABLE_EXTENSION_AUTOLOADING=1 ^
  -DENABLE_EXTENSION_AUTOINSTALL=1 ^
  -DOVERRIDE_GIT_DESCRIBE=v0.0.0 ^
  -S "%PROJ_DIR%duckdb" -B "%PROJ_DIR%build\release"
if errorlevel 1 (
    echo CONFIGURE FAILED
    exit /b 1
)

REM Build extension + unittest
%CMAKE_EXE% --build "%PROJ_DIR%build\release" --config Release --target nats_rw_loadable_extension
if errorlevel 1 (
    echo BUILD FAILED
    exit /b 1
)
%CMAKE_EXE% --build "%PROJ_DIR%build\release" --config Release --target unittest
if errorlevel 1 (
    echo BUILD FAILED
    exit /b 1
)

echo.
echo ================================================
echo  BUILD PASSED
echo ================================================
dir "%PROJ_DIR%build\release\extension\nats_rw\nats_rw.duckdb_extension"
