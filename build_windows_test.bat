@echo off
REM Build test infra + run nats_rw offline tests on Windows.
REM Requires build_windows.bat to have completed (configure + extension build).
REM Acceptance #5 of windows-build-simulation-plan.md

call "C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvarsall.bat" x64
if errorlevel 1 (
    echo FAILED to set MSVC environment
    exit /b 1
)

set PATH=%USERPROFILE%\.cargo\bin;%PATH%;C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin;C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\Common7\IDE\CommonExtensions\Microsoft\CMake\Ninja

REM Local extension repository - required so 'require nats_rw' can locate the
REM built extension binary (build/release/repository/<ver>/<platform>/)
ninja -C build\release duckdb_local_extension_repo
if errorlevel 1 (
    echo REPOSITORY TARGET FAILED
    exit /b 1
)

ninja -C build\release unittest
if errorlevel 1 (
    echo BUILD FAILED
    exit /b 1
)

echo.
echo ================================================
echo  Running nats_rw tests (forward-slash filters!)
echo ================================================
set LOG=.runtime\windows-test-run.log
if not exist .runtime mkdir .runtime

build\release\test\unittest.exe test/sql/nats_rw.test > %LOG% 2>&1
if errorlevel 1 (
    echo TEST nats_rw.test FAILED - see %LOG%
    type %LOG%
    exit /b 1
)
findstr /C:"No tests ran" /C:"All tests were skipped" %LOG% >nul
if not errorlevel 1 (
    echo TEST nats_rw.test DID NOT ACTUALLY RUN ^(skipped/no-match^) - see %LOG%
    type %LOG%
    exit /b 1
)
type %LOG%

build\release\test\unittest.exe test/sql/nats_rw_schema.test > %LOG% 2>&1
if errorlevel 1 (
    echo TEST nats_rw_schema.test FAILED - see %LOG%
    type %LOG%
    exit /b 1
)
findstr /C:"No tests ran" /C:"All tests were skipped" %LOG% >nul
if not errorlevel 1 (
    echo TEST nats_rw_schema.test DID NOT ACTUALLY RUN ^(skipped/no-match^) - see %LOG%
    type %LOG%
    exit /b 1
)
type %LOG%

echo.
echo ================================================
echo  ALL TESTS RAN AND PASSED
echo ================================================
