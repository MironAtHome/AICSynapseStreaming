echo "started batchbuild"
cd "%~dp0"
@echo ~dp0= %~dp0
SET EX=0

cd ..\..

SET JDK_VERSION=zulu8.46.0.19-ca-jdk8.0.252-win_x64
SET JAVA_ROOT=%cd%\Java
echo "Installing Java"
powershell.exe -noprofile -nologo -noninteractive %~dp0\install-java.ps1 -JdkVersion %JDK_VERSION% -JavaRoot %JAVA_ROOT%

SET JAVA_HOME=%JAVA_ROOT%\%JDK_VERSION%
SET PATH=%JAVA_HOME%\bin;%PATH%

REM Ensure SBT is present 

rem set PATH 
powershell  "(New-Object System.Net.WebClient).DownloadFile('https://github.com/sbt/sbt/releases/download/v1.3.10/sbt-1.3.10.zip', 'sbt.zip')"
powershell "Expand-Archive -Path .\sbt.zip -Force"

set SBT_OPTS=-Xms1024M -Xmx4096M -XX:MaxPermSize=4096M -XX:+CMSClassUnloadingEnabled

rem call "%~dp0sparkbuild.cmd"

cd "%~dp0"

rmdir /S /Q target
call ..\..\sbt\sbt\bin\sbt.bat clean package

SET /a EX="%EX%+%ErrorLevel%"



if "%EX%" neq "0" (
echo "One or more sbt projects failed to compile."
) ELSE (
echo "sbt projects compiled successfully"
)

exit /B %EX%