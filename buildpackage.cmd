echo "started modern_build"
cd "%~dp0"
SET EX=0

echo "Configuring Java to be in machine path variable"
SET JDK_VERSION=zulu8.46.0.19-ca-jdk8.0.252-win_x64
SET JAVA_ROOT=%cd%\..\..\Java
SET JAVA_HOME=%JAVA_ROOT%\%JDK_VERSION%
SET PATH=%JAVA_HOME%\bin;%PATH%

set SBT_OPTS=-Xms1024M -Xmx4096M -XX:MaxPermSize=4096M -XX:+CMSClassUnloadingEnabled

echo "Packaging Dependencies"
call ..\..\sbt\sbt\bin\sbt.bat pack
SET /a EX="%EX%+%ErrorLevel%"

echo "Building Release jar"
call ..\..\sbt\sbt\bin\sbt.bat buildRelease
SET /a EX="%EX%+%ErrorLevel%"

if "%EX%" neq "0" (
echo "One or more sbt projects failed to compile."
) ELSE (
echo "sbt projects compiled successfully"
)

exit /B %EX%