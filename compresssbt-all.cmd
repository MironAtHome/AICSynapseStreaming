cd "%~dp0"
@echo ~dp0= %~dp0

powershell Compress-Archive -path "AICSynapseStreaming\target\release" -DestinationPath "AICSynapseStreaming\target\release\release.zip" -Force

rem In case Java sample compiled successfully, exit with 0 exit code so that build does not fail.
SET /a EX=0
exit /B %EX%