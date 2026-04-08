@echo off
set "JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-8.0.462.8-hotspot"
cd /d X:\IntegracaoFastchannel\APPFASTCHANNEL\.worktrees\merge-unify
call gradlew.bat clean publishAddon -Pemail=suporteti@bellube.com.br -Ppassword=102030 -Ppublish=true
echo EXIT_CODE=%ERRORLEVEL%
