@echo off
REM Define source database credentials
SET "SOURCE_SQL_HOSTNAME=10.30.0.4"
SET "SOURCE_SQL_PORT=1433"
SET "SOURCE_SQL_DATABASE=SINGESTAO"
SET "SOURCE_SQL_USERNAME=lucas.ponzo"
SET "SOURCE_SQL_PASSWORD=Mx@2024"

REM Define target database credentials
SET "TARGET_SQL_HOSTNAME=mlsrvresultsmxnc.database.windows.net"
SET "TARGET_SQL_PORT=1433"
SET "TARGET_SQL_DATABASE=mldbresultsmxnc"
SET "TARGET_SQL_USERNAME=mladmin"
SET "TARGET_SQL_PASSWORD=MlSolution-3099"

REM Directory for storing CSV files
SET "EXPORT_DIR=%cd%\bcp_exports"
IF NOT EXIST "%EXPORT_DIR%" (
    mkdir "%EXPORT_DIR%"
)

REM Define tables to transfer
SET "TABLES=transacao bandeira_produto cartao cliente cliente_interno contrato_cliente contrato_credenciado contrato_credenciado_servico credenciado detalhe_item_pedido_carga item_pedido_carga operacao_adquirencia operacao_adquirencia_produto operacao_emissao operacao_emissao_produto operador pedido_carga portador produto recurso_cliente rota"

REM Date range for filtering data (last 7 days)
FOR /F %%A IN ('powershell -command "Get-Date (Get-Date).AddDays(-7) -Format yyyy-MM-dd"') DO SET "START_DATE=%%A"
FOR /F %%A IN ('powershell -command "Get-Date -Format yyyy-MM-dd"') DO SET "END_DATE=%%A"

REM Loop through each table
FOR %%T IN (%TABLES%) DO (
    echo Processing table: %%T

    REM Export data from the source database
    bcp "SELECT * FROM [dbo].[%%T] WHERE data_hora_transacao_inicio BETWEEN '%START_DATE%' AND '%END_DATE%'" queryout "%EXPORT_DIR%\%%T.csv" -c -t, -S "%SOURCE_SQL_HOSTNAME%,%SOURCE_SQL_PORT%" -d "%SOURCE_SQL_DATABASE%" -U "%SOURCE_SQL_USERNAME%" -P "%SOURCE_SQL_PASSWORD%"

    REM Check if the file was created successfully
    IF NOT EXIST "%EXPORT_DIR%\%%T.csv" (
        echo Failed to export data for table: %%T
        GOTO :NEXT
    )

    REM Import data into the target database
    bcp "[dbo].[%%T]" in "%EXPORT_DIR%\%%T.csv" -c -t, -S "%TARGET_SQL_HOSTNAME%,%TARGET_SQL_PORT%" -d "%TARGET_SQL_DATABASE%" -U "%TARGET_SQL_USERNAME%" -P "%TARGET_SQL_PASSWORD%"

    REM Check if import was successful
    IF %ERRORLEVEL% EQU 0 (
        echo Successfully transferred data for table: %%T
    ) ELSE (
        echo Failed to transfer data for table: %%T
    )

    :NEXT
)

REM Cleanup exported CSV files
rmdir /s /q "%EXPORT_DIR%"

echo Data transfer completed.
pause
