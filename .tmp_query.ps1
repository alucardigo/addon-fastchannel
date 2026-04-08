try {
    [System.Reflection.Assembly]::LoadWithPartialName('System.Data') | Out-Null
    $conn = New-Object System.Data.OleDb.OleDbConnection
    $conn.ConnectionString = 'Provider=SQLOLEDB;Data Source=172.16.127.11;Initial Catalog=SANKHYA_PROD;User ID=sup;Password=Azsxdc;'
    $conn.Open()

    Write-Output "=== QUERY 1: TGFTAB AD_* columns ==="
    $cmd = $conn.CreateCommand()
    $cmd.CommandText = "SELECT TOP 5 COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'TGFTAB' AND COLUMN_NAME LIKE 'AD[_]%' ORDER BY COLUMN_NAME"
    $reader = $cmd.ExecuteReader()
    while ($reader.Read()) {
        $col = $reader['COLUMN_NAME'].ToString()
        $dt = $reader['DATA_TYPE'].ToString()
        $len = if ($reader['CHARACTER_MAXIMUM_LENGTH'] -eq [DBNull]::Value) { 'NULL' } else { $reader['CHARACTER_MAXIMUM_LENGTH'].ToString() }
        Write-Output "$col | $dt | $len"
    }
    $reader.Close()

    Write-Output ""
    Write-Output "=== QUERY 2: AD_ESCFASTSYNC table ==="
    $cmd2 = $conn.CreateCommand()
    $cmd2.CommandText = "SELECT TOP 1 TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'AD_ESCFASTSYNC'"
    $reader2 = $cmd2.ExecuteReader()
    $found = $false
    while ($reader2.Read()) {
        Write-Output $reader2['TABLE_NAME'].ToString()
        $found = $true
    }
    if (-not $found) {
        Write-Output "(no rows - table does not exist)"
    }
    $reader2.Close()

    $conn.Close()
} catch {
    Write-Output ("Error: " + $_.Exception.Message)
}
