$ErrorActionPreference = 'Stop'

$url = 'http://localhost:8080/addon-fastchannel/fc-direct?serviceName=FCConfigSP.get&outputType=json'
$resp = Invoke-WebRequest -Uri $url -Method Post -UseBasicParsing -TimeoutSec 30 -Body '{}' -ContentType 'application/json'
if ($resp.StatusCode -ne 200) { throw "HTTP $($resp.StatusCode)" }

$data = $resp.Content | ConvertFrom-Json
if (-not ($data.PSObject.Properties.Name -contains 'syncStatusEnabled')) {
    throw 'Missing syncStatusEnabled in FCConfigSP.get response'
}

Write-Host 'OK: syncStatusEnabled present in FCConfigSP.get response'
