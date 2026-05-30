$connectUrl = "http://localhost:8083"
$dir = "$PSScriptRoot\connectors"

$connectorFiles = @(
    @("debezium-postgres-source", "debezium-postgres-source.json")
    @("elasticsearch-sink",       "elasticsearch-sink.json")
    @("redis-sink",               "redis-sink.json")
    @("neo4j-sink",               "neo4j-sink.json")
    @("mongodb-sink-flat",        "mongodb-sink-flat.json")
    @("mongodb-sink-hierarchy",   "mongodb-sink-hierarchy.json")
)

Write-Host "Registering Kafka Connect connectors..."
Write-Host ""

$i = 1
foreach ($pair in $connectorFiles) {
    $name = $pair[0]
    $file = $pair[1]
    $path = Join-Path $dir $file
    $tmpFile = Join-Path $env:TEMP "kc-register-$name.json"

    Write-Host "[$i/6] $name..."

    $configRaw = [System.IO.File]::ReadAllText($path)
    $body = '{"name":"' + $name + '","config":' + $configRaw + '}'
    [System.IO.File]::WriteAllText($tmpFile, $body, [System.Text.UTF8Encoding]::new($false))

    $result = curl.exe -s -X POST "$connectUrl/connectors" -H "Content-Type: application/json" -d "@$tmpFile"
    try {
        $parsed = $result | ConvertFrom-Json
        if ($parsed.name) { Write-Host "  OK" }
        else { Write-Host "  ERROR: $($parsed.message)" }
    } catch {
        Write-Host "  RAW: $($result.Substring(0, [Math]::Min(200, $result.Length)))"
    }
    Remove-Item $tmpFile -ErrorAction SilentlyContinue
    $i++
}

Write-Host ""
Write-Host "Waiting 30 seconds for connectors to start..."
Start-Sleep 30

Write-Host ""
Write-Host "Connector status:"
foreach ($pair in $connectorFiles) {
    $name = $pair[0]
    $status = curl.exe -s "$connectUrl/connectors/$name/status" | python -c "import sys,json; d=json.load(sys.stdin); print(d.get('connector',{}).get('state','UNKNOWN') if 'connector' in d else 'FAILED')"
    Write-Host "  ${name}: $status"
}

Write-Host ""
Write-Host "Done!"