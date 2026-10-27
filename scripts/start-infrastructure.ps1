$ErrorActionPreference = 'Stop'

$dockerCommand = Get-Command docker -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
$dockerCliPath = if ($dockerCommand) { $dockerCommand.Source } else { $null }
if (-not $dockerCliPath) {
    $dockerCandidates = @(
        (Join-Path $env:LOCALAPPDATA 'Programs\DockerDesktop\resources\bin\docker.exe'),
        (Join-Path $env:LOCALAPPDATA 'Docker\resources\bin\docker.exe'),
        (Join-Path $env:ProgramFiles 'Docker\Docker\resources\bin\docker.exe')
    )
    $dockerCliPath = $dockerCandidates | Where-Object { Test-Path -LiteralPath $_ -PathType Leaf } | Select-Object -First 1
}
if (-not $dockerCliPath) {
    throw 'Docker CLI bulunamadi. Docker Desktop kurun ve terminali yeniden acin.'
}
# Docker credential helpers are installed next to the CLI in per-user installs.
$env:PATH = (Split-Path -Parent $dockerCliPath) + ';' + $env:PATH

function Invoke-Compose {
    param([string[]]$ComposeArguments)
    & $dockerCliPath compose @ComposeArguments
    if ($LASTEXITCODE -ne 0) {
        throw "Docker Compose basarisiz (exit code: $LASTEXITCODE). Docker Desktop ve container loglarini kontrol edin."
    }
}

Push-Location (Join-Path $PSScriptRoot '..')
try {
    & $dockerCliPath info --format '{{.ServerVersion}}'
    if ($LASTEXITCODE -ne 0) {
        throw 'Docker Engine calismiyor. Docker Desktop uygulamasini Linux containers modunda baslatin.'
    }

    Invoke-Compose -ComposeArguments @('up', '-d', '--wait', 'postgres', 'kafka')
    Invoke-Compose -ComposeArguments @('up', '-d', 'kafka-init')
    Invoke-Compose -ComposeArguments @('wait', 'kafka-init')
    Invoke-Compose -ComposeArguments @('up', '-d', 'kafka-ui')

    $uiPort = if ($env:KAFKA_UI_PORT) { $env:KAFKA_UI_PORT } else { '8080' }
    Write-Host "PostgreSQL, Kafka ve topic'ler hazir. Kafka UI: http://localhost:$uiPort"
    Write-Host "API'leri Rider'da http launch profiliyle veya dotnet run --launch-profile http ile baslatin."
}
finally {
    Pop-Location
}
