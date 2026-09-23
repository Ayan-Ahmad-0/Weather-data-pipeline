$Root = Split-Path -Parent $PSScriptRoot
$Python = Join-Path $Root ".venv\Scripts\python.exe"
$Dbt = Join-Path $Root ".venv\Scripts\dbt.exe"
$DbtProject = Join-Path $Root "dbt_project"

if (-not (Test-Path $Python)) {
    throw "Project Python executable not found: $Python"
}

if (-not (Test-Path $Dbt)) {
    throw "dbt executable not found: $Dbt"
}

Push-Location $Root
try {
    Write-Host "[1/3] Extracting + loading current weather..."
    & $Python -m src.load.load_to_postgres
    if ($LASTEXITCODE -ne 0) { throw "Weather loading failed with exit code $LASTEXITCODE" }

    Write-Host "[2/3] Running dbt models..."
    & $Dbt run --project-dir $DbtProject --profiles-dir $DbtProject 2>&1
    if ($LASTEXITCODE -ne 0) { throw "dbt run failed with exit code $LASTEXITCODE" }

    Write-Host "[3/3] Running dbt tests..."
    & $Dbt test --project-dir $DbtProject --profiles-dir $DbtProject 2>&1
    if ($LASTEXITCODE -ne 0) { throw "dbt test failed with exit code $LASTEXITCODE" }

    Write-Host "Done."
}
finally {
    Pop-Location
}
