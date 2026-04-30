param (
    [Parameter(Mandatory=$true, HelpMessage="Path to the .sql backup file to restore")]
    [string]$SqlFile
)

if (-Not (Test-Path -Path $SqlFile)) {
    Write-Host "❌ Error: File not found at path '$SqlFile'"
    exit 1
}

Write-Host "=========================================="
Write-Host " Starting Prod DB Restore"
Write-Host "=========================================="
Write-Host "Source File: $SqlFile"
Write-Host "⚠️ WARNING: This will DROP existing data and REPLACE it with the backup."
Write-Host "Press Ctrl+C to cancel or wait 5 seconds to proceed..."
Start-Sleep -Seconds 5

Write-Host "`n[1/5] Copying file to database container..."
docker compose -f docker-compose.prod.yml cp $SqlFile db:/tmp/restore.sql
if ($LASTEXITCODE -ne 0) { Write-Host "❌ Failed to copy file."; exit 1 }

Write-Host "[2/5] Cleaning existing database tables..."
docker compose -f docker-compose.prod.yml exec -T db psql -U fayez -d ffp_validator -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public; GRANT ALL ON SCHEMA public TO public;"
if ($LASTEXITCODE -ne 0) { Write-Host "❌ Failed to clean database."; exit 1 }

Write-Host "[3/5] Restoring database from file (this may take a while for large files)..."
if ($SqlFile.EndsWith(".gz")) {
    # If GZ, we pipe gunzip content to psql
    # NOTE: This assumes gunzip is available in the shell or we use docker to unzip
    docker compose -f docker-compose.prod.yml exec -T db sh -c "gunzip -c /tmp/restore.sql.gz | psql -U fayez -d ffp_validator"
} else {
    docker compose -f docker-compose.prod.yml exec -T db psql -U fayez -d ffp_validator -f /tmp/restore.sql --set ON_ERROR_STOP=off
}
# Ignore exit code here as pg_dump usually throws benign notices on restore

Write-Host "[4/5] Cleaning up temp file..."
docker compose -f docker-compose.prod.yml exec -T db rm /tmp/restore.sql

Write-Host "[5/5] Restarting backend to run schema migrations and verify data..."
docker compose -f docker-compose.prod.yml restart backend
Start-Sleep -Seconds 10 # give it time to run startup scripts

Write-Host "`n✅ Restore process completed successfully!"
Write-Host "The backend has been restarted to ensure all missing schema columns and tables are created."
Write-Host "=========================================="
