param (
    [string]$BackupFile = "backup_$(Get-Date -Format 'yyyyMMdd_HHmmss').sql"
)

$BackupPath = Join-Path -Path "db_backups" -ChildPath $BackupFile

Write-Host "=========================================="
Write-Host " Starting Prod DB Backup"
Write-Host "=========================================="
Write-Host "Destination: $BackupPath"
Write-Host "Please wait, this may take a few minutes..."

# Using the db-backup container since it already has the volume mounted to /backups
docker compose -f docker-compose.prod.yml exec -T db-backup sh -c "pg_dump -h db -U `$$POSTGRES_USER `$$POSTGRES_DB > /backups/$BackupFile"

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "✅ Backup completed successfully!"
    Write-Host "File saved to: $BackupPath"
} else {
    Write-Host ""
    Write-Host "❌ Backup failed. Check if containers are running."
    exit 1
}
