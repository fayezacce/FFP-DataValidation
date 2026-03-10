# Production Migration & CI/CD Guide

This guide explains how to safely deploy the recent updates and configure the automated CI/CD pipeline for the FFP Data Validation platform.

## 1. Safe Database Migration (Zero Data Loss)

Your current production database holds 1 million records in the `ValidRecord` table. The latest changes introduced a new table called `InvalidRecord` to persistently store failed validations instead of dumping them into JSON files.

**Is it safe to update?** 
Yes! The backend relies on SQLAlchemy's `Base.metadata.create_all(bind=engine)`, which issues `CREATE TABLE IF NOT EXISTS`. It will only create the missing `InvalidRecord` table and leave your 1 million existing rows in `ValidRecord` completely untouched.

**Steps to deploy manually on the production server:**
1. SSH into your production server.
2. Navigate to your project directory.
3. Pull the latest code/images and restart the container stack:
```bash
docker compose -f docker-compose.prod.yml pull
docker compose -f docker-compose.prod.yml up -d --remove-orphans
```
The backend will automatically create the required `invalid_records` table on startup.

## 2. Setting Up GitHub Actions CI/CD

A complete, production-ready CI/CD pipeline is already present in your repository at `.github/workflows/main.yml`. It handles:
- Running backend tests (`pytest`).
- Building optimized Docker images for your Frontend and Backend.
- Pushing those images to GitHub Container Registry (`ghcr.io`).
- Automatically connecting via SSH to your production server to trigger a rolling update.

### Required GitHub Secrets
To enable the pipeline, go to your GitHub Repository -> **Settings** -> **Secrets and variables** -> **Actions** -> **New repository secret**, and add the following:

- `SERVER_HOST`: The IP address or domain of your production server.
- `SERVER_USER`: The SSH user (e.g., `root` or `ubuntu`).
- `SERVER_SSH_KEY`: The private SSH key strictly formatted (e.g., standard RSA).
- `SERVER_PORT`: (Optional) If your SSH port isn't 22.
- `DEPLOY_PATH`: The absolute path on your server where `docker-compose.prod.yml` relies (e.g., `/opt/ffp-datavalidation`).

### Triggering the Pipeline
- **Automatic**: Just push or merge PRs into the `main` branch! 
- **Manual Override**: Go to the **Actions** tab in GitHub -> Select **FF Validator CI/CD** -> Click **Run workflow**.

## 3. Performance & Security Report
- **Performance**: We have previously optimized the chunked file reading. The new `InvalidRecord` table uses a dedicated relational store, drastically reducing heavy JSON operations, lowering RAM footprint, and improving the speed of validation lookups/downloads.
- **Security Check**: The application has role-based endpoints and handles tokens cleanly, however, **make sure to change the default `admin123` password** currently in production, as the backend explicitly logs a security warning for it. 
