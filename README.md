# 🌾 Food Friendly Program (FFP) Data Validator

![FF Validator CI Status](https://github.com/fayezacce/FFP-DataValidation/actions/workflows/main.yml/badge.svg)
![Live Dev Enabled](https://img.shields.io/badge/LiveDev-Enabled-brightgreen)
![Docker Ready](https://img.shields.io/badge/Docker-Ready-blue)

A robust Data Validation and Normalization tool built for the **Food Friendly Program**. This system ensures that beneficiary data (NIDs and Birth Dates) are properly formatted, validated, and normalized before reaching the core government databases.

---

## ✨ Core Features

-   **NID Validation**: Validates Smart NIDs (10-digit) and Standard NIDs (17-digit).
-   **NID Conversion**: Intelligent conversion of 13-digit legacy NIDs to 17-digit format using Date of Birth (DOB) logic.
-   **Multi-Format DOB Processing**: Handles various date formats (DD/MM/YYYY, YYYY-MM-DD, Bengali digits, etc.).
-   **Audit Logging**: Comprehensive tracking of every record processed.
-   **RBAC (Role-Based Access Control)**: Secure access for Admin and Viewer roles.
-   **Excel Integration**: Batch process thousands of records via Excel upload/download.
-   **PDF Reporting**: Generates validation summary reports.

---

## 🛠️ Technology Stack

-   **Backend**: FastAPI (Python 3.12), SQLAlchemy, PostgreSQL, Pytest.
-   **Frontend**: Next.js 14, React, Tailwind CSS.
-   **Infrastructure**: Docker, Nginx, GitHub Actions (CI/CD).
-   **Security**: JWT Authentication, Argon2/Bcrypt Hashing.

---

## 🚀 Quick Start (Local Development)

### Prerequisites
-   Docker and Docker Compose installed.

### Setup
1.  **Clone the Repo**:
    ```bash
    git clone https://github.com/fayezacce/FFP-DataValidation.git
    cd FFP-DataValidation
    ```

2.  **Start with Live Development (Hot-Reloading)**:
    ```bash
    docker compose up -d
    ```
    -   **Backend**: Accessible via proxy at `http://localhost:3000/api` (reloads on save).
    -   **Frontend**: Accessible at `http://localhost:3000` (reloads on save).

---

## 🚢 Production Deployment

For a stable production environment, we use **Image-Based Deployment** via GHCR.

1.  **Pull and Run**:
    ```bash
    docker compose -f docker-compose.prod.yml pull
    docker compose -f docker-compose.prod.yml up -d
    ```

> [!NOTE]
> The CI/CD pipeline automatically builds and pushes the latest verified images to `ghcr.io` whenever you push to the `main` branch.

---

## 🧪 Testing

We prioritize data integrity. Our test suite covers validation logic and API security.

**Run tests locally**:
```bash
cd backend
python -m pytest tests/ -v
```

---

## 📁 Project Structure

```text
.
├── .github/workflows/   # CI/CD (GitHub Actions)
├── backend/
│   ├── app/             # FastAPI Application
│   ├── tests/           # Pytest Suite
│   └── Dockerfile       # Backend Image definition
├── frontend/
│   ├── src/             # Next.js Source
│   └── Dockerfile       # Frontend Multi-stage Build
├── docker-compose.yml   # Dev configuration (Live-Reload)
├── docker-compose.prod.yml # Prod configuration (Image-Based)
└── nginx.conf           # Reverse Proxy Config
```

---

## 🤝 Contribution Guidelines

1.  Create a feature branch from `dev`.
2.  Ensure all tests pass.
3.  Open a Pull Request to `main`.

---

## 🛡️ License
Proprietary for Food Friendly Program.
