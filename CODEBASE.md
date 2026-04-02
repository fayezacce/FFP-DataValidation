# CODEBASE.md - FFP Data Validator

## Directory Map

| Path | Purpose |
| :--- | :--- |
| `backend/app/` | Core API logic, data validation, and database models. |
| `frontend/src/` | Next.js application source code (UI/Data). |
| `downloads/` | Persistent storage for generated Excel and PDF reports. |
| `uploads/` | Temporary storage for newly uploaded validation files. |
| `scripts/` | Utility scripts for database maintenance and geoscraping. |

## Key Files

- **Backend Entry Point**: `backend/app/main.py` - FastAPI app, lifespan, and bulk export routes.
- **Data Validation**: `backend/app/validator.py` - Pandas-based rule sets for NID, Mobile, and Date validation.
- **PDF Generation**: `backend/app/pdf_generator.py` - FPDF/WeasyPrint integration for report creation.
- **Frontend Dashboard**: `frontend/src/app/statistics/page.tsx` - High-level summary of validation results.
- **App Environment**: `.env.production` - Deployment secrets and database credentials.

## Dependencies

- **FastAPI**: Asynchronous web framework.
- **SQLAlchemy 2.0**: Database ORM.
- **Pandas**: High-performance data manipulation.
- **Next.js 14**: Modern web user interface.
- **Lucide-React**: SVG Iconography.
- **Tailwind CSS**: Utility-first styling.
