# DataQuantyx Backend

## Overview

DataQuantyx is a FastAPI backend for CSV upload, analysis, comparison, and HTML report generation.

Current auth uses a real Google OAuth 2.0 flow:

* one auth API handles both register and login
* if the email already exists, the user is logged in
* if the email does not exist, a new user is created and logged in
* auth uses server-side sessions, not JWT

## Main Features

* Upload CSV files
* Analyze numeric columns
* Generate plots
* Compare two uploaded files
* Generate single-file and comparison reports
* User-based data isolation
* Google OAuth sign-in with server-side session tokens

## Tech Stack

* FastAPI
* SQLAlchemy
* SQLite
* Pandas
* Matplotlib

## Run Locally

```bash
cd backend
source ../_env/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Open:

* `http://localhost:8000/docs`
* `http://localhost:8000/openapi.json`

## Auth Flow

Use the Google auth endpoint:

* `POST /api/auth/google/login`

Request body:

```json
{
  "credential": "<google_id_token>"
}
```

Behavior:

* existing email: logs in
* new email: creates user and logs in

Response:

```json
{
  "message": "User logged in successfully",
  "access_token": "<session_token>",
  "token_type": "bearer",
  "auth_provider": "google",
  "is_new_user": false,
  "user": {
    "user_id": "uuid",
    "user_name": "zubair_ahmad",
    "email": "zubair@example.com",
    "first_name": "Zubair",
    "last_name": "Ahmad",
    "organization": null,
    "role": "user",
    "is_active": true
  }
}
```

Send the returned session token as:

```http
Authorization: Bearer <session_token>
```

Other auth endpoints:

* `GET /api/auth/me`
* `POST /api/auth/logout`

## Admin Auth

Admins now use a separate table and separate login API:

* `POST /api/admin/login`
* `GET /api/admin/me`
* `GET /api/admin/users`
* `POST /api/admin/logout`

Request body:

```json
{
  "credential": "<google_id_token>"
}
```

By default, the backend creates one admin account at startup for local development.
You can override it with:

* `DEFAULT_ADMIN_EMAIL`
* `DEFAULT_ADMIN_NAME`

## API Summary

### Public

* `GET /`
* `GET /health`
* `GET /docs`
* `GET /openapi.json`

### Auth

* `POST /api/auth/google/login`
* `GET /api/auth/me`
* `POST /api/auth/logout`

### Admin

* `POST /api/admin/login`
* `GET /api/admin/me`
* `GET /api/admin/users`
* `POST /api/admin/logout`

### Users

* `GET /api/users/user/{user_id}`
* `PUT /api/users/user/{user_id}`
* `POST /api/users/user/{user_id}/deactivate`
* `POST /api/users/user/{user_id}/activate`
* `GET /api/users/user/{user_id}/files`
* `GET /api/users/user/{user_id}/analysis-history`

Admin-oriented routes:

* `GET /api/users`
* `GET /api/users/list`
* `GET /api/users/username/{user_name}`

### Upload and Files

* `POST /api/upload`
* `GET /api/files`
* `GET /api/files/{file_id}`
* `GET /api/files/{file_id}/cleaning-report`
* `DELETE /api/files/{file_id}`

### Analysis

* `POST /api/analyze/{file_id}`
* `GET /api/analyze/{file_id}/insights`
* `GET /api/analyze/{file_id}/correlations`
* `GET /api/analyze/{file_id}/distribution/{column}`
* `GET /api/analyze/{file_id}/outliers/{column}`
* `GET /api/analyze/{file_id}/spikes/{column}`
* `GET /api/analyze/{file_id}/trends/{column}`

### Comparison

* `POST /api/compare`
* `GET /api/compare/{file_id_1}/{file_id_2}`
* `POST /api/compare/insights-only`

### Reports

* `POST /api/report/{file_id}`
* `POST /api/report/compare/{file_id_1}/{file_id_2}`

Comparison reports generate comparison plots as part of report creation.

## Important Product Notes

The application uses Google OAuth. Ensure your Google Client ID is configured in your `.env` files.

## Current Simplifications

* No JWT
* No password login
* No separate profile creation API
* No refresh token flow yet

## Data Model

Main tables in SQLite:

```sql
users(
  user_id TEXT PRIMARY KEY,
  user_name TEXT UNIQUE,
  email TEXT UNIQUE,
  first_name TEXT,
  last_name TEXT,
  organization TEXT,
  role TEXT,
  is_active BOOLEAN
)

admins(
  admin_id TEXT PRIMARY KEY,
  email TEXT UNIQUE,
  full_name TEXT,
  password_hash TEXT,
  is_active BOOLEAN
)

files(
  file_id TEXT PRIMARY KEY,
  user_id TEXT,
  filename TEXT,
  file_path TEXT,
  row_count INTEGER
)

analysis_history(
  history_id TEXT PRIMARY KEY,
  user_id TEXT,
  file_id TEXT
)

auth_sessions(
  session_id TEXT PRIMARY KEY,
  session_token_hash TEXT UNIQUE,
  user_id TEXT,
  provider TEXT,
  created_at DATETIME,
  expires_at DATETIME
)

admin_sessions(
  session_id TEXT PRIMARY KEY,
  session_token_hash TEXT UNIQUE,
  admin_id TEXT,
  created_at DATETIME,
  expires_at DATETIME
)
```

Older tables such as `user_profiles` and `revoked_tokens` may still exist in the local DB from previous iterations, but the active auth flow now uses `auth_sessions`.

## Helper Script

Use [test_api.sh](/Users/zubairahmad/zubair/my_projects/data_quantyx/DataQuantyx-backend/test_api.sh) from the project root.

Examples:

```bash
./test_api.sh health
./test_api.sh google-auth
./test_api.sh admin-login
./test_api.sh admin-users
./test_api.sh me
CSV_PATH=/full/path/sample.csv ./test_api.sh upload
./test_api.sh analyze
./test_api.sh report
./test_api.sh logout
```

One-shot smoke test:

```bash
CSV_PATH=/full/path/sample.csv ./test_api.sh full-smoke-test
```

Useful environment variables:

```bash
BASE_URL=http://localhost:8000
EMAIL=zubair@example.com
NAME="Zubair Ahmad"
ORG="DQ Labs"
ADMIN_EMAIL=admin.dataqtx@gmail.com
CSV_PATH=/full/path/sample.csv
COLUMN=temperature
FILE_ID_2=<second-file-id>
```

## Project Structure

```text
app/
  api/
    auth.py
    users.py
    upload.py
    analysis.py
    compare.py
    report.py
  services/
    auth_service.py
    parser.py
    analyzer.py
    plotting.py
    comparator.py
    report_generator.py
    user_service.py
  utils/
    database.py
    file_service.py
  main.py
db/
data/
plots/
reports/
test_api.sh
```
