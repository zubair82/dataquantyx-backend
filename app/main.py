"""
DataQuantyx - Backend Application Entry Point

This module initializes the FastAPI application, configures middleware,
sets up database connections, and registers API routes.
"""

import os
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from app.api import upload, analysis, compare, users, auth, admin
from app.services.admin_service import ensure_default_admin_exists
from app.utils.database import SessionLocal, init_db, close_db
from app.utils.paths import DATA_DIR, PLOTS_DIR, REPORTS_DIR
from app.api import report

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Initialize FastAPI application
app = FastAPI(
    title="DataQuantyx",
    description="Backend system for processing and analyzing simulation datasets",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json"
)

# Configure CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins (configure as needed)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Initialize required directories
def init_directories():
    """Create required directories for the application"""
    directories = [DATA_DIR, PLOTS_DIR, REPORTS_DIR]
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        logger.info(f"Directory '{directory}' initialized")


# Initialize database
def init_database():
    """Initialize SQLite database and create tables"""
    try:
        init_db()
        db = SessionLocal()
        try:
            ensure_default_admin_exists(db)
        finally:
            db.close()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {str(e)}")


# Startup event
@app.on_event("startup")
async def startup_event():
    """Run on application startup"""
    logger.info("🚀 Starting DataQuantyx")
    init_directories()
    init_database()
    logger.info("✅ Application ready to serve requests")


# Shutdown event
@app.on_event("shutdown")
async def shutdown_event():
    """Run on application shutdown"""
    close_db()
    logger.info("🛑 Shutting down DataQuantyx")


# Include API routes
app.include_router(
    upload.router,
    prefix="/api",
    tags=["Upload"],
    responses={400: {"description": "Invalid request"}, 500: {"description": "Server error"}}
)

app.include_router(
    users.router,
    tags=["Users"],
    responses={400: {"description": "Invalid request"}, 404: {"description": "User not found"}, 500: {"description": "Server error"}}
)

app.include_router(
    analysis.router,
    prefix="/api",
    tags=["Analysis"],
    responses={400: {"description": "Invalid request"}, 404: {"description": "File not found"}, 500: {"description": "Server error"}}
)

app.include_router(
    compare.router,
    prefix="/api",
    tags=["Compare"],
    responses={400: {"description": "Invalid request"}, 404: {"description": "File not found"}, 500: {"description": "Server error"}}
)

# Register other API modules when implemented
app.include_router(auth.router)
app.include_router(admin.router)
app.include_router(report.router, prefix="/api", tags=["Report"])


# Root endpoint
@app.get(
    "/",
    tags=["Info"],
    summary="Get API Information",
    response_description="API metadata and available resources"
)
def read_root():
    """
    Get API root information
    
    Returns metadata about the DataQuantyx API
    """
    return {
        "application": "DataQuantyx",
        "version": "1.0.0",
        "description": "Backend system for processing and analyzing simulation datasets",
        "docs": "/docs",
        "redoc": "/redoc",
        "openapi": "/openapi.json",
        "health": "/health"
    }


# Health check endpoint
@app.get(
    "/health",
    tags=["Monitoring"],
    summary="Health Check",
    response_description="Server health status"
)
def health_check():
    """
    Health check endpoint
    
    Returns the current status of the server
    """
    return {
        "status": "ok",
        "timestamp": datetime.now().isoformat(),
        "service": "DataQuantyx"
    }


# Global exception handler
@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """Handle HTTP exceptions"""
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "status_code": exc.status_code,
            "timestamp": datetime.now().isoformat()
        }
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """Handle unexpected exceptions"""
    logger.error(f"Unexpected error: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "status_code": 500,
            "timestamp": datetime.now().isoformat()
        }
    )


# Optional: Mount static directories for plots and reports
app.mount("/plots", StaticFiles(directory=str(PLOTS_DIR)), name="plots")
app.mount("/reports", StaticFiles(directory=str(REPORTS_DIR)), name="reports")


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
