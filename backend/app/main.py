"""
Financial Intelligence Engine — Main FastAPI Application.

Production-ready financial analysis platform with:
- PDF statement ingestion & parsing
- Transaction normalization & categorization (rule-based + Gemini LLM)
- Analytics, recurring detection, anomaly detection
- Budget tracking & savings goals
- AI financial advisor chat
"""

import os
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, FileResponse
from dotenv import load_dotenv

load_dotenv()

# ─── Logging Configuration ──────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("fintech")


# ─── Lifespan (Startup / Shutdown) ──────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup and shutdown events."""
    # Startup
    logger.info("=" * 60)
    logger.info("Financial Intelligence Engine — Starting up")
    logger.info("=" * 60)

    from app.database import init_db
    init_db()
    logger.info("Database initialized")

    yield

    # Shutdown
    logger.info("Financial Intelligence Engine — Shutting down")


# ─── Create App ──────────────────────────────────────────────────

app = FastAPI(
    title="Financial Intelligence Engine",
    description="AI-powered financial analysis, budgeting, and advisory platform",
    version="1.0.0",
    lifespan=lifespan,
)


# ─── CORS ────────────────────────────────────────────────────────

CORS_ORIGINS = os.getenv("CORS_ORIGINS", "http://localhost:8000,http://localhost:3000").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── File Size Limit Middleware ──────────────────────────────────

MAX_BODY_SIZE = int(os.getenv("MAX_UPLOAD_SIZE_MB", "20")) * 1024 * 1024 + 1024 * 100  # +100KB headroom


@app.middleware("http")
async def limit_upload_size(request: Request, call_next):
    """Reject requests with body exceeding max upload size."""
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > MAX_BODY_SIZE:
        return JSONResponse(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            content={"detail": f"Request body too large. Max: {MAX_BODY_SIZE // (1024*1024)}MB"},
        )
    return await call_next(request)


# ─── Health Check ────────────────────────────────────────────────

@app.get("/health", tags=["System"])
async def health_check():
    """Health check endpoint for Railway/monitoring."""
    return {"status": "healthy", "service": "Financial Intelligence Engine"}


# ─── API Routes ──────────────────────────────────────────────────

from app.routes import auth, upload, transactions, dashboard, budget, goals, advisor, planner, predictive, gamification, smart_budget, flashcards, planning_suite

app.include_router(auth.router, prefix="/api")
app.include_router(upload.router, prefix="/api")
app.include_router(transactions.router, prefix="/api")
app.include_router(dashboard.router, prefix="/api")
app.include_router(budget.router, prefix="/api")
app.include_router(smart_budget.router, prefix="/api")
app.include_router(goals.router, prefix="/api")
app.include_router(advisor.router, prefix="/api")
app.include_router(planner.router, prefix="/api")
app.include_router(predictive.router, prefix="/api")
app.include_router(gamification.router, prefix="/api")
app.include_router(flashcards.router, prefix="/api")
app.include_router(planning_suite.router, prefix="/api")


# ─── Static Frontend ────────────────────────────────────────────

# Determine frontend directory
FRONTEND_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "frontend")
if not os.path.exists(FRONTEND_DIR):
    FRONTEND_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "frontend")


@app.get("/", tags=["Frontend"])
async def serve_index():
    """Serve the main frontend page."""
    index_path = os.path.join(FRONTEND_DIR, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return {"message": "Financial Intelligence Engine API", "docs": "/docs"}


# Mount static files (CSS, JS, etc.) — must be after API routes
if os.path.exists(FRONTEND_DIR):
    app.mount("/", StaticFiles(directory=FRONTEND_DIR, html=True), name="frontend")
    logger.info(f"Frontend static files mounted from: {FRONTEND_DIR}")
