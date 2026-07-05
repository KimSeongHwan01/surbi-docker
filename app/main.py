from fastapi import FastAPI
from app.routers import auth, districts, businesses, supports, scores, favorites

app = FastAPI()

app.include_router(auth.router, prefix="/api/v1")
app.include_router(districts.router, prefix="/api/v1")
app.include_router(businesses.router, prefix="/api/v1")
app.include_router(supports.router, prefix="/api/v1")
app.include_router(scores.router, prefix="/api/v1")
app.include_router(favorites.router, prefix="/api/v1")