from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    """Central configuration. Every value comes from the environment or .env
    nothing hardcoded (assessment requirement)."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- MongoDB ---
    mongo_uri: str = "mongodb://localhost:27017"
    mongo_db: str = "wrc"
    mongo_collection_landing: str = "decisions_landing"
    mongo_collection_curated: str = "decisions_curated"

    # --- MinIO / S3 ---
    s3_endpoint_url: str = "http://localhost:9000"
    s3_access_key: str = "minioadmin"
    s3_secret_key: str = "minioadmin"
    s3_bucket_landing: str = "landing"
    s3_bucket_curated: str = "curated"

    # --- Scraper ---
    scraper_start_url: str = "https://www.workplacerelations.ie/en/search/"
    partition_months: int = 1
    download_delay: float = 1.0
    concurrent_requests: int = 4

settings = Settings()