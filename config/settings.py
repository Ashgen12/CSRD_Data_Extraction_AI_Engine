"""
CSRD Data Extraction Engine - Settings Configuration

Manages all configuration using environment variables and pydantic-settings.
"""
import os
from pathlib import Path
from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    # Google Cloud / Vertex AI
    project_id: str = Field(default="aiss-ai-lab", alias="PROJECT_ID")
    location: str = Field(default="us-central1", alias="LOCATION")
    model_name: str = Field(default="gemini-2.5-flash", alias="MODEL_NAME")
    google_credentials_path: str = Field(
        default="../aiss-ai-lab-643e7e2bd5b5.json",
        alias="GOOGLE_APPLICATION_CREDENTIALS"
    )
    
    # Database
    database_url: str = Field(
        default="postgresql://csrd_user:csrd_secure_pass@localhost:5433/csrd_reports",
        alias="DATABASE_URL"
    )
    
    # Extraction Settings
    extraction_temperature: float = Field(default=0.0, alias="EXTRACTION_TEMPERATURE")
    max_retries: int = Field(default=3, alias="MAX_RETRIES")
    confidence_threshold: float = Field(default=0.6, alias="CONFIDENCE_THRESHOLD")
    
    # Paths
    data_raw_dir: str = Field(default="data/raw", alias="DATA_RAW_DIR")
    data_processed_dir: str = Field(default="data/processed", alias="DATA_PROCESSED_DIR")
    data_output_dir: str = Field(default="data/output", alias="DATA_OUTPUT_DIR")
    chroma_persist_dir: str = Field(default="chroma_db", alias="CHROMA_PERSIST_DIR")
    
    # Logging
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_file: str = Field(default="extraction.log", alias="LOG_FILE")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"
    
    @property
    def base_dir(self) -> Path:
        """Get the base directory of the project."""
        return Path(__file__).parent.parent
    
    @property
    def credentials_absolute_path(self) -> Path:
        """Get absolute path to Google credentials file."""
        creds_path = Path(self.google_credentials_path)
        if not creds_path.is_absolute():
            creds_path = self.base_dir / creds_path
        return creds_path.resolve()
    
    @property
    def raw_data_path(self) -> Path:
        """Get absolute path to raw data directory."""
        return self.base_dir / self.data_raw_dir
    
    @property
    def processed_data_path(self) -> Path:
        """Get absolute path to processed data directory."""
        return self.base_dir / self.data_processed_dir
    
    @property
    def output_data_path(self) -> Path:
        """Get absolute path to output data directory."""
        return self.base_dir / self.data_output_dir
    
    @property
    def chroma_path(self) -> Path:
        """Get absolute path to ChromaDB directory."""
        return self.base_dir / self.chroma_persist_dir
    
    @property
    def indicators_config_path(self) -> Path:
        """Get path to indicators YAML configuration."""
        return self.base_dir / "config" / "indicators.yaml"
    
    def setup_google_credentials(self) -> None:
        """Set up Google Application Credentials environment variable."""
        creds_path = self.credentials_absolute_path
        if creds_path.exists():
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(creds_path)
        else:
            raise FileNotFoundError(
                f"Google credentials file not found at: {creds_path}"
            )
    
    def ensure_directories(self) -> None:
        """Ensure all required directories exist."""
        for path in [
            self.raw_data_path,
            self.processed_data_path,
            self.output_data_path,
            self.chroma_path,
        ]:
            path.mkdir(parents=True, exist_ok=True)


# Global settings instance
settings = Settings()
