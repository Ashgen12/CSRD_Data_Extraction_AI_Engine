"""
CSRD Data Extraction Engine - Database Handler

SQLAlchemy ORM for PostgreSQL with CRUD operations and CSV export.
"""
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
from contextlib import contextmanager
import uuid

from sqlalchemy import (
    create_engine,
    Column,
    String,
    Integer,
    Float,
    Text,
    DateTime,
    UniqueConstraint,
    Index,
)
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from sqlalchemy.dialects.postgresql import UUID
import pandas as pd

from .models import CSRDIndicator, BankExtractionResult

logger = logging.getLogger(__name__)

Base = declarative_base()


class SustainabilityIndicator(Base):
    """SQLAlchemy model for sustainability indicators table."""
    
    __tablename__ = "sustainability_indicators"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    company = Column(String(100), nullable=False, index=True)
    report_year = Column(Integer, nullable=False, index=True)
    indicator_id = Column(String(20), nullable=False, index=True)
    indicator_name = Column(String(200), nullable=False)
    value = Column(Float, nullable=True)
    unit = Column(String(50), nullable=False)
    confidence_score = Column(Float, nullable=False, default=0.0)
    source_page = Column(Integer, nullable=True)
    source_section = Column(String(200), nullable=True)
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Constraints
    __table_args__ = (
        UniqueConstraint('company', 'report_year', 'indicator_id', 
                        name='uq_company_year_indicator'),
        Index('ix_confidence_desc', confidence_score.desc()),
    )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": str(self.id),
            "company": self.company,
            "report_year": self.report_year,
            "indicator_id": self.indicator_id,
            "indicator_name": self.indicator_name,
            "value": self.value,
            "unit": self.unit,
            "confidence_score": self.confidence_score,
            "source_page": self.source_page,
            "source_section": self.source_section,
            "notes": self.notes,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class ExtractionRun(Base):
    """SQLAlchemy model for tracking extraction runs."""
    
    __tablename__ = "extraction_runs"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    company = Column(String(100), nullable=False)
    report_year = Column(Integer, nullable=False)
    started_at = Column(DateTime, default=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)
    status = Column(String(20), default="running")  # running, completed, failed
    total_indicators = Column(Integer, default=0)
    successful_extractions = Column(Integer, default=0)
    avg_confidence = Column(Float, nullable=True)
    error_message = Column(Text, nullable=True)


class DatabaseHandler:
    """
    Handles all database operations for the CSRD extraction engine.
    
    Provides CRUD operations, batch inserts, and export functionality.
    """
    
    def __init__(self, database_url: Optional[str] = None):
        """
        Initialize the database handler.
        
        Args:
            database_url: PostgreSQL connection URL
        """
        from config.settings import settings
        
        self.database_url = database_url or settings.database_url
        
        # Create engine
        self.engine = create_engine(
            self.database_url,
            echo=False,  # Set to True for SQL debugging
            pool_pre_ping=True,  # Check connections before use
        )
        
        # Create session factory
        self.SessionLocal = sessionmaker(
            bind=self.engine,
            autocommit=False,
            autoflush=False,
        )
        
        logger.info(f"Database handler initialized")
    
    def create_tables(self) -> None:
        """Create all database tables."""
        Base.metadata.create_all(self.engine)
        logger.info("Database tables created")
    
    def drop_tables(self) -> None:
        """Drop all database tables (use with caution!)."""
        Base.metadata.drop_all(self.engine)
        logger.warning("Database tables dropped")
    
    @contextmanager
    def get_session(self) -> Session:
        """Get a database session with automatic cleanup."""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
    
    def save_indicator(self, indicator: CSRDIndicator, company: str, report_year: int) -> str:
        """
        Save a single indicator to the database.
        
        Args:
            indicator: The indicator to save
            company: Company name
            report_year: Report year
            
        Returns:
            ID of the saved record
        """
        with self.get_session() as session:
            db_indicator = SustainabilityIndicator(
                company=company,
                report_year=report_year,
                indicator_id=indicator.indicator_id,
                indicator_name=indicator.indicator_name,
                value=indicator.value,
                unit=indicator.unit,
                confidence_score=indicator.confidence_score,
                source_page=indicator.source_page,
                source_section=indicator.source_section,
                notes=indicator.notes,
            )
            session.add(db_indicator)
            session.flush()
            return str(db_indicator.id)
    
    def save_extraction_result(self, result: BankExtractionResult) -> int:
        """
        Save a complete extraction result to the database.
        
        Args:
            result: Complete bank extraction result
            
        Returns:
            Number of indicators saved
        """
        logger.info(f"Saving extraction result for {result.company}")
        
        saved_count = 0
        
        with self.get_session() as session:
            for indicator in result.indicators:
                # Check for existing record
                existing = session.query(SustainabilityIndicator).filter(
                    SustainabilityIndicator.company == result.company,
                    SustainabilityIndicator.report_year == result.report_year,
                    SustainabilityIndicator.indicator_id == indicator.indicator_id,
                ).first()
                
                if existing:
                    # Update existing record
                    existing.value = indicator.value
                    existing.unit = indicator.unit
                    existing.confidence_score = indicator.confidence_score
                    existing.source_page = indicator.source_page
                    existing.source_section = indicator.source_section
                    existing.notes = indicator.notes
                    existing.updated_at = datetime.utcnow()
                else:
                    # Insert new record
                    db_indicator = SustainabilityIndicator(
                        company=result.company,
                        report_year=result.report_year,
                        indicator_id=indicator.indicator_id,
                        indicator_name=indicator.indicator_name,
                        value=indicator.value,
                        unit=indicator.unit,
                        confidence_score=indicator.confidence_score,
                        source_page=indicator.source_page,
                        source_section=indicator.source_section,
                        notes=indicator.notes,
                    )
                    session.add(db_indicator)
                
                saved_count += 1
        
        logger.info(f"Saved {saved_count} indicators for {result.company}")
        return saved_count
    
    def get_all_indicators(
        self, 
        company: Optional[str] = None,
        report_year: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get all indicators, optionally filtered.
        
        Args:
            company: Filter by company name
            report_year: Filter by report year
            
        Returns:
            List of indicator dictionaries
        """
        with self.get_session() as session:
            query = session.query(SustainabilityIndicator)
            
            if company:
                query = query.filter(SustainabilityIndicator.company == company)
            if report_year:
                query = query.filter(SustainabilityIndicator.report_year == report_year)
            
            query = query.order_by(
                SustainabilityIndicator.company,
                SustainabilityIndicator.indicator_id,
            )
            
            return [ind.to_dict() for ind in query.all()]
    
    def get_low_confidence_indicators(
        self, 
        threshold: float = 0.6
    ) -> List[Dict[str, Any]]:
        """
        Get indicators with confidence below threshold.
        
        Args:
            threshold: Confidence threshold
            
        Returns:
            List of low-confidence indicators
        """
        with self.get_session() as session:
            query = session.query(SustainabilityIndicator).filter(
                SustainabilityIndicator.confidence_score < threshold
            ).order_by(SustainabilityIndicator.confidence_score)
            
            return [ind.to_dict() for ind in query.all()]
    
    def get_summary_stats(self) -> Dict[str, Any]:
        """
        Get summary statistics for the database.
        
        Returns:
            Dictionary with statistics
        """
        with self.get_session() as session:
            total = session.query(SustainabilityIndicator).count()
            
            # Get counts by company
            from sqlalchemy import func
            company_counts = session.query(
                SustainabilityIndicator.company,
                func.count(SustainabilityIndicator.id).label("count"),
                func.avg(SustainabilityIndicator.confidence_score).label("avg_confidence"),
            ).group_by(SustainabilityIndicator.company).all()
            
            return {
                "total_indicators": total,
                "by_company": [
                    {
                        "company": c.company,
                        "count": c.count,
                        "avg_confidence": round(c.avg_confidence, 3) if c.avg_confidence else 0,
                    }
                    for c in company_counts
                ],
            }
    
    def export_to_csv(
        self, 
        output_path: Path,
        company: Optional[str] = None,
        report_year: Optional[int] = None,
    ) -> int:
        """
        Export indicators to CSV file.
        
        Args:
            output_path: Path for the CSV file
            company: Filter by company
            report_year: Filter by year
            
        Returns:
            Number of rows exported
        """
        indicators = self.get_all_indicators(company, report_year)
        
        if not indicators:
            logger.warning("No indicators to export")
            return 0
        
        df = pd.DataFrame(indicators)
        
        # Reorder columns for better readability
        column_order = [
            "company",
            "report_year",
            "indicator_id",
            "indicator_name",
            "value",
            "unit",
            "confidence_score",
            "source_page",
            "source_section",
            "notes",
        ]
        
        # Only include columns that exist
        columns = [c for c in column_order if c in df.columns]
        df = df[columns]
        
        # Save to CSV
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False, encoding="utf-8")
        
        logger.info(f"Exported {len(df)} rows to {output_path}")
        return len(df)
    
    def delete_company_data(self, company: str, report_year: Optional[int] = None) -> int:
        """
        Delete all data for a company.
        
        Args:
            company: Company name
            report_year: Optional year filter
            
        Returns:
            Number of rows deleted
        """
        with self.get_session() as session:
            query = session.query(SustainabilityIndicator).filter(
                SustainabilityIndicator.company == company
            )
            
            if report_year:
                query = query.filter(SustainabilityIndicator.report_year == report_year)
            
            count = query.delete()
            logger.warning(f"Deleted {count} indicators for {company}")
            return count


def test_database_handler():
    """Test the database handler."""
    from .models import CSRDIndicator, BankExtractionResult
    
    handler = DatabaseHandler()
    
    # Create tables
    handler.create_tables()
    print("Tables created")
    
    # Create test data
    test_result = BankExtractionResult(
        company="TEST_BANK",
        report_year=2024,
        pdf_filename="test_2024.pdf",
        indicators=[
            CSRDIndicator(
                indicator_id="E1",
                indicator_name="Scope 1 GHG Emissions",
                value=15000,
                unit="tCO₂e",
                confidence_score=0.85,
                source_page=42,
            ),
            CSRDIndicator(
                indicator_id="S1",
                indicator_name="Total Employees",
                value=10500,
                unit="FTE",
                confidence_score=0.95,
                source_page=28,
            ),
        ],
    )
    
    # Save extraction
    count = handler.save_extraction_result(test_result)
    print(f"Saved {count} indicators")
    
    # Get all indicators
    indicators = handler.get_all_indicators()
    print(f"Retrieved {len(indicators)} indicators")
    
    # Get summary
    stats = handler.get_summary_stats()
    print(f"Stats: {stats}")
    
    # Export to CSV
    from config.settings import settings
    output_path = settings.output_data_path / "test_export.csv"
    exported = handler.export_to_csv(output_path)
    print(f"Exported {exported} rows to {output_path}")


if __name__ == "__main__":
    test_database_handler()
