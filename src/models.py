"""
CSRD Data Extraction Engine - Pydantic Data Models

Defines structured data models for ESG indicators with validation rules.
"""
from datetime import datetime
from typing import Optional, List, Any
from pydantic import BaseModel, Field, field_validator, model_validator
import uuid


class CSRDIndicator(BaseModel):
    """Base model for a single CSRD indicator extraction."""
    
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    indicator_id: str = Field(..., description="Indicator ID (E1, S3, G2, etc.)")
    indicator_name: str = Field(..., description="Full indicator name")
    value: Optional[float] = Field(None, description="Extracted numeric value")
    unit: str = Field(..., description="Unit of measurement")
    confidence_score: float = Field(
        ..., 
        ge=0.0, 
        le=1.0, 
        description="Confidence score from 0.0 to 1.0"
    )
    source_page: Optional[int] = Field(None, description="PDF page number where value was found")
    source_section: Optional[str] = Field(None, description="Section heading in the report")
    notes: Optional[str] = Field(None, description="Additional context or notes")
    raw_text: Optional[str] = Field(None, description="Original text excerpt")
    
    @field_validator('confidence_score')
    @classmethod
    def validate_confidence(cls, v: float) -> float:
        """Ensure confidence is within valid range."""
        if v < 0.0 or v > 1.0:
            raise ValueError(f"Confidence score must be between 0.0 and 1.0, got {v}")
        return round(v, 3)


class EnvironmentalData(BaseModel):
    """Environmental indicators (E1-E8) with validation."""
    
    scope_1_emissions: Optional[float] = Field(
        None, 
        description="Scope 1 direct GHG emissions in tCO₂e",
        ge=0
    )
    scope_2_emissions: Optional[float] = Field(
        None, 
        description="Scope 2 indirect GHG emissions in tCO₂e",
        ge=0
    )
    scope_2_location_based: Optional[float] = Field(
        None,
        description="Scope 2 location-based emissions in tCO₂e",
        ge=0
    )
    scope_2_market_based: Optional[float] = Field(
        None,
        description="Scope 2 market-based emissions in tCO₂e",
        ge=0
    )
    scope_3_emissions: Optional[float] = Field(
        None, 
        description="Scope 3 value chain emissions in tCO₂e",
        ge=0
    )
    ghg_intensity: Optional[float] = Field(
        None, 
        description="GHG emissions intensity in tCO₂e per €M revenue",
        ge=0
    )
    total_energy_consumption: Optional[float] = Field(
        None, 
        description="Total energy consumption in MWh",
        ge=0
    )
    renewable_energy_percentage: Optional[float] = Field(
        None, 
        description="Percentage of renewable energy (0-100)",
        ge=0,
        le=100
    )
    net_zero_target_year: Optional[int] = Field(
        None, 
        description="Target year for net zero emissions",
        ge=2024,
        le=2100
    )
    green_financing_volume: Optional[float] = Field(
        None, 
        description="Green financing volume in €M",
        ge=0
    )
    
    @model_validator(mode='after')
    def validate_scope_2_logic(self):
        """Validate Scope 2 market vs location-based logic."""
        loc = self.scope_2_location_based
        mkt = self.scope_2_market_based
        
        # Market-based is typically lower than location-based due to green tariffs
        if mkt is not None and loc is not None:
            if mkt > (loc * 1.5):
                # This is unusual but not necessarily wrong - add warning
                pass
        
        return self
    
    @field_validator('renewable_energy_percentage')
    @classmethod
    def validate_percentage(cls, v: Optional[float]) -> Optional[float]:
        """Ensure renewable percentage is valid."""
        if v is not None and (v < 0 or v > 100):
            raise ValueError(f"Renewable energy percentage must be 0-100, got {v}")
        return v


class SocialData(BaseModel):
    """Social indicators (S1-S7) with validation."""
    
    total_employees: Optional[int] = Field(
        None, 
        description="Total number of full-time equivalent employees",
        ge=0
    )
    female_employees_percentage: Optional[float] = Field(
        None, 
        description="Percentage of female employees (0-100)",
        ge=0,
        le=100
    )
    gender_pay_gap: Optional[float] = Field(
        None, 
        description="Gender pay gap percentage (can be negative)",
        ge=-100,
        le=100
    )
    training_hours_per_employee: Optional[float] = Field(
        None, 
        description="Average training hours per employee per year",
        ge=0
    )
    employee_turnover_rate: Optional[float] = Field(
        None, 
        description="Annual employee turnover rate percentage",
        ge=0,
        le=100
    )
    work_related_accidents: Optional[int] = Field(
        None, 
        description="Number of work-related accidents/injuries",
        ge=0
    )
    collective_bargaining_coverage: Optional[float] = Field(
        None, 
        description="Percentage of employees covered by collective bargaining",
        ge=0,
        le=100
    )


class GovernanceData(BaseModel):
    """Governance indicators (G1-G5) with validation."""
    
    board_female_percentage: Optional[float] = Field(
        None, 
        description="Percentage of female board members (0-100)",
        ge=0,
        le=100
    )
    board_meetings_per_year: Optional[int] = Field(
        None, 
        description="Number of board meetings held per year",
        ge=0
    )
    corruption_incidents: Optional[int] = Field(
        None, 
        description="Number of confirmed corruption/bribery incidents",
        ge=0
    )
    avg_supplier_payment_days: Optional[float] = Field(
        None, 
        description="Average days to pay suppliers",
        ge=0
    )
    suppliers_screened_esg_percentage: Optional[float] = Field(
        None, 
        description="Percentage of suppliers screened for ESG criteria",
        ge=0,
        le=100
    )


class BankExtractionResult(BaseModel):
    """Complete extraction result for a single bank."""
    
    company: str = Field(..., description="Bank name")
    report_year: int = Field(..., description="Reporting year")
    extraction_timestamp: datetime = Field(
        default_factory=datetime.utcnow,
        description="When extraction was performed"
    )
    pdf_filename: str = Field(..., description="Source PDF filename")
    total_pages: Optional[int] = Field(None, description="Total pages in PDF")
    
    # Structured data
    environmental: EnvironmentalData = Field(default_factory=EnvironmentalData)
    social: SocialData = Field(default_factory=SocialData)
    governance: GovernanceData = Field(default_factory=GovernanceData)
    
    # Individual indicators with metadata
    indicators: List[CSRDIndicator] = Field(
        default_factory=list,
        description="List of all extracted indicators with metadata"
    )
    
    # Quality metrics
    avg_confidence: Optional[float] = Field(None, description="Average confidence score")
    low_confidence_count: int = Field(
        default=0, 
        description="Number of indicators with confidence < threshold"
    )
    missing_indicators: List[str] = Field(
        default_factory=list,
        description="List of indicator IDs that could not be extracted"
    )
    
    def calculate_metrics(self, threshold: float = 0.6) -> None:
        """Calculate quality metrics based on extracted indicators."""
        if self.indicators:
            scores = [i.confidence_score for i in self.indicators]
            self.avg_confidence = sum(scores) / len(scores)
            self.low_confidence_count = sum(1 for s in scores if s < threshold)
    
    def to_csv_rows(self) -> List[dict]:
        """Convert extraction result to CSV-compatible rows."""
        rows = []
        for indicator in self.indicators:
            rows.append({
                "company": self.company,
                "report_year": self.report_year,
                "indicator_id": indicator.indicator_id,
                "indicator_name": indicator.indicator_name,
                "value": indicator.value,
                "unit": indicator.unit,
                "confidence_score": indicator.confidence_score,
                "source_page": indicator.source_page,
                "source_section": indicator.source_section,
                "notes": indicator.notes,
            })
        return rows


class ExtractionError(BaseModel):
    """Model for tracking extraction errors."""
    
    indicator_id: str
    error_type: str
    error_message: str
    raw_output: Optional[str] = None
    retry_count: int = 0
    resolved: bool = False
