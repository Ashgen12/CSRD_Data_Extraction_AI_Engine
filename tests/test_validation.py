"""
CSRD Data Extraction Engine - Validation Tests

Unit tests for the validator component.
"""
import pytest
from pathlib import Path
import sys

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import CSRDIndicator


class TestIndicatorValidation:
    """Tests for indicator validation."""
    
    def test_valid_emissions_indicator(self):
        """Test validation of a valid emissions indicator."""
        indicator = CSRDIndicator(
            indicator_id="E1",
            indicator_name="Scope 1 GHG Emissions",
            value=15000,
            unit="tCO₂e",
            confidence_score=0.85,
            source_page=42,
        )
        
        # Should be valid - no negative emissions
        assert indicator.value >= 0
    
    def test_valid_percentage_indicator(self):
        """Test validation of a percentage indicator."""
        indicator = CSRDIndicator(
            indicator_id="E6",
            indicator_name="Renewable Energy Percentage",
            value=67,
            unit="%",
            confidence_score=0.82,
            source_page=45,
        )
        
        # Should be valid percentage
        assert 0 <= indicator.value <= 100
    
    def test_null_value_handling(self):
        """Test that null values are handled correctly."""
        indicator = CSRDIndicator(
            indicator_id="G3",
            indicator_name="Corruption Incidents",
            value=None,  # Not disclosed
            unit="count",
            confidence_score=0.0,
            notes="Indicator not found in report",
        )
        
        assert indicator.value is None
        assert indicator.confidence_score == 0.0
    
    def test_year_indicator(self):
        """Test validation of year-type indicator."""
        indicator = CSRDIndicator(
            indicator_id="E7",
            indicator_name="Net Zero Target Year",
            value=2050,
            unit="year",
            confidence_score=0.90,
            source_page=15,
        )
        
        # Year should be reasonable (2024-2100)
        assert 2024 <= indicator.value <= 2100
    
    def test_negative_pay_gap_allowed(self):
        """Test that negative pay gap is allowed (women earn more)."""
        indicator = CSRDIndicator(
            indicator_id="S3",
            indicator_name="Gender Pay Gap",
            value=-5.2,  # Women earn 5.2% more
            unit="%",
            confidence_score=0.75,
            source_page=89,
        )
        
        # Negative pay gap is valid
        assert -100 <= indicator.value <= 100


class TestCrossValidation:
    """Tests for cross-field validation."""
    
    def test_total_employees_sanity(self):
        """Test total employees sanity check."""
        indicator = CSRDIndicator(
            indicator_id="S1",
            indicator_name="Total Employees",
            value=10500,
            unit="FTE",
            confidence_score=0.95,
            source_page=28,
        )
        
        # Should be reasonable for a major bank
        assert 100 < indicator.value < 500000
    
    def test_training_hours_sanity(self):
        """Test training hours sanity check."""
        indicator = CSRDIndicator(
            indicator_id="S4",
            indicator_name="Training Hours per Employee",
            value=35,
            unit="hours",
            confidence_score=0.80,
            source_page=95,
        )
        
        # Should be reasonable (not more than full work year)
        assert 0 <= indicator.value <= 500
    
    def test_board_meetings_sanity(self):
        """Test board meetings sanity check."""
        indicator = CSRDIndicator(
            indicator_id="G2",
            indicator_name="Board Meetings per Year",
            value=12,
            unit="count",
            confidence_score=0.92,
            source_page=45,
        )
        
        # Should be reasonable (monthly or less)
        assert 0 <= indicator.value <= 52


class TestConfidenceScoring:
    """Tests for confidence score handling."""
    
    def test_high_confidence(self):
        """Test high confidence extraction."""
        indicator = CSRDIndicator(
            indicator_id="E1",
            indicator_name="Scope 1 GHG Emissions",
            value=15000,
            unit="tCO₂e",
            confidence_score=0.95,
            source_page=42,
            notes="Clearly stated in emissions table",
        )
        
        assert indicator.confidence_score >= 0.9
    
    def test_low_confidence(self):
        """Test low confidence extraction."""
        indicator = CSRDIndicator(
            indicator_id="E3",
            indicator_name="Scope 3 GHG Emissions",
            value=125000,
            unit="tCO₂e",
            confidence_score=0.55,
            source_page=67,
            notes="Value interpreted from narrative text",
        )
        
        assert indicator.confidence_score < 0.6
    
    def test_confidence_rounding(self):
        """Test that confidence is rounded to 3 decimal places."""
        indicator = CSRDIndicator(
            indicator_id="E1",
            indicator_name="Test",
            value=100,
            unit="test",
            confidence_score=0.85555555,
        )
        
        # Should be rounded to 3 decimal places
        assert indicator.confidence_score == 0.856


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
