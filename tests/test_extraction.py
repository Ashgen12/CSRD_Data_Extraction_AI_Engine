"""
CSRD Data Extraction Engine - Extraction Tests

Unit tests for the extraction engine and related components.
"""
import pytest
from pathlib import Path
import sys

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models import (
    CSRDIndicator,
    EnvironmentalData,
    SocialData,
    GovernanceData,
    BankExtractionResult,
)


class TestCSRDIndicator:
    """Tests for CSRDIndicator model."""
    
    def test_valid_indicator(self):
        """Test creating a valid indicator."""
        indicator = CSRDIndicator(
            indicator_id="E1",
            indicator_name="Scope 1 GHG Emissions",
            value=15000,
            unit="tCO₂e",
            confidence_score=0.85,
            source_page=42,
        )
        
        assert indicator.indicator_id == "E1"
        assert indicator.value == 15000
        assert indicator.confidence_score == 0.85
    
    def test_null_value_allowed(self):
        """Test that null values are allowed."""
        indicator = CSRDIndicator(
            indicator_id="E1",
            indicator_name="Scope 1 GHG Emissions",
            value=None,
            unit="tCO₂e",
            confidence_score=0.0,
        )
        
        assert indicator.value is None
    
    def test_confidence_validation(self):
        """Test confidence score validation."""
        # Valid confidence
        indicator = CSRDIndicator(
            indicator_id="E1",
            indicator_name="Test",
            value=100,
            unit="test",
            confidence_score=0.5,
        )
        assert indicator.confidence_score == 0.5
        
        # Invalid confidence (too high)
        with pytest.raises(ValueError):
            CSRDIndicator(
                indicator_id="E1",
                indicator_name="Test",
                value=100,
                unit="test",
                confidence_score=1.5,
            )
        
        # Invalid confidence (negative)
        with pytest.raises(ValueError):
            CSRDIndicator(
                indicator_id="E1",
                indicator_name="Test",
                value=100,
                unit="test",
                confidence_score=-0.1,
            )


class TestEnvironmentalData:
    """Tests for EnvironmentalData model."""
    
    def test_valid_environmental_data(self):
        """Test creating valid environmental data."""
        data = EnvironmentalData(
            scope_1_emissions=15000,
            scope_2_emissions=8500,
            scope_3_emissions=125000,
            renewable_energy_percentage=67,
            net_zero_target_year=2050,
        )
        
        assert data.scope_1_emissions == 15000
        assert data.renewable_energy_percentage == 67
    
    def test_percentage_validation(self):
        """Test renewable energy percentage validation."""
        # Valid percentage
        data = EnvironmentalData(renewable_energy_percentage=50)
        assert data.renewable_energy_percentage == 50
        
        # Invalid percentage (> 100)
        with pytest.raises(ValueError):
            EnvironmentalData(renewable_energy_percentage=150)
        
        # Invalid percentage (< 0)
        with pytest.raises(ValueError):
            EnvironmentalData(renewable_energy_percentage=-10)
    
    def test_emissions_non_negative(self):
        """Test that emissions must be non-negative."""
        with pytest.raises(ValueError):
            EnvironmentalData(scope_1_emissions=-1000)


class TestSocialData:
    """Tests for SocialData model."""
    
    def test_valid_social_data(self):
        """Test creating valid social data."""
        data = SocialData(
            total_employees=10500,
            female_employees_percentage=45,
            training_hours_per_employee=35,
            employee_turnover_rate=8.5,
        )
        
        assert data.total_employees == 10500
        assert data.female_employees_percentage == 45
    
    def test_percentage_fields(self):
        """Test percentage field validation."""
        # Valid percentages
        data = SocialData(
            female_employees_percentage=50,
            employee_turnover_rate=15,
            collective_bargaining_coverage=80,
        )
        assert data.female_employees_percentage == 50
        
        # Invalid turnover rate
        with pytest.raises(ValueError):
            SocialData(employee_turnover_rate=150)


class TestGovernanceData:
    """Tests for GovernanceData model."""
    
    def test_valid_governance_data(self):
        """Test creating valid governance data."""
        data = GovernanceData(
            board_female_percentage=40,
            board_meetings_per_year=12,
            corruption_incidents=0,
            avg_supplier_payment_days=30,
        )
        
        assert data.board_female_percentage == 40
        assert data.corruption_incidents == 0
    
    def test_board_percentage_validation(self):
        """Test board percentage validation."""
        with pytest.raises(ValueError):
            GovernanceData(board_female_percentage=110)


class TestBankExtractionResult:
    """Tests for BankExtractionResult model."""
    
    def test_create_extraction_result(self):
        """Test creating an extraction result."""
        result = BankExtractionResult(
            company="AIB",
            report_year=2024,
            pdf_filename="aib_2024.pdf",
            total_pages=200,
        )
        
        assert result.company == "AIB"
        assert result.report_year == 2024
        assert result.indicators == []
    
    def test_calculate_metrics(self):
        """Test metric calculation."""
        result = BankExtractionResult(
            company="AIB",
            report_year=2024,
            pdf_filename="aib_2024.pdf",
            indicators=[
                CSRDIndicator(
                    indicator_id="E1",
                    indicator_name="Test",
                    value=100,
                    unit="test",
                    confidence_score=0.8,
                ),
                CSRDIndicator(
                    indicator_id="E2",
                    indicator_name="Test2",
                    value=200,
                    unit="test",
                    confidence_score=0.6,
                ),
            ],
        )
        
        result.calculate_metrics(threshold=0.65)
        
        assert result.avg_confidence == 0.7
        assert result.low_confidence_count == 1
    
    def test_to_csv_rows(self):
        """Test CSV row generation."""
        result = BankExtractionResult(
            company="AIB",
            report_year=2024,
            pdf_filename="aib_2024.pdf",
            indicators=[
                CSRDIndicator(
                    indicator_id="E1",
                    indicator_name="Scope 1",
                    value=15000,
                    unit="tCO₂e",
                    confidence_score=0.85,
                    source_page=42,
                ),
            ],
        )
        
        rows = result.to_csv_rows()
        
        assert len(rows) == 1
        assert rows[0]["company"] == "AIB"
        assert rows[0]["indicator_id"] == "E1"
        assert rows[0]["value"] == 15000


class TestValidation:
    """Tests for validation logic."""
    
    def test_scope_2_validation(self):
        """Test Scope 2 location vs market-based validation."""
        # Normal case: market-based < location-based
        data = EnvironmentalData(
            scope_2_location_based=10000,
            scope_2_market_based=5000,
        )
        # Should pass without error
        assert data.scope_2_market_based < data.scope_2_location_based


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
