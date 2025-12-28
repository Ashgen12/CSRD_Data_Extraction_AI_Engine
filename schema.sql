-- CSRD Extraction Engine Database Schema
-- PostgreSQL

-- Main table for extracted indicators
CREATE TABLE IF NOT EXISTS sustainability_indicators (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company VARCHAR(100) NOT NULL,
    report_year INTEGER NOT NULL,
    indicator_id VARCHAR(20) NOT NULL,
    indicator_name VARCHAR(200) NOT NULL,
    value FLOAT,
    unit VARCHAR(50),
    confidence_score FLOAT CHECK (confidence_score >= 0 AND confidence_score <= 1),
    source_page INTEGER,
    source_section VARCHAR(200),
    notes TEXT,
    extraction_method VARCHAR(50) DEFAULT 'llm',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Ensure unique combination per bank/year/indicator
    CONSTRAINT unique_indicator UNIQUE (company, report_year, indicator_id)
);

-- Index for faster queries
CREATE INDEX IF NOT EXISTS idx_company ON sustainability_indicators(company);
CREATE INDEX IF NOT EXISTS idx_indicator_id ON sustainability_indicators(indicator_id);
CREATE INDEX IF NOT EXISTS idx_report_year ON sustainability_indicators(report_year);

-- View for summary statistics
CREATE OR REPLACE VIEW extraction_summary AS
SELECT 
    company,
    report_year,
    COUNT(*) as total_indicators,
    COUNT(value) as extracted_count,
    ROUND(AVG(confidence_score)::numeric, 2) as avg_confidence,
    ROUND((COUNT(value)::float / COUNT(*)::float * 100)::numeric, 1) as accuracy_percent
FROM sustainability_indicators
GROUP BY company, report_year
ORDER BY company, report_year;

-- Sample query to view all data
-- SELECT * FROM sustainability_indicators ORDER BY company, indicator_id;
