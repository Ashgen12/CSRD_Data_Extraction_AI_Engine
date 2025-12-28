# CSRD Data Extraction Engine

An AI-powered extraction engine for automatically extracting structured sustainability data from CSRD (Corporate Sustainability Reporting Directive) reports.

## 🎯 Results

**Overall Accuracy: 81.7%** (49/60 indicators extracted)

| Bank | Extracted | Accuracy |
|------|-----------|----------|
| AIB  | 17/20     | 85%      |
| BBVA | 18/20     | 90%      |
| BPCE | 14/20     | 70%      |

## 📊 Key Features

- ✅ **AI-Powered Extraction** - Gemini 2.5 Flash for intelligent extraction
- ✅ **60 Data Points** extracted (20 indicators × 3 banks)
- ✅ **Confidence Scoring** (0.0-1.0) for each extraction
- ✅ **Web UI** - FastAPI dashboard with real-time WebSocket updates
- ✅ **Download CSV** - Export results directly from the UI
- ✅ **PostgreSQL** - Structured data storage

## 🚀 Quick Start (Fresh Installation)

### Step 1: Prerequisites

- Python 3.10+ 
- Docker (for PostgreSQL)
- Google Cloud Service Account with Vertex AI access

### Step 2: Clone and Setup Environment

```bash
# Create conda environment
conda create -n csrd_env python=3.11
conda activate csrd_env

# Navigate to project
cd csrd_extractor

# Install dependencies
pip install -r requirements.txt
```

### Step 3: Configure Environment

Create a `.env` file in the project root:

```env
PROJECT_ID=your-gcp-project-id
LOCATION=us-central1
MODEL_NAME=gemini-2.5-flash-preview-05-20
GOOGLE_APPLICATION_CREDENTIALS=path/to/your/service-account.json
DATABASE_URL=postgresql://csrd_user:csrd_secure_pass@localhost:5433/csrd_reports
```

### Step 4: Start PostgreSQL Database

```bash
docker-compose up -d
```

### Step 5: Prepare PDF Reports

Place the CSRD reports in `data/raw/`:
```
data/raw/
├── aib_2024.pdf    # Download from aib.ie
├── bbva_2024.pdf   # Download from bbva.com
└── bpce_2024.pdf   # Download from groupebpce.com
```

### Step 6: Process PDFs (One-time)

```bash
python reextract_pdfs.py
```

This creates markdown files in `data/processed/`.

### Step 7: Run the Application

**Option A: Web UI (Recommended)**
```bash
python app.py
# Open http://localhost:8000
# Click "Start Extraction" to begin
# Click "Download CSV" to export results
```

**Option B: Command Line**
```bash
python run_extraction_v3.py
```

## 📁 Project Structure

```
csrd_extractor/
├── app.py                   # FastAPI Web UI with WebSocket
├── run_extraction_v3.py     # Main extraction script
├── reextract_pdfs.py        # PDF to Markdown converter
├── config/
│   ├── settings.py          # Pydantic settings
│   └── indicators.yaml      # ESG indicator definitions
├── src/
│   ├── models.py            # Pydantic data models
│   └── database_handler.py  # PostgreSQL ORM
├── data/
│   ├── raw/                 # Original PDF reports
│   ├── processed/           # Extracted Markdown pages
│   └── output/              # CSV exports
├── tests/                   # Unit tests
├── docker-compose.yml       # PostgreSQL setup
├── requirements.txt         # Python dependencies
└── .env                     # Environment config
```

## 📋 ESG Indicators Extracted

### Environmental (E1-E8)
| ID | Indicator | Unit |
|----|-----------|------|
| E1 | Scope 1 GHG Emissions | tCO₂e |
| E2 | Scope 2 GHG Emissions | tCO₂e |
| E3 | Scope 3 GHG Emissions | tCO₂e |
| E4 | GHG Emissions Intensity | tCO₂e/€M |
| E5 | Total Energy Consumption | MWh |
| E6 | Renewable Energy % | % |
| E7 | Net Zero Target Year | year |
| E8 | Green Financing Volume | €M |

### Social (S1-S7)
| ID | Indicator | Unit |
|----|-----------|------|
| S1 | Total Employees | FTE |
| S2 | Female Employees % | % |
| S3 | Gender Pay Gap | % |
| S4 | Training Hours/Employee | hours |
| S5 | Employee Turnover Rate | % |
| S6 | Work-Related Accidents | count |
| S7 | Collective Bargaining Coverage | % |

### Governance (G1-G5)
| ID | Indicator | Unit |
|----|-----------|------|
| G1 | Board Female Representation | % |
| G2 | Board Meetings/Year | count |
| G3 | Corruption Incidents | count |
| G4 | Avg Supplier Payment Days | days |
| G5 | Suppliers Screened for ESG | % |

## 📤 Output Format

The CSV output includes:
```csv
company,report_year,indicator_id,indicator_name,value,unit,confidence_score,source_page,notes
AIB,2024,E1,Scope 1 GHG Emissions,2875.0,tCO2e,1.0,78,Found in Table 1...
```

## 🛠️ Technology Stack

| Component | Technology |
|-----------|------------|
| LLM | Vertex AI Gemini 2.5 Flash |
| PDF Parsing | pymupdf4llm |
| Backend | FastAPI + WebSocket |
| Database | PostgreSQL + SQLAlchemy |
| Validation | Pydantic |

## 🔍 Troubleshooting

| Issue | Solution |
|-------|----------|
| "ModuleNotFoundError" | Run `pip install -r requirements.txt` |
| "PostgreSQL connection refused" | Run `docker-compose up -d` |
| "Credentials not found" | Check `.env` file and credentials path |
| "No document found" | Run `python reextract_pdfs.py` first |

## 📈 Performance

- **Extraction Time**: ~10-15 minutes for all 3 banks (60 indicators)
- **Accuracy**: 81.7% (49/60 indicators)
- **Cost**: ~$0.05-0.10 per full extraction (Gemini 2.5 Flash)

---

**Built for CSRD compliance automation** 🌱
