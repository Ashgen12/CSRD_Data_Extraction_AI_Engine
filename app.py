"""
CSRD Data Extraction Engine - FastAPI Web Application

Professional UI for monitoring extraction progress and viewing statistics.
"""
import asyncio
import json
import subprocess
import sys
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any
from contextlib import asynccontextmanager
import threading
import queue
import traceback

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from config.settings import settings
from src.database_handler import DatabaseHandler

# ============================================================================
# Global State
# ============================================================================

class ExtractionState:
    """Manages extraction state and WebSocket connections."""
    
    def __init__(self):
        self.is_running = False
        self.current_bank = None
        self.current_indicator = None
        self.progress = 0
        self.total_indicators = 60  # 20 per bank * 3 banks
        self.logs: List[str] = []
        self.results: Dict[str, Any] = {}
        self.connections: List[WebSocket] = []
        self.log_queue = queue.Queue()
        
    async def broadcast(self, message: dict):
        """Broadcast message to all connected WebSocket clients."""
        disconnected = []
        for ws in self.connections:
            try:
                await ws.send_json(message)
            except:
                disconnected.append(ws)
        for ws in disconnected:
            self.connections.remove(ws)


state = ExtractionState()


# ============================================================================
# FastAPI Application
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    print("🚀 CSRD Extraction Engine starting...")
    yield
    print("👋 Shutting down...")


app = FastAPI(
    title="CSRD Extraction Engine",
    description="AI-powered ESG data extraction from CSRD reports",
    version="3.0.0",
    lifespan=lifespan
)


# ============================================================================
# HTML Template - Professional Minimalistic UI
# ============================================================================

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CSRD Extraction Engine</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-primary: #0a0a0b;
            --bg-secondary: #111113;
            --bg-tertiary: #1a1a1d;
            --bg-card: #16161a;
            --text-primary: #ffffff;
            --text-secondary: #a1a1aa;
            --text-muted: #71717a;
            --accent: #3b82f6;
            --accent-hover: #2563eb;
            --success: #22c55e;
            --warning: #f59e0b;
            --error: #ef4444;
            --border: #27272a;
            --shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.3);
        }
        
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            min-height: 100vh;
            line-height: 1.6;
        }
        
        /* Header */
        header {
            background: var(--bg-secondary);
            border-bottom: 1px solid var(--border);
            padding: 1.5rem 2rem;
            position: sticky;
            top: 0;
            z-index: 100;
        }
        
        .header-content {
            max-width: 1400px;
            margin: 0 auto;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        
        .logo {
            display: flex;
            align-items: center;
            gap: 0.75rem;
        }
        
        .logo-icon {
            width: 40px;
            height: 40px;
            background: linear-gradient(135deg, var(--accent), #8b5cf6);
            border-radius: 10px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 1.25rem;
        }
        
        .logo h1 {
            font-size: 1.25rem;
            font-weight: 600;
            letter-spacing: -0.5px;
        }
        
        .logo span {
            color: var(--text-muted);
            font-size: 0.75rem;
            font-weight: 400;
        }
        
        .status-badge {
            display: flex;
            align-items: center;
            gap: 0.5rem;
            padding: 0.5rem 1rem;
            background: var(--bg-tertiary);
            border-radius: 9999px;
            font-size: 0.875rem;
        }
        
        .status-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: var(--success);
            animation: pulse 2s infinite;
        }
        
        .status-dot.idle {
            background: var(--text-muted);
            animation: none;
        }
        
        .status-dot.running {
            background: var(--accent);
        }
        
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }
        
        /* Main Content */
        main {
            max-width: 1400px;
            margin: 0 auto;
            padding: 2rem;
        }
        
        /* Hero Section */
        .hero {
            text-align: center;
            padding: 3rem 0;
            margin-bottom: 2rem;
        }
        
        .hero h2 {
            font-size: 2.5rem;
            font-weight: 700;
            letter-spacing: -1px;
            margin-bottom: 1rem;
            background: linear-gradient(135deg, #fff, #a1a1aa);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }
        
        .hero p {
            color: var(--text-secondary);
            font-size: 1.125rem;
            max-width: 600px;
            margin: 0 auto 2rem;
        }
        
        .hero-features {
            display: flex;
            justify-content: center;
            gap: 2rem;
            flex-wrap: wrap;
        }
        
        .feature {
            display: flex;
            align-items: center;
            gap: 0.5rem;
            color: var(--text-muted);
            font-size: 0.875rem;
        }
        
        .feature-icon {
            width: 24px;
            height: 24px;
            background: var(--bg-tertiary);
            border-radius: 6px;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        
        /* Cards Grid */
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 1.5rem;
            margin-bottom: 2rem;
        }
        
        .card {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 16px;
            padding: 1.5rem;
            transition: transform 0.2s, box-shadow 0.2s;
        }
        
        .card:hover {
            transform: translateY(-2px);
            box-shadow: var(--shadow);
        }
        
        .card-header {
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 1rem;
        }
        
        .card-title {
            font-size: 0.875rem;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .card-value {
            font-size: 2.5rem;
            font-weight: 700;
            letter-spacing: -1px;
        }
        
        .card-value.success { color: var(--success); }
        .card-value.warning { color: var(--warning); }
        .card-value.accent { color: var(--accent); }
        
        .card-subtitle {
            font-size: 0.875rem;
            color: var(--text-secondary);
            margin-top: 0.5rem;
        }
        
        /* Progress Section */
        .progress-section {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 16px;
            padding: 2rem;
            margin-bottom: 2rem;
        }
        
        .progress-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 1.5rem;
        }
        
        .progress-title {
            font-size: 1.25rem;
            font-weight: 600;
        }
        
        .progress-bar-container {
            background: var(--bg-tertiary);
            border-radius: 9999px;
            height: 12px;
            overflow: hidden;
            margin-bottom: 1rem;
        }
        
        .progress-bar {
            background: linear-gradient(90deg, var(--accent), #8b5cf6);
            height: 100%;
            border-radius: 9999px;
            transition: width 0.5s ease;
        }
        
        .progress-info {
            display: flex;
            justify-content: space-between;
            color: var(--text-secondary);
            font-size: 0.875rem;
        }
        
        /* Bank Cards */
        .bank-grid {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 1rem;
            margin-top: 1.5rem;
        }
        
        @media (max-width: 768px) {
            .bank-grid {
                grid-template-columns: 1fr;
            }
        }
        
        .bank-card {
            background: var(--bg-tertiary);
            border-radius: 12px;
            padding: 1.25rem;
            position: relative;
            overflow: hidden;
        }
        
        .bank-card.active {
            border: 2px solid var(--accent);
        }
        
        .bank-card.completed {
            border: 2px solid var(--success);
        }
        
        .bank-name {
            font-weight: 600;
            margin-bottom: 0.5rem;
        }
        
        .bank-stats {
            font-size: 0.875rem;
            color: var(--text-secondary);
        }
        
        .bank-accuracy {
            font-size: 1.5rem;
            font-weight: 700;
            margin-top: 0.5rem;
        }
        
        /* Action Button */
        .action-btn {
            background: linear-gradient(135deg, var(--accent), #8b5cf6);
            color: white;
            border: none;
            padding: 1rem 2rem;
            font-size: 1rem;
            font-weight: 600;
            border-radius: 12px;
            cursor: pointer;
            transition: transform 0.2s, opacity 0.2s;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }
        
        .action-btn:hover:not(:disabled) {
            transform: translateY(-2px);
        }
        
        .action-btn:disabled {
            opacity: 0.5;
            cursor: not-allowed;
        }
        
        /* Log Console */
        .console {
            background: var(--bg-secondary);
            border: 1px solid var(--border);
            border-radius: 16px;
            overflow: hidden;
        }
        
        .console-header {
            background: var(--bg-tertiary);
            padding: 1rem 1.5rem;
            display: flex;
            align-items: center;
            gap: 0.5rem;
            border-bottom: 1px solid var(--border);
        }
        
        .console-dot {
            width: 12px;
            height: 12px;
            border-radius: 50%;
        }
        
        .console-dot.red { background: #ef4444; }
        .console-dot.yellow { background: #f59e0b; }
        .console-dot.green { background: #22c55e; }
        
        .console-body {
            padding: 1.5rem;
            height: 300px;
            overflow-y: auto;
            font-family: 'JetBrains Mono', 'Fira Code', monospace;
            font-size: 0.8125rem;
            line-height: 1.8;
        }
        
        .log-entry {
            color: var(--text-secondary);
            padding: 0.25rem 0;
        }
        
        .log-entry.success { color: var(--success); }
        .log-entry.error { color: var(--error); }
        .log-entry.info { color: var(--accent); }
        .log-entry.highlight { color: var(--warning); }
        
        /* Footer */
        footer {
            text-align: center;
            padding: 2rem;
            color: var(--text-muted);
            font-size: 0.875rem;
            border-top: 1px solid var(--border);
            margin-top: 2rem;
        }
        
        /* Animations */
        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(10px); }
            to { opacity: 1; transform: translateY(0); }
        }
        
        .animate-in {
            animation: fadeIn 0.5s ease forwards;
        }
        
        /* Spinner */
        .spinner {
            width: 20px;
            height: 20px;
            border: 2px solid transparent;
            border-top-color: currentColor;
            border-radius: 50%;
            animation: spin 1s linear infinite;
        }
        
        @keyframes spin {
            to { transform: rotate(360deg); }
        }
    </style>
</head>
<body>
    <header>
        <div class="header-content">
            <div class="logo">
                <div class="logo-icon">📊</div>
                <div>
                    <h1>CSRD Extraction Engine</h1>
                    <span>ESG Analytics</span>
                </div>
            </div>
            <div class="status-badge">
                <div class="status-dot idle" id="statusDot"></div>
                <span id="statusText">Ready</span>
            </div>
        </div>
    </header>
    
    <main>
        <!-- Hero Section -->
        <section class="hero animate-in">
            <h2>ESG Data Extraction</h2>
            <p>Extract 20 key ESG indicators from CSRD sustainability reports using AI-powered regex and LLM extraction</p>
            <div class="hero-features">
                <div class="feature">
                    <div class="feature-icon">📄</div>
                    <span>PDF Processing</span>
                </div>
                <div class="feature">
                    <div class="feature-icon">🔍</div>
                    <span>Smart Extraction</span>
                </div>
                <div class="feature">
                    <div class="feature-icon">🤖</div>
                    <span>Gemini 2.5 Flash</span>
                </div>
                <div class="feature">
                    <div class="feature-icon">📊</div>
                    <span>20 Indicators</span>
                </div>
            </div>
        </section>
        
        <!-- Stats Cards -->
        <div class="grid">
            <div class="card animate-in" style="animation-delay: 0.1s">
                <div class="card-header">
                    <span class="card-title">Total Indicators</span>
                    <span>📊</span>
                </div>
                <div class="card-value accent" id="totalFound">--</div>
                <div class="card-subtitle">of 60 extracted</div>
            </div>
            <div class="card animate-in" style="animation-delay: 0.2s">
                <div class="card-header">
                    <span class="card-title">Accuracy</span>
                    <span>🎯</span>
                </div>
                <div class="card-value success" id="accuracy">--%</div>
                <div class="card-subtitle">extraction rate</div>
            </div>
            <div class="card animate-in" style="animation-delay: 0.3s">
                <div class="card-header">
                    <span class="card-title">High Confidence</span>
                    <span>💎</span>
                </div>
                <div class="card-value warning" id="highConf">--</div>
                <div class="card-subtitle">confidence ≥ 0.7</div>
            </div>
            <div class="card animate-in" style="animation-delay: 0.4s">
                <div class="card-header">
                    <span class="card-title">Last Run</span>
                    <span>🕐</span>
                </div>
                <div class="card-value" style="font-size: 1.25rem" id="lastRun">Never</div>
                <div class="card-subtitle" id="lastDuration">--</div>
            </div>
        </div>
        
        <!-- Progress Section -->
        <section class="progress-section animate-in" style="animation-delay: 0.5s">
            <div class="progress-header">
                <h3 class="progress-title">Extraction Progress</h3>
                <div style="display: flex; gap: 1rem;">
                    <button class="action-btn" id="downloadBtn" onclick="downloadCSV()" style="background: linear-gradient(135deg, #22c55e, #16a34a);">
                        <span>⬇</span> Download CSV
                    </button>
                    <button class="action-btn" id="runBtn" onclick="startExtraction()">
                        <span>▶</span> Start Extraction
                    </button>
                </div>
            </div>
            <div class="progress-bar-container">
                <div class="progress-bar" id="progressBar" style="width: 0%"></div>
            </div>
            <div class="progress-info">
                <span id="currentTask">Waiting to start...</span>
                <span id="progressPercent">0%</span>
            </div>
            
            <!-- Bank Progress Cards -->
            <div class="bank-grid">
                <div class="bank-card" id="aibCard">
                    <div class="bank-name">🇮🇪 AIB Group</div>
                    <div class="bank-stats" id="aibStats">-- / 20 indicators</div>
                    <div class="bank-accuracy" id="aibAccuracy">--%</div>
                </div>
                <div class="bank-card" id="bbvaCard">
                    <div class="bank-name">🇪🇸 BBVA</div>
                    <div class="bank-stats" id="bbvaStats">-- / 20 indicators</div>
                    <div class="bank-accuracy" id="bbvaAccuracy">--%</div>
                </div>
                <div class="bank-card" id="bpceCard">
                    <div class="bank-name">🇫🇷 BPCE</div>
                    <div class="bank-stats" id="bpceStats">-- / 20 indicators</div>
                    <div class="bank-accuracy" id="bpceAccuracy">--%</div>
                </div>
            </div>
        </section>
        
        <!-- Console -->
        <section class="console animate-in" style="animation-delay: 0.6s">
            <div class="console-header">
                <div class="console-dot red"></div>
                <div class="console-dot yellow"></div>
                <div class="console-dot green"></div>
                <span style="margin-left: 0.5rem; color: var(--text-muted); font-size: 0.875rem">Extraction Logs</span>
            </div>
            <div class="console-body" id="logConsole">
                <div class="log-entry info">Welcome to CSRD Extraction Engine v3.0</div>
                <div class="log-entry">Ready to extract ESG indicators from CSRD reports.</div>
                <div class="log-entry">Click "Start Extraction" to begin...</div>
            </div>
        </section>
    </main>
    
    <footer>
        CSRD Extraction Engine • Powered by Gemini 2.5 Flash • Built with FastAPI
    </footer>
    
    <script>
        let ws = null;
        let isRunning = false;
        
        // Initialize WebSocket
        function connectWebSocket() {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            ws = new WebSocket(`${protocol}//${window.location.host}/ws`);
            
            ws.onmessage = (event) => {
                const data = JSON.parse(event.data);
                handleMessage(data);
            };
            
            ws.onclose = () => {
                setTimeout(connectWebSocket, 3000);
            };
        }
        
        function handleMessage(data) {
            switch(data.type) {
                case 'status':
                    updateStatus(data);
                    break;
                case 'progress':
                    updateProgress(data);
                    break;
                case 'log':
                    addLog(data.message, data.level);
                    break;
                case 'bank_complete':
                    updateBankCard(data);
                    break;
                case 'complete':
                    extractionComplete(data);
                    break;
                case 'stats':
                    updateStats(data);
                    break;
            }
        }
        
        function updateStatus(data) {
            const dot = document.getElementById('statusDot');
            const text = document.getElementById('statusText');
            
            dot.className = 'status-dot ' + (data.running ? 'running' : 'idle');
            text.textContent = data.running ? 'Running' : 'Ready';
            isRunning = data.running;
            
            document.getElementById('runBtn').disabled = isRunning;
            document.getElementById('runBtn').innerHTML = isRunning 
                ? '<div class="spinner"></div> Extracting...' 
                : '<span>▶</span> Start Extraction';
        }
        
        function updateProgress(data) {
            const percent = Math.round((data.current / data.total) * 100);
            document.getElementById('progressBar').style.width = percent + '%';
            document.getElementById('progressPercent').textContent = percent + '%';
            document.getElementById('currentTask').textContent = data.task || 'Processing...';
            
            // Highlight active bank
            ['aib', 'bbva', 'bpce'].forEach(bank => {
                const card = document.getElementById(bank + 'Card');
                card.classList.remove('active');
            });
            
            if (data.bank) {
                const bankCard = document.getElementById(data.bank.toLowerCase() + 'Card');
                if (bankCard) bankCard.classList.add('active');
            }
        }
        
        function addLog(message, level = 'info') {
            const console = document.getElementById('logConsole');
            const entry = document.createElement('div');
            entry.className = 'log-entry ' + (level || '');
            entry.textContent = `[${new Date().toLocaleTimeString()}] ${message}`;
            console.appendChild(entry);
            console.scrollTop = console.scrollHeight;
        }
        
        function updateBankCard(data) {
            const card = document.getElementById(data.bank.toLowerCase() + 'Card');
            card.classList.remove('active');
            card.classList.add('completed');
            
            document.getElementById(data.bank.toLowerCase() + 'Stats').textContent = 
                `${data.found} / 20 indicators`;
            document.getElementById(data.bank.toLowerCase() + 'Accuracy').textContent = 
                `${data.accuracy}%`;
        }
        
        function extractionComplete(data) {
            isRunning = false;
            document.getElementById('statusDot').className = 'status-dot idle';
            document.getElementById('statusText').textContent = 'Completed';
            document.getElementById('runBtn').disabled = false;
            document.getElementById('runBtn').innerHTML = '<span>▶</span> Start Extraction';
            
            updateStats(data);
            addLog(`✅ Extraction complete! Total: ${data.total_found}/60 (${data.accuracy}%)`, 'success');
        }
        
        function updateStats(data) {
            document.getElementById('totalFound').textContent = data.total_found || '--';
            document.getElementById('accuracy').textContent = (data.accuracy || '--') + '%';
            document.getElementById('highConf').textContent = data.high_conf || '--';
            
            if (data.last_run) {
                document.getElementById('lastRun').textContent = data.last_run;
            }
            if (data.duration) {
                document.getElementById('lastDuration').textContent = `Duration: ${data.duration}`;
            }
        }
        
        async function startExtraction() {
            if (isRunning) return;
            
            // Clear console
            const console = document.getElementById('logConsole');
            console.innerHTML = '';
            addLog('Starting extraction...', 'info');
            
            // Reset bank cards
            ['aib', 'bbva', 'bpce'].forEach(bank => {
                const card = document.getElementById(bank + 'Card');
                card.classList.remove('active', 'completed');
                document.getElementById(bank + 'Stats').textContent = '-- / 20 indicators';
                document.getElementById(bank + 'Accuracy').textContent = '--%';
            });
            
            try {
                const response = await fetch('/api/extract', { method: 'POST' });
                const data = await response.json();
                
                if (!data.success) {
                    addLog('Error: ' + data.error, 'error');
                }
            } catch (error) {
                addLog('Failed to start extraction: ' + error.message, 'error');
            }
        }
        
        // Load initial stats
        async function loadStats() {
            try {
                const response = await fetch('/api/stats');
                const data = await response.json();
                updateStats(data);
                
                // Update bank cards
                if (data.banks) {
                    data.banks.forEach(bank => {
                        const bankName = bank.name.toLowerCase();
                        document.getElementById(bankName + 'Stats').textContent = 
                            `${bank.found} / 20 indicators`;
                        document.getElementById(bankName + 'Accuracy').textContent = 
                            `${bank.accuracy}%`;
                        if (bank.found > 0) {
                            document.getElementById(bankName + 'Card').classList.add('completed');
                        }
                    });
                }
            } catch (error) {
                console.error('Failed to load stats:', error);
            }
        }
        
        // Download CSV file
        function downloadCSV() {
            window.location.href = '/api/download';
        }
        
        // Initialize
        connectWebSocket();
        loadStats();
    </script>
</body>
</html>
"""


# ============================================================================
# API Endpoints
# ============================================================================

@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve the main dashboard."""
    return HTML_TEMPLATE


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket for real-time updates."""
    await websocket.accept()
    state.connections.append(websocket)
    
    # Send initial status
    await websocket.send_json({
        "type": "status",
        "running": state.is_running
    })
    
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        if websocket in state.connections:
            state.connections.remove(websocket)


@app.get("/api/stats")
async def get_stats():
    """Get extraction statistics from database and CSV."""
    try:
        # Try to read from latest CSV
        csv_path = Path(settings.data_output_dir) / "extracted_indicators_v3.csv"
        
        stats = {
            "total_found": 0,
            "accuracy": 0,
            "high_conf": 0,
            "last_run": None,
            "duration": None,
            "banks": []
        }
        
        if csv_path.exists():
            import csv
            
            bank_data = {"aib": [], "bbva": [], "bpce": []}
            
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    bank = row.get('bank', '').lower()
                    if bank in bank_data:
                        bank_data[bank].append(row)
            
            total_found = 0
            total_high_conf = 0
            
            for bank_name, rows in bank_data.items():
                found = sum(1 for r in rows if r.get('value') and r.get('value') != 'N/A')
                high_conf = sum(1 for r in rows if float(r.get('confidence', 0) or 0) >= 0.7)
                accuracy = round(found / 20 * 100, 1) if rows else 0
                
                total_found += found
                total_high_conf += high_conf
                
                stats["banks"].append({
                    "name": bank_name.upper(),
                    "found": found,
                    "accuracy": accuracy,
                    "high_conf": high_conf
                })
            
            stats["total_found"] = total_found
            stats["accuracy"] = round(total_found / 60 * 100, 1)
            stats["high_conf"] = total_high_conf
            
            # Get last modified time
            mtime = csv_path.stat().st_mtime
            stats["last_run"] = datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M")
        
        return stats
        
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/download")
async def download_csv():
    """Download the extracted indicators CSV file."""
    csv_path = Path(settings.data_output_dir) / "extracted_indicators_v3.csv"
    
    if not csv_path.exists():
        raise HTTPException(status_code=404, detail="CSV file not found. Run extraction first.")
    
    return FileResponse(
        path=str(csv_path),
        media_type="text/csv",
        filename="csrd_extracted_indicators.csv"
    )


@app.post("/api/extract")
async def start_extraction():
    """Start the extraction process."""
    if state.is_running:
        return {"success": False, "error": "Extraction already running"}
    
    state.is_running = True
    
    # Broadcast status
    await state.broadcast({
        "type": "status",
        "running": True
    })
    
    # Run extraction in background
    asyncio.create_task(run_extraction())
    
    return {"success": True, "message": "Extraction started"}


async def run_extraction():
    """Run the extraction script and stream progress using threading (Windows compatible)."""
    
    def run_subprocess():
        """Run the subprocess in a separate thread."""
        script_path = Path(__file__).parent / "run_extraction_v3.py"
        
        try:
            # Use subprocess.Popen which works on Windows
            process = subprocess.Popen(
                [sys.executable, str(script_path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                cwd=str(Path(__file__).parent),
                bufsize=1,
                universal_newlines=True,
                encoding='utf-8',
                errors='ignore'
            )
            
            current_bank = None
            indicator_count = 0
            bank_results = {}
            
            # Read output line by line
            for line in process.stdout:
                text = line.strip()
                if not text:
                    continue
                
                # Put logs in queue for async processing
                state.log_queue.put(("log", text))
                
                # Parse log line for progress
                if "PROCESSING:" in text:
                    if "AIB" in text:
                        current_bank = "AIB"
                    elif "BBVA" in text:
                        current_bank = "BBVA"
                    elif "BPCE" in text:
                        current_bank = "BPCE"
                    state.log_queue.put(("bank_start", current_bank))
                
                if "Extracting" in text:
                    indicator_count += 1
                    indicator_name = text.split("Extracting")[-1].strip().rstrip("...")
                    state.log_queue.put(("progress", {
                        "current": indicator_count,
                        "total": 60,
                        "task": f"Extracting {indicator_name}",
                        "bank": current_bank
                    }))
                
                if "Values found:" in text and current_bank:
                    try:
                        parts = text.split("Values found:")[-1].strip()
                        found_str = parts.split("/")[0].strip()
                        found = int(found_str)
                        accuracy = round(found / 20 * 100, 1)
                        bank_results[current_bank] = {"found": found, "accuracy": accuracy}
                        state.log_queue.put(("bank_complete", {
                            "bank": current_bank,
                            "found": found,
                            "accuracy": accuracy
                        }))
                    except:
                        pass
            
            process.wait()
            
            # Calculate totals
            total_found = sum(b.get("found", 0) for b in bank_results.values())
            total_accuracy = round(total_found / 60 * 100, 1) if total_found else 0
            
            state.log_queue.put(("complete", {
                "total_found": total_found,
                "accuracy": total_accuracy,
                "high_conf": total_found,
                "banks": [{"name": k, **v} for k, v in bank_results.items()]
            }))
            
        except Exception as e:
            state.log_queue.put(("error", str(e)))
        finally:
            state.log_queue.put(("done", None))
    
    # Start subprocess in thread
    await state.broadcast({
        "type": "log",
        "message": "Starting V3 High-Accuracy Extraction Pipeline...",
        "level": "info"
    })
    await state.broadcast({
        "type": "log",
        "message": "Using Regex + LLM extraction with targeted context retrieval",
        "level": "info"
    })
    
    # Start the extraction thread
    extraction_thread = threading.Thread(target=run_subprocess, daemon=True)
    extraction_thread.start()
    
    # Process queue messages asynchronously
    try:
        while True:
            await asyncio.sleep(0.1)  # Small delay to prevent busy loop
            
            while not state.log_queue.empty():
                msg_type, data = state.log_queue.get_nowait()
                
                if msg_type == "done":
                    return  # Exit the function
                elif msg_type == "error":
                    await state.broadcast({"type": "log", "message": f"Error: {data}", "level": "error"})
                elif msg_type == "log":
                    log_level = ""
                    if "[OK]" in data:
                        log_level = "success"
                    elif "Not found" in data or "failed" in data.lower():
                        log_level = "error"
                    elif "PROCESSING:" in data:
                        log_level = "highlight"
                    await state.broadcast({"type": "log", "message": data, "level": log_level})
                elif msg_type == "progress":
                    await state.broadcast({"type": "progress", **data})
                elif msg_type == "bank_start":
                    await state.broadcast({"type": "log", "message": f"Processing {data}...", "level": "info"})
                elif msg_type == "bank_complete":
                    await state.broadcast({"type": "bank_complete", **data})
                elif msg_type == "complete":
                    await state.broadcast({"type": "complete", **data})
                    await state.broadcast({"type": "log", "message": f"Extraction complete! Total: {data['total_found']}/60 ({data['accuracy']}%)", "level": "success"})
                    return
            
            # Check if thread is still alive
            if not extraction_thread.is_alive():
                break
                
    except Exception as e:
        error_trace = traceback.format_exc()
        print(f"Extraction error: {error_trace}")
        await state.broadcast({"type": "log", "message": f"Error: {str(e)}", "level": "error"})
    finally:
        state.is_running = False
        await state.broadcast({"type": "status", "running": False})


# ============================================================================
# Entry Point
# ============================================================================

if __name__ == "__main__":
    print("""
    ================================================================
    |        CSRD EXTRACTION ENGINE - Web Interface                |
    |                                                              |
    |   Open in browser: http://localhost:8000                     |
    ================================================================
    """)
    
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
