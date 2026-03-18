# 🐋 ORCA — Options Research & Catalyst Analyzer

Automated options scanner that runs daily in the cloud (free), builds its own IV database over time, and sends trade alerts to your phone via Telegram.

## What It Does

Every weekday at **8:30 AM ET** (before market open), ORCA runs a 2-stage pipeline:

### Stage 1: Technical Scanner (orca.py)
1. Scans **120+ tickers** across tech, semis, energy, defense, tankers, crypto, gold
2. Calculates **real historical volatility** (Close-Close, Parkinson, Yang-Zhang)
3. Pulls **live options chains** from Yahoo Finance (free, 15-min delay)
4. Compares **implied vol vs realized vol** to find mispriced premium
5. Checks **earnings** implied move vs historical move
6. Flags **unusual options activity** (volume/OI spikes, put/call skew)
7. Ranks everything by **expected value**
8. Saves results + **builds IV database** (gets smarter every day)

### Stage 2: Analyst — Claude AI (analyst.py)
9. Scrapes **30+ financial news feeds** (Reuters, CNBC, Bloomberg, MarketWatch)
10. Filters for **market-moving headlines** using keyword scoring
11. Sends news + ORCA technical data to **Claude API**
12. Claude finds **second and third-order effects** the market hasn't priced
13. Returns **specific trade recommendations** with thesis, entry, edge
14. Sends **final plays to your Telegram**

### The Edge
The scanner finds WHERE options are mispriced (IV vs HV).
The analyst finds WHY they're mispriced (news → cause-effect chains).
Together: "STNG IV is only 45% but Strait of Hormuz just closed → buy calls."

## Setup (10 minutes)

### Step 1: Create the GitHub Repo

```bash
# Create a new private repo on GitHub, then:
git clone https://github.com/YOUR_USERNAME/orca-bot.git
cd orca-bot

# Copy all files into it
# (orca.py, notify.py, .github/workflows/orca_daily.yml)

git add -A
git commit -m "Initial ORCA setup"
git push
```

### Step 2: Set Up Claude API Key (powers the analyst brain)

1. Go to https://console.anthropic.com → **API Keys** → Create key
2. In your GitHub repo → **Settings** → **Secrets and variables** → **Actions**
3. Add **Repository Secret**:
   - `ANTHROPIC_API_KEY` = your key

Cost: ~$0.01-0.03 per daily scan (one Sonnet call with news + ORCA data).
That's about **$0.50/month** for daily AI-powered trade analysis.

### Step 3: Set Up Telegram Alerts (optional but recommended)

1. Open Telegram, search for **@BotFather**
2. Send `/newbot`, name it "ORCA Alerts" or whatever you like
3. Copy the **bot token** it gives you
4. Search for **@userinfobot**, send `/start` — copy your **chat ID**
5. In your GitHub repo → **Settings** → **Secrets and variables** → **Actions**
6. Add two **Repository Secrets**:
   - `TELEGRAM_BOT_TOKEN` = your bot token
   - `TELEGRAM_CHAT_ID` = your chat ID

### Step 4: Configure Capital (optional)

1. In your GitHub repo → **Settings** → **Secrets and variables** → **Actions** → **Variables** tab
2. Add a **Repository Variable**:
   - `ORCA_CAPITAL` = `1000` (or whatever your trading capital is)

### Step 5: Test It

1. Go to your repo → **Actions** tab
2. Click **ORCA Daily Scan** on the left
3. Click **Run workflow** → **Run workflow**
4. Watch the logs — you should see the scan output
5. Check Telegram — you should get an alert

That's it. It runs automatically every weekday morning.

## How It Gets Smarter Over Time

ORCA stores ATM IV readings in `orca_iv_history.db` (SQLite). This file gets committed back to the repo after every scan.

| Day | IV Rank Quality |
|-----|----------------|
| 1-5 | Approximate (uses HV percentile as proxy) |
| 5-10 | Getting reliable |
| 10-20 | Good — real IV rank from stored data |
| 20+ | Solid — true IV percentile across market conditions |

After a month, you have data no free service provides.

## Manual Usage

You can also run ORCA on your own machine:

```bash
pip install yfinance pandas numpy

# Full scan
python orca.py

# Premium selling opportunities only
python orca.py --mode premium

# Earnings plays only
python orca.py --mode earnings

# Deep dive on a single ticker
python orca.py --dive AVGO

# Set capital
python orca.py --capital 5000
```

## Scan Modes

| Mode | What It Finds |
|------|--------------|
| `premium` | IV > HV by 20%+ → sell iron condors, strangles |
| `earnings` | Implied move vs historical → sell overpriced or buy underpriced |
| `unusual` | Volume/OI spikes, put/call skew → follow smart money |
| `all` | Everything above, ranked by expected value |

## Trigger a Manual Scan from GitHub

Go to **Actions** → **ORCA Daily Scan** → **Run workflow**

You can choose:
- **Scan mode**: all / premium / earnings / unusual
- **Deep dive ticker**: Enter a ticker for single-stock analysis

## File Structure

```
orca-bot/
├── orca.py                          # Stage 1: Technical scanner
├── analyst.py                       # Stage 2: Claude AI dot-connector
├── notify.py                        # Telegram alerts (fallback)
├── requirements.txt
├── orca_iv_history.db               # IV database (auto-created, grows daily)
├── orca_results/                    # All results (auto-created)
│   ├── orca_20260301_0830.csv       # Technical scan results
│   ├── analyst_20260301_0832.md     # Claude analysis + trade ideas
│   └── ...
├── .github/
│   └── workflows/
│       └── orca_daily.yml           # 2-stage pipeline
└── README.md
```

## Updating Earnings Calendar

Edit the `EARNINGS_THIS_WEEK` dict in `orca.py` every Sunday night. Format:

```python
"AVGO": {
    "date": "2026-03-04",      # Earnings date
    "timing": "AMC",            # BMO (before open) or AMC (after close)
    "hist_moves": [8.3, 12.5, -4.2, 24.4, 5.1]  # Last 5 earnings moves (%)
},
```

Get historical earnings moves from: https://optionsstrat.com or https://marketchameleon.com

## Daily Workflow

1. **8:30 AM**: Stage 1 — ORCA scans 120+ tickers (technical: IV, HV, vol, flow)
2. **8:32 AM**: Stage 2 — Analyst scrapes news, sends to Claude API with ORCA data
3. **8:33 AM**: Claude connects dots (2nd/3rd order effects), finds mispriced options
4. **8:34 AM**: Telegram buzzes with 5-7 specific trade recommendations
5. **9:00 AM**: You review the plays, pick top 2-3
6. **9:30 AM**: Market opens — execute
7. **Repeat daily** — IV database compounds, analysis gets sharper

## Cost

- **GitHub Actions**: Free (2,000 min/month, ORCA uses ~3 min/day = ~60/month)
- **Yahoo Finance**: Free (15-min delay, fine for daily setups)
- **Claude API (Sonnet)**: ~$0.01-0.03/day = **~$0.50/month**
- **Telegram**: Free
- **Total**: ~$0.50/month for automated AI-powered options analysis
