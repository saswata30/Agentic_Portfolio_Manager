# AgneticPortFolioManager  with Databricks AgentBricks
## Overview
A Portfolio Manager at Northbridge Capital (a subsidiary of Fairfox Financial Holding) seeks an agentic framework to ingest FactSet factor vectors for any stock ticker (3-year history), enrich them with the firm’s internal positions, orders, and research notes, and expose a Q&A application over unified data. The demo includes an Investor_Report PDF collection and sources metadata referencing: /Volumes/demo_generator/saswata_sengupta_agneticportfoliomanager/raw_data/incoming_data/Investor_Report/interim report.pdf. In June 2025, the framework flags a regime shift for NVDA: momentum and earnings quality vectors decouple, volatility vector rises 1.9x vs baseline, and internal risk limits begin breaching in Growth-US sleeve. Root cause aligns to a 2025-06-18 internal risk policy change that tightened VAR bands after a spike in option gamma. The agent answers natural questions and quantifies impact (PnL drawdown -2.3% in affected sleeve, turnover +35%, slippage +18bps).


![alt text](https://github.com/saswata30/Agentic_Portfolio_Manager/blob/main/Images/Flow.png?raw=true)
![alt text](https://github.com/saswata30/Agentic_Portfolio_Manager/blob/main/Images/Dashboard.png?raw=true)
![alt text](https://github.com/saswata30/Agentic_Portfolio_Manager/blob/main/Images/Genie.png?raw=true)
![alt text](https://github.com/saswata30/Agentic_Portfolio_Manager/blob/main/Images/Multi%20Agent%201.png?raw=true)
![alt text](https://github.com/saswata30/Agentic_Portfolio_Manager/blob/main/Images/Multi%20Agent%201.png?raw=true)


## What We Monitor
1. Market feature vectors from FactSet by ticker/date: momentum, earnings quality, valuation, volatility, and news sentiment.
2 . Internal positions and exposures (delta, beta, sector weights), orders/executions (fills, slippage, spread, liquidity), and research signals.
3. Risk policy changes and limit breaches (e.g., VAR, concentration, option Greeks).
4. KPIs: daily PnL (%), vector shifts vs baseline, limit breach count, turnover (%), average slippage (bps), realized volatility vs vector volatility, spread cost, and concentration by sector.
## Baseline Behavior (Jan–May 2025)
1. Momentum and earnings quality vectors moderately correlated (r ~0.55) for NVDA and peer basket; volatility vector median ~1.2x of 3-year rolling baseline on event days but ~1.0x otherwise.
2. Internal turnover averages ~12% monthly; slippage ~9–12 bps; limit breaches rare (<2 per month) and mostly concentration flags.
3. Research sentiment is positive with occasional neutral periods around earnings.
## Event to Capture (Regime Shift and Policy Tightening)
•	Trigger window: 2025-06-15..2025-06-22.
•	Observed FactSet anomaly: volatility vector rises to ~1.9x baseline starting 2025-06-18; momentum remains high while earnings quality vector dips ~22% vs its 90-day mean; sentiment becomes mixed.
•	Internal signals: Growth-US sleeve records 11 risk limit breaches (VAR and gamma) vs typical 0–2; turnover spikes +35% in late June; average slippage increases to ~18 bps due to higher spreads and urgency.
•	Root cause: 2025-06-18 internal risk policy update tightened VAR bands and gamma thresholds following a 3-standard-deviation move in options-implied volatility. This dated change aligns temporally with breach count and order urgency.
•	Business consequence: short-term drawdown of -2.3% in the Growth-US sleeve (NVDA/semis exposure), followed by controlled risk reduction; opportunity cost estimated at -$4.2M vs staying fully invested, but improved tail-risk protection.
## Narrative Business Story
1.	Anomaly observed in external market vectors: NVDA volatility vector jumps to ~1.9x baseline on 2025-06-18, while earnings quality vector declines ~22%. Momentum remains elevated, creating a decoupling in signal mix that typically precedes wider spreads and execution difficulty.
2.	Supporting internal metrics: Orders and executions show urgency (marketable orders), wider quoted spreads, realized slippage +18 bps, and turnover +35%. Positions data show concentration reduction in semis; breach logs show VAR/gamma breaches clustering on 2025-06-18..2025-06-20.
3.	Root cause aligned to change log: Internal risk_policy_changes table records a 2025-06-18 update tightening VAR/gamma limits for Growth-US, with explicit policy IDs, scopes, and notes. Correlation between breach count and the change date is strong; post-change, the number of marketable orders increases.
4.	Business impact quantified: Sleeve PnL dips -2.3% in late June; realized volatility vs vector volatility widens; liquidity costs rise. The agent can trace which tickers (NVDA, AMD) contributed most and surface Q&A such as whether the policy change prevented larger drawdowns.
## Q&A App Requirements
1. Natural-language interface over unified datasets (external vectors and internal records) with guardrails: answer derivations must reference dated facts, show calculations (e.g., baseline vs event ratios), and cite source tables/fields. KA source configuration includes the Investor_Report collection and the interim report.pdf path for governance and policy context.
2. Capabilities: ticker-level and sleeve-level questions; time-window filters; factor attribution; breach explanations; execution cost breakdown; policy timeline alignment; scenario backtests (e.g., no-policy-change).
3. Constraints: responses must include date ranges, magnitudes, and affected segments; expose uncertainty where applicable (e.g., backtest assumptions).
## Genie Configuration
1. Supported intents: trend_analysis, root_cause_alignment, execution_cost_attribution, exposure_change, policy_impact, scenario_backtest, sentiment_momentum_divergence.
2. Response schema: include fields [time_window, entities (ticker, sleeve), metrics, calculations (baseline_vs_event), citations (table.column), confidence].
3. Guardrails: strict date-window adherence; cite policy_id for any policy-driven answer; explain assumptions for backtests.
## What We Monitor (KPIs)
1. Vector Shift Ratio (per factor): current_value / 90-day baseline.
2. Breach Count per day and per policy type.
3. Turnover (% of portfolio traded) and Orders Urgency (% marketable).
4. Slippage (bps) and Effective Spread (bps).
5. PnL (%) daily and cumulative; Realized Vol vs Vector Vol.
6. Concentration metrics (top positions share, sector weights).
## Measurements and Windows
1. Period: 2025-04-01..2025-10-21 (3-year vectors cover 2022-10-01..2025-10-21 per ticker; we focus on recent 6 months for impact charts).
2. Event window: 2025-06-15..2025-06-22; recovery: through July.
3. Segments: sleeve={Growth-US, Core-EMEA, Tech-APAC}, sector={Semis, Software, Hardware}, order_type={marketable, limit}, venue={NASDAQ, NYSE, ARCA}.


## Deployment

This bundle can be deployed to any Databricks workspace using Databricks Asset Bundles (DAB):

### Prerequisites
1. **Databricks CLI**: Install the latest version
   ```bash
   pip install databricks-cli
   ```
2. **Authentication**: Configure your workspace credentials
   ```bash
   databricks configure
   ```
3. **Workspace Access**: Ensure you have permissions for:
   - Unity Catalog catalog/schema creation
   - SQL Warehouse access
   - Workspace file storage

### Deploy the Bundle
```bash
# Navigate to the dab directory
cd dab/

# Validate the bundle configuration
databricks bundle validate

# Deploy to your workspace (--force-lock to override any existing locks)
databricks bundle deploy --force-lock

# Run the data generation workflow
databricks bundle run demo_workflow
```

The deployment will:
1. Create Unity Catalog resources (schema and volume)
2. Upload PDF files to workspace (if applicable)
3. Deploy job and dashboard resources

The workflow will:
1. Create Unity Catalog catalog if it doesn't exist (DAB doesn't support catalog creation)
2. Generate synthetic data using Faker and write to Unity Catalog Volume
3. Execute SQL transformations (bronze → silver → gold)
4. Deploy agent bricks (Genie spaces, Knowledge Assistants, Multi-Agent Supervisors) if configured

## Bundle Contents

### Core Files
- `databricks.yml` - Asset bundle configuration defining jobs, dashboards, and deployment settings
- `bricks_conf.json` - Agent brick configurations (Genie/KA/MAS) if applicable
- `agent_bricks_service.py` - Service for managing agent brick resources (includes type definitions)
- `deploy_resources.py` - Script to recreate agent bricks in the target workspace

### Data Generation
- Python scripts using Faker library for realistic synthetic data
- Configurable row counts, schemas, and business logic
- Automatic Delta table creation in Unity Catalog
- Upload investor report from the PDF in this repo in case you are not using DAB
  ![alt text](https://github.com/saswata30/Agentic_Portfolio_Manager/blob/main/Images/Investor%20Report.png?raw=true)

### SQL Transformations
- `transformations.sql` - SQL transformations for data processing
- Bronze (raw) → Silver (cleaned) → Gold (aggregated) medallion architecture
- Views and tables for business analytics

### Agent Bricks
This bundle includes AI agent resources:

- **Genie Space** (ID: `01f0af941e6314e1bc2ed7e66496e9ff`)
  - Natural language interface for data exploration
  - Configured with table identifiers from your catalog/schema
  - Sample questions and instructions included

- **Knowledge Assistant** (ID: `031f41ef-6d4a-4183-8f1e-81d50a9815ec`)
  - AI assistant with knowledge sources from Unity Catalog volumes
  - Vector search-powered retrieval augmented generation (RAG)
  - Example questions and guidelines included

- **Multi-Agent Supervisor** (ID: `bd255457-8c45-4f51-8705-d8d8b1c2759d`)
  - Orchestrates multiple specialized agents
  - Routes queries to appropriate sub-agents (Genie, KA, endpoints)
  - Complex multi-step workflows supported

### Dashboards
This bundle includes Lakeview dashboards:
- **Portfolio Risk and Execution Overview** - Business intelligence dashboard with visualizations

### PDF Documents
This bundle includes PDF documents that will be uploaded to the workspace:
- PDF files are automatically uploaded during bundle deployment via DAB artifacts
- These PDFs are included in the bundle workspace path
- You can manually copy them to Unity Catalog Volumes if needed for RAG scenarios
- Example: Use Databricks Files API or `dbutils.fs.cp` to move files to a volume

## Configuration

### Unity Catalog
- **Catalog**: `demo_generator`
- **Schema**: `saswata_sengupta_agneticportfoliomanager`
- **Workspace Path**: `/Users/saswata.sengupta@databricks.com/AgneticPortFolioManager`

### Customization
You can modify the bundle by editing `databricks.yml`:
- Change target catalog/schema in the `variables` section
- Adjust cluster specifications for data generation
- Add additional tasks or resources

## Key Questions This Demo Answers
1. When did the volatility vector shift occur for NVDA, how large was it versus the 90-day baseline, and how long did it persist?
2. Which tickers and sectors contributed most to breach counts and turnover during 2025-06-15..2025-06-22, and how did execution costs change?
3. How strongly do the policy change timestamps align with breach clusters and order urgency, and what is the correlation strength?
4. What was the sleeve-level PnL drawdown and cost attribution (spread vs market impact) during late June, and which trades drove it?
5. Did the tightened VAR/gamma limits prevent larger losses under plausible scenarios, and what is the estimated opportunity cost?
6. How did realized volatility compare to the FactSet volatility vector pre/post event, and is the gap closing in July?
7. What residual risks remain (concentration, liquidity) and what timeline do we expect for normalization?
8. Show the top 10 tickers by cumulative turnover and slippage since 2025-06-01, segmented by sleeve.
9. Quantify the delta and beta exposure reduction for Semiconductors in Growth-US from 2025-06-10 to 2025-07-15.
10. Which policy_id changes in 2025-06 directly aligned (time_delta_days <= 1) with VAR breaches, and what was the average change_magnitude?

## Deployment to New Workspaces

This bundle is **portable** and can be deployed to any Databricks workspace:

1. The bundle will recreate all resources in the target workspace
2. Agent bricks (Genie/KA/MAS) are recreated from saved configurations in `bricks_conf.json`
3. SQL transformations and data generation scripts are environment-agnostic
4. Dashboards are deployed as Lakeview dashboard definitions

Simply run `databricks bundle deploy` in any workspace where you have the required permissions.

## Troubleshooting

### Common Issues

**Bundle validation fails:**
- Ensure `databricks.yml` has valid YAML syntax
- Check that catalog and schema names are valid
- Verify warehouse lookup matches an existing warehouse

**Agent brick deployment fails:**
- Check that `bricks_conf.json` exists and contains valid configurations
- Ensure you have permissions to create Genie spaces, KA tiles, and MAS tiles
- Verify vector search endpoint exists for Knowledge Assistants

**SQL transformations fail:**
- Ensure the catalog and schema exist in the target workspace
- Check warehouse permissions and availability
- Review SQL syntax for Unity Catalog compatibility (3-level namespace: `catalog.schema.table`)

### Getting Help
- Review Databricks Asset Bundles documentation: https://docs.databricks.com/dev-tools/bundles/
- Check the generated code in this bundle for implementation details
- Contact your Databricks workspace administrator for permissions issues


**Created**: 2025-10-22 20:50:54
**User**: saswata.sengupta@databricks.com
