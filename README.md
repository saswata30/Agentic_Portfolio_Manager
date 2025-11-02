# AgneticPortFolioManager - Databricks Asset Bundle

## Overview
A Portfolio Manager at Northbridge Capital (a subsidiary of Fairfox Financial Holding) seeks an agentic framework to ingest FactSet factor vectors for any stock ticker (3-year history), enrich them with the firm’s internal positions, orders, and research notes, and expose a Q&A application over unified data. The demo includes an Investor_Report PDF collection and sources metadata referencing: /Volumes/demo_generator/saswata_sengupta_agneticportfoliomanager/raw_data/incoming_data/Investor_Report/interim report.pdf. In June 2025, the framework flags a regime shift for NVDA: momentum and earnings quality vectors decouple, volatility vector rises 1.9x vs baseline, and internal risk limits begin breaching in Growth-US sleeve. Root cause aligns to a 2025-06-18 internal risk policy change that tightened VAR bands after a spike in option gamma. The agent answers natural questions and quantifies impact (PnL drawdown -2.3% in affected sleeve, turnover +35%, slippage +18bps).


![alt text](https://github.com/saswata30/Agentic_Portfolio_Manager/blob/main/Images/Flow.png?raw=true)
![alt text](https://github.com/saswata30/Agentic_Portfolio_Manager/blob/main/Images/Dashboard.png?raw=true)
![alt text](https://github.com/saswata30/Agentic_Portfolio_Manager/blob/main/Images/Genie.png?raw=true)
![alt text](https://github.com/saswata30/Agentic_Portfolio_Manager/blob/main/Images/Multi%20Agent%201.png?raw=true)
![alt text](https://github.com/saswata30/Agentic_Portfolio_Manager/blob/main/Images/Multi%20Agent%201.png?raw=true)


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

## Generated with AI Demo Generator
🤖 This bundle was automatically created using the Databricks AI Demo Generator.

**Created**: 2025-10-22 20:50:54
**User**: saswata.sengupta@databricks.com
