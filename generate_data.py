# ============================================================
# === Agentic Portfolio Manager - RAW Data Generation      ===
# ============================================================
# PURPOSE
# Generate RAW synthetic datasets that match demo_story.json for
# AgneticPortFolioManager. The output encodes baseline vs event
# dynamics (June 2025 regime shift and policy tightening) across
# external vectors and internal records, ready for Silver/Gold.
#
# CONTRACTS
# - Schema fidelity: columns/types exactly per datasources.
# - RAW-only: no derived baselines/ratios here; primitive fields only.
# - Referential integrity: tickers/sleeves/sectors align across tables.
# - Timestamps are tz-naive and floored to millisecond precision.
# - Performance: vectorized operations; reproducible seeds.
#
# STORY HIGHLIGHTS ENCODED
# - Event window 2025-06-15..2025-06-22; regime shift on 2025-06-18.
# - NVDA volatility vector ~1.9x baseline on 2025-06-18; quality dips ~22%;
#   momentum stays high; sentiment mixed. AMD shows smaller similar effects.
# - Internal: Growth-US sleeve limit breaches (VAR/gamma) cluster 2025-06-18..20;
#   turnover +35%, slippage ~18 bps, more marketable orders; de-risking SELL bias.
# - Risk policy change 2025-06-18 tightens VAR/gamma thresholds for Growth-US.
# - PnL drawdown will be derived later from positions + proxy returns.
# ============================================================

import random
import string

import numpy as np
import pandas as pd
from faker import Faker

# External holidays is available (seasonality support)
import holidays

# Provided utility to save datasets
from utils import save_to_parquet

# Set environment variables for Databricks Volumes
import os
os.environ['CATALOG'] = 'sas_demo'
os.environ['SCHEMA'] = 'saswata_sengupta_agneticportfoliomanager'
os.environ['VOLUME'] = 'raw_data'



# ===============================
# === REPRO & GLOBAL WINDOWS
# ===============================
SEED = 42
np.random.seed(SEED)
random.seed(SEED)
fake = Faker()
Faker.seed(SEED)

# Avoid tz-aware; floor to ms per guidance
RANGE_START = pd.Timestamp('2022-10-01').floor('ms')
RANGE_END = pd.Timestamp('2025-10-21').floor('ms')
IMPACT_START = pd.Timestamp('2025-04-01').floor('ms')  # internal data period
IMPACT_END = pd.Timestamp('2025-10-21').floor('ms')
EVENT_START = pd.Timestamp('2025-06-15').floor('ms')
EVENT_PIVOT = pd.Timestamp('2025-06-18').floor('ms')
EVENT_END = pd.Timestamp('2025-06-22').floor('ms')

# Tickers and sector mapping (Semis higher vol tails)
TICKERS = ['NVDA', 'AMD', 'INTC', 'AVGO', 'MSFT', 'ADBE', 'AAPL']
SECTOR_BY_TICKER = {
    'NVDA': 'Semiconductors',
    'AMD': 'Semiconductors',
    'INTC': 'Semiconductors',
    'AVGO': 'Semiconductors',
    'MSFT': 'Software',
    'ADBE': 'Software',
    'AAPL': 'Hardware',
}

SLEEVES = ['Growth-US', 'Core-EMEA', 'Tech-APAC']
VENUES = {'NVDA': 'NASDAQ', 'AMD': 'NASDAQ', 'INTC': 'NASDAQ', 'AVGO': 'NASDAQ', 'MSFT': 'NASDAQ', 'ADBE': 'NASDAQ', 'AAPL': 'NASDAQ'}

# Precompute hours distribution for intraday order timestamps
HOURS = np.arange(9, 16)
HOURS_PROB = (np.array([0.20, 0.12, 0.11, 0.10, 0.09, 0.10, 0.28]) / 1.0)
# Cache for trading days
_TRADING_DAYS_CACHE = {}

# ===============================
# === UTIL HELPERS
# ===============================

def trading_days(start: pd.Timestamp, end: pd.Timestamp) -> pd.DatetimeIndex:
    """Return tz-naive business days (Mon-Fri), excluding major US holidays. Cached for reuse."""
    start_n = start.normalize()
    end_n = end.normalize()
    cache_key = (int(start_n.value), int(end_n.value))
    cached = _TRADING_DAYS_CACHE.get(cache_key)
    if cached is not None:
        return cached

    days = pd.to_datetime(pd.date_range(start_n, end_n, freq='B'), errors='coerce')
    # Exclude US holidays for realism
    hol = holidays.UnitedStates(years=list(range(start_n.year, end_n.year + 1)))
    hol_set = {pd.Timestamp(d).normalize() for d in hol.keys()}
    mask = ~np.isin(days.normalize(), list(hol_set))
    result = days[mask]
    _TRADING_DAYS_CACHE[cache_key] = result
    return result


def _dirichlet_nonflat(base_weights: np.ndarray, alpha_scale: float, size: int) -> np.ndarray:
    """Draw non-flat mixtures that sum to 1 using Dirichlet."""
    base = np.tile(base_weights, (size, 1))
    alphas = base * alpha_scale
    return np.array([np.random.dirichlet(a) for a in alphas])


def _id_seq(prefix: str, n: int, digits: int = 6) -> np.ndarray:
    return np.array([f"{prefix}-{i+1:0{digits}d}" for i in range(n)], dtype=object)


# ===============================
# === 1) FactSet Factor Vectors
# ===============================

def generate_factset_factor_vectors() -> pd.DataFrame:
    print('Generating factset_factor_vectors (3-year daily vectors)...')
    days = trading_days(RANGE_START, RANGE_END)

    # Sector-level parameters for quality drift & volatility tails
    sector_params = {
        'Semiconductors': {
            'vol_tail_sigma': 0.50,
            'quality_mean': 0.62,
            'quality_sd': 0.10,
        },
        'Software': {
            'vol_tail_sigma': 0.35,
            'quality_mean': 0.68,
            'quality_sd': 0.08,
        },
        'Hardware': {
            'vol_tail_sigma': 0.40,
            'quality_mean': 0.65,
            'quality_sd': 0.09,
        },
    }

    # Base momentum/sentiment: clustered spikes around earnings (approx quarterly)
    # Create simple quarterly earnings windows for spikes
    earnings_months = {1, 4, 7, 10}
    is_earnings_day = (np.isin(days.month, list(earnings_months)) & ((days.day >= 20) & (days.day <= 28)))

    rows = []
    progress_interval = max(1, len(TICKERS) // 10)

    for i, t in enumerate(TICKERS):
        if (i + 1) % progress_interval == 0 or i == 0:
            progress = ((i + 1) / len(TICKERS)) * 100
            print(f"  Tickers progress: {progress:.0f}% ({i + 1:,}/{len(TICKERS):,})")

        sector = SECTOR_BY_TICKER[t]
        p = sector_params[sector]

        # Volatility baseline per ticker (slow drift)
        vol_base = np.clip(np.random.normal(0.45, 0.07), 0.20, 0.70)
        vol_noise = np.random.lognormal(mean=0.0, sigma=p['vol_tail_sigma'], size=len(days))
        volatility = np.clip(vol_base * (0.9 + 0.2 * vol_noise), 0.02, 0.98)

        # Momentum: log-normal with spikes near earnings
        mom_base = np.clip(np.random.normal(0.55 if t in ['NVDA', 'AMD'] else 0.50, 0.05), 0.25, 0.75)
        momentum = np.clip(np.random.lognormal(mean=np.log(mom_base), sigma=0.20, size=len(days)), 0.02, 0.98)
        momentum[is_earnings_day] = np.clip(momentum[is_earnings_day] * np.random.uniform(1.2, 1.6, size=is_earnings_day.sum()), 0.02, 0.98)

        # Earnings quality: sector mean with drift; semis can dip around supply-chain notes
        quality_base = np.clip(np.random.normal(p['quality_mean'], p['quality_sd']), 0.30, 0.90)
        quality_noise = np.random.normal(0.0, p['quality_sd'], size=len(days))
        earnings_quality = np.clip(quality_base + quality_noise, 0.02, 0.98)

        # Valuation: slow drift by sector/ticker (higher => cheaper)
        val_base = np.clip(np.random.normal(0.48 if sector == 'Semiconductors' else 0.52, 0.06), 0.25, 0.75)
        valuation = np.clip(val_base + np.random.normal(0.0, 0.05, size=len(days)), 0.02, 0.98)

        # Sentiment: mixed around event and earnings
        sent_base = np.clip(np.random.normal(0.55, 0.08), 0.25, 0.80)
        news_sentiment = np.clip(np.random.normal(sent_base, 0.18, size=len(days)), 0.02, 0.98)
        news_sentiment[is_earnings_day] = np.clip(news_sentiment[is_earnings_day] * np.random.uniform(1.05, 1.25, size=is_earnings_day.sum()), 0.02, 0.98)

        # Inject event anomalies for NVDA & AMD
        event_mask = (days >= EVENT_START) & (days <= EVENT_END)
        pivot_mask = days == EVENT_PIVOT
        if t == 'NVDA':
            # Volatility ~1.9x baseline on pivot, elevated around event window
            baseline_window = (days >= pd.Timestamp('2025-03-20')) & (days <= pd.Timestamp('2025-06-14'))
            baseline_vol = np.maximum(0.10, float(np.nanmedian(volatility[baseline_window])))
            volatility[event_mask] *= np.random.uniform(1.25, 1.60)
            volatility[pivot_mask] = np.clip(baseline_vol * 1.90, 0.02, 0.98)
            # Momentum remains high
            momentum[event_mask] *= np.random.uniform(1.05, 1.20)
            # Earnings quality dips ~22%
            earnings_quality[event_mask] = np.clip(earnings_quality[event_mask] * 0.78, 0.02, 0.98)
            # Sentiment becomes mixed
            news_sentiment[event_mask] = np.clip(np.random.normal(sent_base * 0.90, 0.25, size=event_mask.sum()), 0.02, 0.98)
        elif t == 'AMD':
            baseline_window = (days >= pd.Timestamp('2025-03-20')) & (days <= pd.Timestamp('2025-06-14'))
            baseline_vol = np.maximum(0.10, float(np.nanmedian(volatility[baseline_window])))
            volatility[event_mask] *= np.random.uniform(1.15, 1.40)
            volatility[pivot_mask] = np.clip(baseline_vol * 1.55, 0.02, 0.98)
            momentum[event_mask] *= np.random.uniform(1.03, 1.12)
            earnings_quality[event_mask] = np.clip(earnings_quality[event_mask] * 0.85, 0.02, 0.98)
            news_sentiment[event_mask] = np.clip(np.random.normal(sent_base * 0.95, 0.22, size=event_mask.sum()), 0.02, 0.98)

        # Assemble rows (vectorized)
        df_t = pd.DataFrame(
            {
                'ticker': np.repeat(t, len(days)),
                'date': pd.to_datetime(days, errors='coerce').floor('ms'),
                'momentum_score': momentum.astype(float),
                'earnings_quality_score': earnings_quality.astype(float),
                'valuation_score': valuation.astype(float),
                'volatility_score': volatility.astype(float),
                'news_sentiment_score': news_sentiment.astype(float),
            }
        )
        rows.append(df_t)

    df = pd.concat(rows, ignore_index=True)

    # Ensure ms-floored datetime
    df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.floor('ms')
    print(f"factset_factor_vectors rows: {len(df):,}")
    return df


# ===============================
# === 2) Internal Positions
# ===============================

def generate_internal_positions() -> pd.DataFrame:
    print('Generating internal_positions (daily per ticker & sleeve)...')
    days = trading_days(IMPACT_START, IMPACT_END)

    # Exposure concentration: Growth-US concentrated in Semis; others diversified
    sleeve_ticker_weights = {
        'Growth-US': {
            'NVDA': 0.26, 'AMD': 0.14, 'INTC': 0.08, 'AVGO': 0.10, 'MSFT': 0.08, 'ADBE': 0.06, 'AAPL': 0.28
        },
        'Core-EMEA': {
            'NVDA': 0.08, 'AMD': 0.06, 'INTC': 0.10, 'AVGO': 0.06, 'MSFT': 0.28, 'ADBE': 0.20, 'AAPL': 0.22
        },
        'Tech-APAC': {
            'NVDA': 0.12, 'AMD': 0.10, 'INTC': 0.12, 'AVGO': 0.10, 'MSFT': 0.18, 'ADBE': 0.14, 'AAPL': 0.24
        },
    }

    # Sleeve market value baseline (USD)
    sleeve_mv_base = {
        'Growth-US': 4_200_000_000.0,  # Growth-US sleeve scale
        'Core-EMEA': 5_800_000_000.0,
        'Tech-APAC': 2_500_000_000.0,
    }

    rows = []
    for s in SLEEVES:
        weights = np.array([sleeve_ticker_weights[s][t] for t in TICKERS])
        weights = weights / weights.sum()
        # Daily sleeve MV with mild drift and event drawdown late June
        mv = np.ones(len(days)) * sleeve_mv_base[s]
        mv = mv * (1.0 + np.linspace(-0.02, 0.03, len(days)))  # slow drift
        # Event drawdown -2.3% for Growth-US; smaller elsewhere
        event_mask = (days >= EVENT_PIVOT) & (days <= (EVENT_PIVOT + pd.Timedelta(days=7)))
        drop = -0.023 if s == 'Growth-US' else -0.010
        mv[event_mask] *= (1.0 + drop)
        # Recovery into July
        rec_mask = (days > EVENT_END) & (days <= pd.Timestamp('2025-07-31'))
        mv[rec_mask] *= 0.995 + np.linspace(0.0, 0.012, rec_mask.sum())

        # Ticker MV by weights + noise
        ticker_mv = (mv[:, None] * weights[None, :]) * np.random.lognormal(mean=0.0, sigma=0.12, size=(len(days), len(TICKERS)))

        # Quantities derived from MV via approximate price proxies (not stored elsewhere)
        # Use per-ticker synthetic price scale to create heavy-tailed quantities
        price_scale = {
            'NVDA': 1050.0, 'AMD': 175.0, 'INTC': 38.0, 'AVGO': 1500.0, 'MSFT': 420.0, 'ADBE': 600.0, 'AAPL': 210.0
        }

        for j, t in enumerate(TICKERS):
            sector = SECTOR_BY_TICKER[t]
            delta = 0.98 if sector == 'Semiconductors' else (0.95 if sector == 'Hardware' else 0.92)
            beta = 1.20 if sector == 'Semiconductors' else (1.05 if sector == 'Hardware' else 1.00)

            qty = np.clip((ticker_mv[:, j] / price_scale[t]).astype(int), 1000, None)

            # De-risking late June: reduce semis quantities for Growth-US
            if s == 'Growth-US' and sector == 'Semiconductors':
                reduce_mask = (days >= EVENT_PIVOT) & (days <= (EVENT_PIVOT + pd.Timedelta(days=10)))
                qty[reduce_mask] = (qty[reduce_mask] * np.random.uniform(0.70, 0.88)).astype(int)
                # market_value updates accordingly
                ticker_mv[:, j][reduce_mask] = qty[reduce_mask] * price_scale[t]

            # Concentration flags if MV share > 12%
            concentration_flag = (ticker_mv[:, j] / mv) > 0.12

            df = pd.DataFrame(
                {
                    'position_id': [f"POS-{pd.Timestamp(d).strftime('%Y%m%d')}-{s[:2].upper()}-{t}-{k:04d}" for k, d in enumerate(days, start=1)],
                    'date': pd.to_datetime(days, errors='coerce').floor('ms'),
                    'ticker': np.repeat(t, len(days)),
                    'sleeve': np.repeat(s, len(days)),
                    'sector': np.repeat(sector, len(days)),
                    'quantity': qty.astype(int),
                    'market_value_usd': ticker_mv[:, j].astype(float),
                    'delta': np.repeat(delta, len(days)).astype(float),
                    'beta': np.repeat(beta, len(days)).astype(float),
                    'concentration_flag': concentration_flag.astype(bool),
                }
            )
            rows.append(df)

    df = pd.concat(rows, ignore_index=True)
    df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.floor('ms')

    print(f"internal_positions rows: {len(df):,}")
    return df


# ===============================
# === 3) Internal Orders & Executions
# ===============================

def generate_internal_orders_executions() -> pd.DataFrame:
    print('Generating internal_orders_executions (atomic intraday events)...')
    days = trading_days(IMPACT_START, IMPACT_END)

    # Preallocate column lists to avoid per-iteration DataFrame construction
    order_id_col = []
    parent_order_id_col = []
    timestamp_col = []
    date_col = []
    ticker_col = []
    sleeve_col = []
    side_col = []
    order_type_col = []
    venue_col = []
    quote_spread_bps_col = []
    depth_col = []
    fill_qty_col = []
    avg_price_col = []
    slippage_bps_col = []

    order_counter = 1
    progress_interval = max(1, len(days) // 10)

    # Baseline per-day orders per sleeve-ticker (non-flat; Growth-US higher)
    base_orders = {
        'Growth-US': {t: (45 if SECTOR_BY_TICKER[t] == 'Semiconductors' else 28) for t in TICKERS},
        'Core-EMEA': {t: (26 if SECTOR_BY_TICKER[t] == 'Semiconductors' else 32) for t in TICKERS},
        'Tech-APAC': {t: (22 if SECTOR_BY_TICKER[t] == 'Semiconductors' else 20) for t in TICKERS},
    }

    for i, d in enumerate(days):
        if (i + 1) % progress_interval == 0 or i == 0:
            progress = ((i + 1) / len(days)) * 100
            print(f"  Days progress: {progress:.0f}% ({i + 1:,}/{len(days):,})")

        day = pd.Timestamp(d).floor('ms').normalize()
        in_event = (day >= EVENT_START.normalize()) & (day <= EVENT_END.normalize())

        for s in SLEEVES:
            for t in TICKERS:
                n = base_orders[s][t]
                # Turnover +35% late June for Growth-US
                if s == 'Growth-US' and in_event:
                    n = int(n * 1.35)
                # Market hours distribution (more activity on open/close)
                hours = np.random.choice(HOURS, size=n, p=HOURS_PROB)
                minutes = np.random.randint(0, 60, size=n)
                ts = (
                    pd.to_datetime(day)
                    + pd.to_timedelta(hours, unit='h')
                    + pd.to_timedelta(minutes, unit='m')
                ).floor('ms')

                side = np.random.choice(['BUY', 'SELL'], size=n, p=[0.48, 0.52])
                # Event SELL bias for semis
                if in_event and SECTOR_BY_TICKER[t] == 'Semiconductors':
                    side = np.random.choice(['BUY', 'SELL'], size=n, p=[0.35, 0.65])

                # Order type mix shifts to more market/marketable_limit in event
                order_type = np.random.choice(
                    ['market', 'marketable_limit', 'limit'],
                    size=n,
                    p=(np.array([0.18, 0.32, 0.50]) if not in_event else np.array([0.30, 0.40, 0.30]))
                )

                venue = np.repeat(VENUES.get(t, 'NASDAQ'), n)

                # Microstructure fields
                spread = np.random.lognormal(mean=np.log(10.0), sigma=0.35, size=n)  # baseline ~10 bps
                depth = np.random.lognormal(mean=np.log(25_000), sigma=0.60, size=n).astype(int)

                if in_event:
                    spread *= np.random.uniform(1.4, 1.9)
                    depth = (depth * np.random.uniform(0.55, 0.85)).astype(int)

                # Fills and prices
                fill_qty = np.random.poisson(lam=2_500, size=n) + np.random.randint(100, 800, size=n)
                avg_price = np.random.lognormal(mean=np.log(200.0), sigma=0.45, size=n)

                # Slippage bps baseline 9-12; event ~18 bps heavy-tailed in low depth
                slip = np.random.normal(loc=10.5, scale=3.0, size=n)
                if in_event:
                    slip = np.random.normal(loc=18.0, scale=6.0, size=n)
                    low_depth = depth < 12_000
                    slip[low_depth] += np.random.uniform(2.0, 8.0, size=low_depth.sum())

                # Parent order linkage: small null rate <=0.1%
                parent = np.array([f"PORD-{random.randint(100000, 999999)}" for _ in range(n)], dtype=object)
                null_mask = np.random.rand(n) < 0.001
                parent[null_mask] = None

                # Extend preallocated columns
                order_id_col.extend([f"ORD-{order_counter + k:08d}" for k in range(n)])
                parent_order_id_col.extend(parent.tolist())
                timestamp_col.extend(ts.tolist())
                date_col.extend([day] * n)
                ticker_col.extend([t] * n)
                sleeve_col.extend([s] * n)
                side_col.extend(side.tolist())
                order_type_col.extend(order_type.tolist())
                venue_col.extend(venue.tolist())
                quote_spread_bps_col.extend(spread.astype(float).tolist())
                depth_col.extend(depth.astype(int).tolist())
                fill_qty_col.extend(fill_qty.astype(int).tolist())
                avg_price_col.extend(avg_price.astype(float).tolist())
                slippage_bps_col.extend(slip.astype(float).tolist())

                order_counter += n

    # Single DataFrame construction at the end
    df = pd.DataFrame(
        {
            'order_id': order_id_col,
            'parent_order_id': parent_order_id_col,
            'timestamp': timestamp_col,
            'date': date_col,
            'ticker': ticker_col,
            'sleeve': sleeve_col,
            'side': side_col,
            'order_type': order_type_col,
            'venue': venue_col,
            'quote_spread_bps': quote_spread_bps_col,
            'top_of_book_depth_shares': depth_col,
            'fill_qty': fill_qty_col,
            'avg_fill_price': avg_price_col,
            'slippage_bps': slippage_bps_col,
        }
    )

    # Normalize datetime columns to ms precision (tz-naive)
    for c in ['timestamp', 'date']:
        df[c] = pd.to_datetime(df[c], errors='coerce').dt.floor('ms')

    print(f"internal_orders_executions rows: {len(df):,}")
    return df


# ===============================
# === 4) Risk Policy Changes
# ===============================

def generate_risk_policy_changes() -> pd.DataFrame:
    print('Generating risk_policy_changes (dated change log)...')
    # A handful of changes across sleeves; critical 2025-06-18 for Growth-US
    changes = []

    # Helper to create policy rows
    def add_change(change_dt: pd.Timestamp, sleeve: str, sector: str, ltype: str, old: float, new: float, notes: str, idx: int):
        changes.append(
            {
                'policy_id': f"POL-{change_dt.strftime('%Y%m%d')}-{idx:03d}",
                'change_date': (change_dt + pd.Timedelta(hours=np.random.randint(8, 12))).floor('ms'),
                'scope_sleeve': sleeve,
                'scope_sector': sector,
                'limit_type': ltype,
                'old_threshold': float(old),
                'new_threshold': float(new),
                'notes': notes,
            }
        )

    # Pre/post event routine tweaks
    add_change(pd.Timestamp('2025-05-15'), 'Core-EMEA', None, 'concentration', 0.15, 0.14, 'Routine concentration band tuning EMEA diversified sleeve.', 101)
    add_change(pd.Timestamp('2025-05-28'), 'Tech-APAC', None, 'VAR', 2.4, 2.3, 'Quarterly VAR refresh for APAC tech exposure.', 102)

    # Critical policy tightening on 2025-06-18 (VAR and gamma) for Growth-US
    add_change(pd.Timestamp('2025-06-18'), 'Growth-US', 'Semiconductors', 'VAR', 2.5, 2.1, 'Tightened VAR bands following 3-sigma spike in options-implied vol; semis exposure.', 201)
    add_change(pd.Timestamp('2025-06-18'), 'Growth-US', 'Semiconductors', 'gamma', 1.8, 1.4, 'Gamma threshold reduced due to elevated option greeks; objective to curb tail risk.', 202)

    # Post-event tuning
    add_change(pd.Timestamp('2025-07-10'), 'Growth-US', None, 'concentration', 0.14, 0.13, 'Post-event concentration trim to improve liquidity flexibility.', 203)

    # Additional minor changes
    add_change(pd.Timestamp('2025-08-22'), 'Core-EMEA', 'Software', 'gamma', 2.0, 1.9, 'Software gamma fine-tune after quarterly review.', 204)
    add_change(pd.Timestamp('2025-09-30'), 'Tech-APAC', 'Hardware', 'VAR', 2.3, 2.2, 'Hardware VAR minor tightening to align with global framework.', 205)

    df = pd.DataFrame(changes)
    df['change_date'] = pd.to_datetime(df['change_date'], errors='coerce').dt.floor('ms')
    print(f"risk_policy_changes rows: {len(df):,}")
    return df


# ===============================
# === 5) Risk Limit Breaches
# ===============================

def generate_risk_limit_breaches() -> pd.DataFrame:
    print('Generating risk_limit_breaches (daily breach records)...')
    days = trading_days(IMPACT_START, IMPACT_END)

    rows = []
    progress_interval = max(1, len(days) // 10)

    for i, d in enumerate(days):
        if (i + 1) % progress_interval == 0 or i == 0:
            progress = ((i + 1) / len(days)) * 100
            print(f"  Days progress: {progress:.0f}% ({i + 1:,}/{len(days):,})")
        day = pd.Timestamp(d).floor('ms').normalize()
        in_cluster = (day >= EVENT_PIVOT.normalize()) & (day <= (EVENT_PIVOT.normalize() + pd.Timedelta(days=2)))

        for s in SLEEVES:
            for ltype in ['VAR', 'gamma', 'concentration']:
                # Baseline near zero for VAR/gamma; occasional concentration
                base_mean = 0.3 if ltype in ['VAR', 'gamma'] else 1.0
                count = np.random.poisson(lam=base_mean)
                severity = np.clip(np.random.lognormal(mean=-0.5, sigma=0.8), 0.01, 0.98)

                if s == 'Growth-US' and ltype in ['VAR', 'gamma'] and in_cluster:
                    # Clustered breaches: total around 11 across 18..20 June
                    count = np.random.choice([3, 4, 5])
                    severity = np.clip(np.random.lognormal(mean=0.0, sigma=0.9), 0.18, 0.98)

                rows.append(
                    {
                        'breach_id': f"BR-{pd.Timestamp(day).strftime('%Y%m%d')}-{s[:2].upper()}-{ltype}-{np.random.randint(1000, 9999)}",
                        'date': pd.to_datetime(day, errors='coerce').floor('ms'),
                        'sleeve': s,
                        'limit_type': ltype,
                        'breach_count': int(count),
                        'severity_score': float(severity),
                    }
                )

    df = pd.DataFrame(rows)
    df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.floor('ms')
    print(f"risk_limit_breaches rows: {len(df):,}")
    return df


# ===============================
# === MAIN SAVE & QA SUMMARY
# ===============================
if __name__ == '__main__':
    print('Starting Agentic Portfolio Manager data generation...')
    print('-' * 60)

    factset_vectors = generate_factset_factor_vectors()
    save_to_parquet(factset_vectors, 'factset_factor_vectors', num_files=12)

    positions = generate_internal_positions()
    save_to_parquet(positions, 'internal_positions', num_files=8)

    orders_execs = generate_internal_orders_executions()
    save_to_parquet(orders_execs, 'internal_orders_executions', num_files=12)

    policy_changes = generate_risk_policy_changes()
    save_to_parquet(policy_changes, 'risk_policy_changes', num_files=1)

    breaches = generate_risk_limit_breaches()
    save_to_parquet(breaches, 'risk_limit_breaches', num_files=4)

    # ===============================
    # === Validation & QA Summary ===
    # ===============================
    print('\nQA SUMMARY:')
    # Event signals in vectors
    nvda = factset_vectors[factset_vectors['ticker'] == 'NVDA']
    pivot_row = nvda[nvda['date'] == EVENT_PIVOT]
    if not pivot_row.empty:
        base_window = nvda[(nvda['date'] >= pd.Timestamp('2025-03-20')) & (nvda['date'] <= pd.Timestamp('2025-06-14'))]
        base_vol = float(base_window['volatility_score'].median())
        pivot_vol = float(pivot_row['volatility_score'].iloc[0])
        ratio = pivot_vol / max(base_vol, 1e-6)
        print(f"- NVDA volatility pivot {EVENT_PIVOT.date()} ratio vs baseline: {ratio:.2f} (target ~1.9x)")
        event_win = nvda[(nvda['date'] >= EVENT_START) & (nvda['date'] <= EVENT_END)]
        q_drop = (float(event_win['earnings_quality_score'].mean()) / float(base_window['earnings_quality_score'].mean()))
        print(f"- NVDA earnings_quality drop vs baseline: {q_drop:.2f} (target ~0.78x)")

    # Orders/executions event slippage and marketable share
    # Ensure 'date' is datetime before comparisons
    orders_execs['date'] = pd.to_datetime(orders_execs['date'], errors='coerce').dt.floor('ms')
    # Ensure 'date' is tz-naive and ms precision before comparisons
    orders_execs['date'] = pd.to_datetime(orders_execs['date'], errors='coerce').dt.tz_localize(None).dt.floor('ms')
    ev_mask = (orders_execs['date'] >= EVENT_START) & (orders_execs['date'] <= EVENT_END)
    base_mask = (orders_execs['date'] < EVENT_START)
    avg_slip_event = float(orders_execs.loc[ev_mask, 'slippage_bps'].mean())
    avg_slip_base = float(orders_execs.loc[base_mask, 'slippage_bps'].mean())
    print(f"- Slippage baseline ~{avg_slip_base:.1f} bps; event ~{avg_slip_event:.1f} bps (target ~18 bps)")

    # Breach clustering check
    # Ensure breaches['date'] is datetime ms-precision before between
    breaches['date'] = pd.to_datetime(breaches['date'], errors='coerce').dt.tz_localize(None).dt.floor('ms')
    gr = breaches[(breaches['sleeve'] == 'Growth-US') & (breaches['date'].between(EVENT_PIVOT, EVENT_PIVOT + pd.Timedelta(days=2)))]
    print(f"- Growth-US VAR/gamma entries during 2025-06-18..20: {len(gr):,} rows; sample day counts: {gr['breach_count'].head(6).tolist()}")

    print('\nAll timestamps are naive and floored to ms. Generation complete.')
