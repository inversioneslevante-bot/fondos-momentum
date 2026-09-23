"""
Deep Quantitative Research: UCITS Mutual Fund Strategy Analysis
No lookahead bias: at month T, only data from months <= T-1 is used.
"""

import sqlite3
import pandas as pd
import numpy as np
from collections import defaultdict
import warnings
warnings.filterwarnings('ignore')

import os
DB_PATH = os.environ.get(
    "FONDOS_DB_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "cache.db"),
)

# ─────────────────────────────────────────────
# 1. LOAD ALL DATA
# ─────────────────────────────────────────────
print("=" * 70)
print("LOADING DATA")
print("=" * 70)

conn = sqlite3.connect(DB_PATH)

nav_df = pd.read_sql_query(
    "SELECT isin, year_month, nav_value, return_pct FROM monthly_nav ORDER BY year_month, isin",
    conn
)
funds_df = pd.read_sql_query(
    "SELECT isin, name, category_morningstar, category_mediolanum FROM funds",
    conn
)
conn.close()

print(f"monthly_nav rows   : {len(nav_df):,}")
print(f"Unique ISINs       : {nav_df['isin'].nunique()}")
print(f"Date range         : {nav_df['year_month'].min()} → {nav_df['year_month'].max()}")
print(f"Funds with metadata: {len(funds_df)}")
print(f"NULL return_pct    : {nav_df['return_pct'].isna().sum()}")

# Build a wide pivot: rows = year_month, cols = isin, values = return_pct
ret_wide = nav_df.pivot(index='year_month', columns='isin', values='return_pct')
ret_wide = ret_wide.sort_index()
all_months = list(ret_wide.index)
print(f"\nTotal months in pivot: {len(all_months)}  ({all_months[0]} → {all_months[-1]})")

# ─────────────────────────────────────────────
# 2. HELPER: BENCHMARK (equal-weight all funds each month)
# ─────────────────────────────────────────────
benchmark_monthly = ret_wide.mean(axis=1, skipna=True)  # equal-weight, skip NaN
print(f"\nBenchmark (equal-weight) descriptive stats:")
print(f"  Mean monthly return : {benchmark_monthly.mean():.4f}%")
print(f"  Std monthly return  : {benchmark_monthly.std():.4f}%")
print(f"  Min monthly return  : {benchmark_monthly.min():.4f}%")
print(f"  Max monthly return  : {benchmark_monthly.max():.4f}%")

# ─────────────────────────────────────────────
# 3. STRATEGY SIMULATION ENGINE
# ─────────────────────────────────────────────

def compound_return(returns_series):
    """Compute compound return from a list/array of percent returns."""
    r = np.array(returns_series, dtype=float)
    r = r[~np.isnan(r)]
    if len(r) == 0:
        return np.nan
    return np.prod(1 + r / 100) * 100 - 100


def get_available_isins(month_idx, min_months=1):
    """ISINs that have at least min_months of return data ending at month_idx-1."""
    if month_idx < 1:
        return []
    available = ret_wide.iloc[:month_idx].notna().sum()
    return list(available[available >= min_months].index)


def simulate_strategy(selection_fn, start_month_idx=1, top_n=5):
    """
    Generic strategy runner.
    selection_fn(month_idx, all_months, ret_wide, funds_df, benchmark_monthly)
        -> list of ISINs (or empty list = no position this month)

    Returns a Series of monthly returns aligned to benchmark_monthly index.
    """
    monthly_returns = {}
    for t in range(start_month_idx, len(all_months)):
        current_month = all_months[t]
        selected = selection_fn(t, all_months, ret_wide, funds_df, benchmark_monthly)
        if not selected:
            monthly_returns[current_month] = np.nan
        else:
            # Equal-weight portfolio return at month T
            rets = [ret_wide.loc[current_month, isin]
                    for isin in selected
                    if isin in ret_wide.columns and not pd.isna(ret_wide.loc[current_month, isin])]
            if rets:
                monthly_returns[current_month] = np.mean(rets)
            else:
                monthly_returns[current_month] = np.nan
    return pd.Series(monthly_returns)


def compute_metrics(strategy_returns, benchmark_returns, label=""):
    """
    Compute full performance metrics for a monthly return series.
    strategy_returns: pd.Series (% returns per month)
    benchmark_returns: pd.Series (% returns per month)
    """
    s = strategy_returns.dropna()
    b = benchmark_returns.reindex(s.index).dropna()

    # Align both to common dates
    common = s.index.intersection(b.index)
    s = s.loc[common]
    b = b.loc[common]

    n = len(s)
    if n == 0:
        return {label: "NO DATA"}

    # Total return (compound)
    total_ret = compound_return(s)

    # CAGR
    years = n / 12
    cagr = ((1 + total_ret / 100) ** (1 / years) - 1) * 100 if years > 0 else np.nan

    # Sharpe (monthly, annualised)
    mean_m = s.mean()
    std_m = s.std(ddof=1)
    sharpe = (mean_m / std_m * np.sqrt(12)) if std_m > 0 else np.nan

    # Max drawdown
    cum = (1 + s / 100).cumprod()
    roll_max = cum.cummax()
    drawdown = (cum - roll_max) / roll_max * 100
    max_dd = drawdown.min()

    # Win rate vs benchmark
    wins = (s > b).sum()
    win_rate = wins / n * 100

    # Avg monthly outperformance
    avg_outperf = (s - b).mean()

    # Benchmark metrics for reference
    bench_total = compound_return(b)
    bench_cagr = ((1 + bench_total / 100) ** (1 / years) - 1) * 100 if years > 0 else np.nan

    return {
        'n_months': n,
        'total_ret': total_ret,
        'cagr': cagr,
        'sharpe': sharpe,
        'max_dd': max_dd,
        'win_rate': win_rate,
        'avg_outperf': avg_outperf,
        'bench_total': bench_total,
        'bench_cagr': bench_cagr,
        'mean_monthly': mean_m,
        'std_monthly': std_m,
        'returns': s,
        'benchmark': b,
    }


def annual_breakdown(strategy_returns, benchmark_returns):
    """Year-by-year annual returns."""
    s = strategy_returns.dropna()
    b = benchmark_returns.reindex(s.index)
    data = []
    for yr in sorted(set(m[:4] for m in s.index)):
        months = [m for m in s.index if m.startswith(yr)]
        sr = compound_return(s.loc[months])
        br = compound_return(b.loc[months].dropna())
        data.append({'year': yr, 'strategy': sr, 'benchmark': br, 'alpha': sr - br if not np.isnan(sr) and not np.isnan(br) else np.nan})
    return pd.DataFrame(data).set_index('year')


# ─────────────────────────────────────────────
# 4. DEFINE ALL 10 STRATEGIES
# ─────────────────────────────────────────────

# ── S1: 1-month momentum ──
def s1_select(t, months, ret, funds, bench):
    prev = months[t - 1]
    row = ret.loc[prev].dropna()
    if len(row) < 5:
        return []
    return list(row.nlargest(5).index)


# ── S2: 3-month momentum ──
def s2_select(t, months, ret, funds, bench):
    if t < 3:
        return []
    window = months[t - 3:t]
    sub = ret.loc[window].dropna(axis=1, how='any')
    if sub.shape[1] < 5:
        return []
    comp = sub.apply(lambda col: compound_return(col))
    return list(comp.nlargest(5).index)


# ── S3: 6-month momentum ──
def s3_select(t, months, ret, funds, bench):
    if t < 6:
        return []
    window = months[t - 6:t]
    sub = ret.loc[window].dropna(axis=1, how='any')
    if sub.shape[1] < 5:
        return []
    comp = sub.apply(lambda col: compound_return(col))
    return list(comp.nlargest(5).index)


# ── S4: 12-month momentum ──
def s4_select(t, months, ret, funds, bench):
    if t < 12:
        return []
    window = months[t - 12:t]
    sub = ret.loc[window].dropna(axis=1, how='any')
    if sub.shape[1] < 5:
        return []
    comp = sub.apply(lambda col: compound_return(col))
    return list(comp.nlargest(5).index)


# ── S5: Risk-adjusted (Sharpe proxy) over last 6 months, min 4 months data ──
def s5_select(t, months, ret, funds, bench):
    if t < 6:
        return []
    window = months[t - 6:t]
    sub = ret.loc[window]
    # Require at least 4 non-NaN months per fund
    valid = sub.dropna(thresh=4, axis=1)
    if valid.shape[1] < 5:
        return []
    mean_r = valid.mean()
    std_r = valid.std(ddof=1)
    sharpe_proxy = mean_r / std_r.replace(0, np.nan)
    sharpe_proxy = sharpe_proxy.dropna()
    if len(sharpe_proxy) < 5:
        return []
    return list(sharpe_proxy.nlargest(5).index)


# ── S6: Consistency — top 20% appearances over last 6 months ──
def s6_select(t, months, ret, funds, bench):
    if t < 6:
        return []
    window = months[t - 6:t]
    sub = ret.loc[window]
    # Count how many times each fund was in top 20% in each month
    counts = pd.Series(0, index=sub.columns)
    for m in window:
        row = sub.loc[m].dropna()
        if len(row) < 5:
            continue
        threshold = row.quantile(0.80)
        top20_pct = row[row >= threshold].index
        counts.loc[counts.index.intersection(top20_pct)] += 1
    counts = counts[counts > 0]
    if len(counts) < 5:
        return []
    return list(counts.nlargest(5).index)


# ── S7: Momentum acceleration — last month > own 3m avg AND top 30% ──
def s7_select(t, months, ret, funds, bench):
    if t < 3:
        return []
    last_month = months[t - 1]
    window3 = months[t - 3:t]
    last_ret = ret.loc[last_month].dropna()
    sub3 = ret.loc[window3]
    mean3 = sub3.mean()  # 3-month mean for each fund (NaN if no data)
    # Funds where last month > their own 3m average (acceleration)
    accel = last_ret > mean3.reindex(last_ret.index)
    accel_isins = accel[accel].index
    if len(accel_isins) == 0:
        return []
    # Among those, pick top 30% overall by last month return
    threshold_30 = last_ret.quantile(0.70)
    top30 = last_ret[last_ret >= threshold_30].index
    candidates = accel_isins.intersection(top30)
    if len(candidates) < 5:
        return []
    # Pick top 5 by last month return
    return list(last_ret.loc[candidates].nlargest(5).index)


# ── S8: Low-vol momentum — top 30 by 3m return, then lowest vol ──
def s8_select(t, months, ret, funds, bench):
    if t < 6:
        return []
    window3 = months[t - 3:t]
    window6 = months[t - 6:t]
    sub3 = ret.loc[window3].dropna(axis=1, how='any')
    if sub3.shape[1] < 30:
        return []
    comp3 = sub3.apply(compound_return)
    top30_isins = comp3.nlargest(30).index
    sub6 = ret.loc[window6][top30_isins]
    vol6 = sub6.std(ddof=1)
    vol6 = vol6.dropna()
    if len(vol6) < 5:
        return []
    return list(vol6.nsmallest(5).index)


# ── S9: Category rotation ──
def s9_select(t, months, ret, funds, bench):
    if t < 3:
        return []
    last_month = months[t - 1]
    # Category avg return last month
    row = ret.loc[last_month].dropna().reset_index()
    row.columns = ['isin', 'return_pct']
    merged = row.merge(funds[['isin', 'category_morningstar']], on='isin', how='left')
    merged = merged.dropna(subset=['category_morningstar'])
    if merged.empty:
        return []
    cat_avg = merged.groupby('category_morningstar')['return_pct'].mean()
    top2_cats = cat_avg.nlargest(2).index.tolist()
    # 3-month compound return for funds in those categories
    window3 = months[t - 3:t]
    sub3 = ret.loc[window3].dropna(axis=1, how='any')
    comp3 = sub3.apply(compound_return)
    # Filter to funds in top2_cats
    cat_isins = merged[merged['category_morningstar'].isin(top2_cats)]['isin'].tolist()
    candidates = comp3.reindex(cat_isins).dropna()
    if len(candidates) < 5:
        # Fall back to whatever we have (at least 1)
        if len(candidates) == 0:
            return []
        return list(candidates.nlargest(min(5, len(candidates))).index)
    return list(candidates.nlargest(5).index)


# ── S10: Anti-crash filter (S1 with down-market adjustment) ──
# We track the previous selection from S1
_s10_prev_selection = {}

def s10_select(t, months, ret, funds, bench):
    prev = months[t - 1]
    row = ret.loc[prev].dropna()
    if len(row) < 5:
        return []
    top5 = list(row.nlargest(5).index)
    # Check if last month benchmark was < -2%
    if t >= 2:
        bench_prev = bench.get(prev, np.nan) if hasattr(bench, 'get') else bench.loc[prev] if prev in bench.index else np.nan
        if not np.isnan(bench_prev) and bench_prev < -2.0:
            # Drop the worst performer from last month's top5 and replace
            # with best performer from the month before that (T-2 top selection)
            prev_prev = months[t - 2]
            row_pp = ret.loc[prev_prev].dropna()
            if len(row_pp) >= 1:
                # Remove worst from current top5
                def get_val(x):
                    return row.loc[x] if x in row.index else -999
                worst_of_top5 = min(top5, key=get_val)
                top5.remove(worst_of_top5)
                # Add best from T-2 (not already in top5)
                row_pp_sorted = row_pp.sort_values(ascending=False)
                for isin in row_pp_sorted.index:
                    if isin not in top5:
                        top5.append(isin)
                        break
    return top5


# ─────────────────────────────────────────────
# 5. RUN ALL STRATEGIES
# ─────────────────────────────────────────────
print("\n" + "=" * 70)
print("RUNNING STRATEGIES (this may take a moment...)")
print("=" * 70)

strategies = {
    'S1_1m_momentum':    s1_select,
    'S2_3m_momentum':    s2_select,
    'S3_6m_momentum':    s3_select,
    'S4_12m_momentum':   s4_select,
    'S5_sharpe_proxy':   s5_select,
    'S6_consistency':    s6_select,
    'S7_accel':          s7_select,
    'S8_lowvol_mom':     s8_select,
    'S9_cat_rotation':   s9_select,
    'S10_anticrash':     s10_select,
}

results = {}
for name, fn in strategies.items():
    print(f"  Running {name}...", end='', flush=True)
    ret_series = simulate_strategy(fn, start_month_idx=13)  # Start at month 14 so everyone has 12m history
    metrics = compute_metrics(ret_series, benchmark_monthly, label=name)
    results[name] = metrics
    results[name]['series'] = ret_series
    print(f" done  (n={metrics['n_months']} months, CAGR={metrics['cagr']:.2f}%)")


# ─────────────────────────────────────────────
# 6. COMPARISON TABLE RANKED BY SHARPE
# ─────────────────────────────────────────────
print("\n" + "=" * 70)
print("STRATEGY COMPARISON TABLE (ranked by Sharpe ratio)")
print("=" * 70)

rows = []
for name, m in results.items():
    rows.append({
        'Strategy': name,
        'N Months': m['n_months'],
        'Total Ret%': round(m['total_ret'], 2),
        'CAGR%': round(m['cagr'], 2),
        'Sharpe': round(m['sharpe'], 3),
        'Max DD%': round(m['max_dd'], 2),
        'Win Rate%': round(m['win_rate'], 1),
        'Avg Outperf%': round(m['avg_outperf'], 4),
        'Mean M Ret%': round(m['mean_monthly'], 4),
        'Std M Ret%': round(m['std_monthly'], 4),
    })

table_df = pd.DataFrame(rows).sort_values('Sharpe', ascending=False).reset_index(drop=True)
table_df.index += 1  # 1-based rank

# Print aligned table
col_widths = {
    'Strategy': 22,
    'N Months': 9,
    'Total Ret%': 11,
    'CAGR%': 7,
    'Sharpe': 7,
    'Max DD%': 8,
    'Win Rate%': 10,
    'Avg Outperf%': 13,
    'Mean M Ret%': 12,
    'Std M Ret%': 11,
}

header = f"{'Rank':>4}  " + "  ".join(f"{c:<{col_widths[c]}}" for c in col_widths)
print(header)
print("-" * len(header))
for rank, row in table_df.iterrows():
    line = f"{rank:>4}  " + "  ".join(f"{str(row[c]):>{col_widths[c]}}" for c in col_widths)
    print(line)

# Also print benchmark stats for reference
print(f"\nBENCHMARK REFERENCE (equal-weight, same period):")
# Use the period covered by S1 (same n_months as strategies)
bench_ref = benchmark_monthly.reindex(results['S1_1m_momentum']['returns'].index).dropna()
bench_total = compound_return(bench_ref)
bench_years = len(bench_ref) / 12
bench_cagr = ((1 + bench_total / 100) ** (1 / bench_years) - 1) * 100
bench_sharpe = bench_ref.mean() / bench_ref.std(ddof=1) * np.sqrt(12)
bench_cum = (1 + bench_ref / 100).cumprod()
bench_dd = ((bench_cum - bench_cum.cummax()) / bench_cum.cummax() * 100).min()
print(f"  N Months: {len(bench_ref)}  |  Total Ret: {bench_total:.2f}%  |  CAGR: {bench_cagr:.2f}%  |  Sharpe: {bench_sharpe:.3f}  |  Max DD: {bench_dd:.2f}%")


# ─────────────────────────────────────────────
# 7. YEAR-BY-YEAR BREAKDOWN FOR TOP 3 STRATEGIES
# ─────────────────────────────────────────────
top3 = list(table_df['Strategy'].iloc[:3])
print("\n" + "=" * 70)
print(f"YEAR-BY-YEAR PERFORMANCE — TOP 3 STRATEGIES")
print("=" * 70)

for strat in top3:
    m = results[strat]
    yby = annual_breakdown(m['series'], benchmark_monthly)
    print(f"\n  {strat}")
    print(f"  {'Year':<6}  {'Strategy%':>10}  {'Benchmark%':>11}  {'Alpha%':>8}")
    print(f"  {'-'*6}  {'-'*10}  {'-'*11}  {'-'*8}")
    for yr, row in yby.iterrows():
        s_str = f"{row['strategy']:.2f}" if not np.isnan(row['strategy']) else "  N/A"
        b_str = f"{row['benchmark']:.2f}" if not np.isnan(row['benchmark']) else "  N/A"
        a_str = f"{row['alpha']:.2f}" if not np.isnan(row['alpha']) else "  N/A"
        print(f"  {yr:<6}  {s_str:>10}  {b_str:>11}  {a_str:>8}")

    total_alpha = m['total_ret'] - m['bench_total']
    print(f"  {'TOTAL':<6}  {m['total_ret']:>10.2f}  {m['bench_total']:>11.2f}  {total_alpha:>8.2f}")


# ─────────────────────────────────────────────
# 8. AUTOCORRELATION ANALYSIS
# ─────────────────────────────────────────────
print("\n" + "=" * 70)
print("AUTOCORRELATION ANALYSIS — Does last month's winner win next month?")
print("=" * 70)

# For each month T, rank funds by return at T and T-1; correlate
from scipy.stats import spearmanr, pearsonr

lag1_corrs = []
lag1_months = []
for t in range(1, len(all_months)):
    prev = all_months[t - 1]
    curr = all_months[t]
    prev_row = ret_wide.loc[prev].dropna()
    curr_row = ret_wide.loc[curr].dropna()
    common_isins = prev_row.index.intersection(curr_row.index)
    if len(common_isins) < 20:
        continue
    r_pearson, _ = pearsonr(prev_row[common_isins], curr_row[common_isins])
    r_spearman, _ = spearmanr(prev_row[common_isins], curr_row[common_isins])
    lag1_corrs.append({'month': curr, 'pearson': r_pearson, 'spearman': r_spearman})

corr_df = pd.DataFrame(lag1_corrs)
print(f"\n  Lag-1 Pearson correlation  (return_T vs return_T-1):")
print(f"    Mean : {corr_df['pearson'].mean():.4f}")
print(f"    Median: {corr_df['pearson'].median():.4f}")
print(f"    Std  : {corr_df['pearson'].std():.4f}")
print(f"    % Positive months: {(corr_df['pearson'] > 0).mean() * 100:.1f}%")

print(f"\n  Lag-1 Spearman rank correlation (rank_T vs rank_T-1):")
print(f"    Mean : {corr_df['spearman'].mean():.4f}")
print(f"    Median: {corr_df['spearman'].median():.4f}")
print(f"    Std  : {corr_df['spearman'].std():.4f}")
print(f"    % Positive months: {(corr_df['spearman'] > 0).mean() * 100:.1f}%")

# Top winner persistence: does the top decile at T-1 beat the bottom decile at T?
top_decile_beats = []
for t in range(1, len(all_months)):
    prev = all_months[t - 1]
    curr = all_months[t]
    prev_row = ret_wide.loc[prev].dropna()
    curr_row = ret_wide.loc[curr].dropna()
    common = prev_row.index.intersection(curr_row.index)
    if len(common) < 20:
        continue
    top10_pct_isins = prev_row[common].nlargest(int(len(common) * 0.1)).index
    bot10_pct_isins = prev_row[common].nsmallest(int(len(common) * 0.1)).index
    mean_top = curr_row[top10_pct_isins].mean()
    mean_bot = curr_row[bot10_pct_isins].mean()
    top_decile_beats.append(mean_top > mean_bot)

pct_top_beats = np.mean(top_decile_beats) * 100
print(f"\n  Top-decile persistence: top 10% last month beats bottom 10% next month")
print(f"    {pct_top_beats:.1f}% of months ({sum(top_decile_beats)}/{len(top_decile_beats)})")
print(f"  Interpretation: {'Moderate momentum signal present' if pct_top_beats > 55 else 'Weak or no momentum signal'}")


# ─────────────────────────────────────────────
# 9. CATEGORY MOMENTUM ANALYSIS
# ─────────────────────────────────────────────
print("\n" + "=" * 70)
print("CATEGORY MOMENTUM ANALYSIS")
print("=" * 70)

# For each category, compute avg monthly return and momentum persistence
isin_to_cat = funds_df.set_index('isin')['category_morningstar'].to_dict()

cat_monthly_rets = defaultdict(list)
for month in all_months:
    row = ret_wide.loc[month].dropna()
    for isin, r in row.items():
        cat = isin_to_cat.get(isin, None)
        if cat:
            cat_monthly_rets[cat].append(r)

cat_stats = []
for cat, rets in cat_monthly_rets.items():
    if len(rets) < 50:  # Require reasonable sample
        continue
    arr = np.array(rets)
    cat_stats.append({
        'Category': cat,
        'N obs': len(arr),
        'Mean%': round(arr.mean(), 4),
        'Std%': round(arr.std(), 4),
        'Sharpe': round(arr.mean() / arr.std() * np.sqrt(12), 3) if arr.std() > 0 else np.nan,
        'Skew': round(float(pd.Series(arr).skew()), 3),
    })

cat_df = pd.DataFrame(cat_stats).sort_values('Sharpe', ascending=False)

# Now compute category-level momentum persistence (lag-1 corr)
cat_lag1_corrs = {}
cats_with_enough = [row['Category'] for _, row in cat_df.iterrows() if row['N obs'] >= 100]

for cat in cats_with_enough:
    cat_isins = [i for i, c in isin_to_cat.items() if c == cat and i in ret_wide.columns]
    if len(cat_isins) < 3:
        continue
    cat_ret = ret_wide[cat_isins].mean(axis=1, skipna=True).dropna()
    if len(cat_ret) < 12:
        continue
    lag1_corr = cat_ret.autocorr(lag=1)
    cat_lag1_corrs[cat] = lag1_corr

cat_df['Lag1 AutoCorr'] = cat_df['Category'].map(cat_lag1_corrs).round(4)

print(f"\n  Top 15 categories by risk-adjusted return (Sharpe):")
print(f"  {'Category':<55}  {'N obs':>6}  {'Mean%':>7}  {'Std%':>6}  {'Sharpe':>7}  {'AutoCorr':>9}")
print(f"  {'-'*55}  {'-'*6}  {'-'*7}  {'-'*6}  {'-'*7}  {'-'*9}")
for _, row in cat_df.head(15).iterrows():
    cat_short = str(row['Category'])[:53]
    ac = f"{row['Lag1 AutoCorr']:.4f}" if not pd.isna(row['Lag1 AutoCorr']) else "  N/A"
    print(f"  {cat_short:<55}  {row['N obs']:>6}  {row['Mean%']:>7.4f}  {row['Std%']:>6.4f}  {row['Sharpe']:>7.3f}  {ac:>9}")

print(f"\n  Categories with HIGHEST momentum persistence (Lag-1 AutoCorr):")
sorted_by_ac = sorted(cat_lag1_corrs.items(), key=lambda x: x[1], reverse=True)
for cat, ac in sorted_by_ac[:10]:
    n_isins = len([i for i, c in isin_to_cat.items() if c == cat])
    print(f"    {cat[:60]:<60}  AutoCorr={ac:.4f}  (N funds={n_isins})")


# ─────────────────────────────────────────────
# 10. OPTIMAL LOOKBACK ANALYSIS
# ─────────────────────────────────────────────
print("\n" + "=" * 70)
print("OPTIMAL LOOKBACK PERIOD ANALYSIS (Top-5 momentum, monthly rebalancing)")
print("=" * 70)

lookback_periods = [1, 2, 3, 6, 9, 12]
lookback_results = {}

for lb in lookback_periods:
    def make_lb_fn(lookback):
        def fn(t, months, ret, funds, bench):
            if t < lookback:
                return []
            window = months[t - lookback:t]
            sub = ret.loc[window].dropna(axis=1, how='any')
            if sub.shape[1] < 5:
                return []
            if lookback == 1:
                comp = sub.iloc[0]
            else:
                comp = sub.apply(compound_return)
            return list(comp.nlargest(5).index)
        return fn

    fn = make_lb_fn(lb)
    ret_series = simulate_strategy(fn, start_month_idx=max(13, lb + 1))
    m = compute_metrics(ret_series, benchmark_monthly)
    lookback_results[lb] = m
    print(f"  Lookback {lb:>2}m: CAGR={m['cagr']:>7.2f}%  Sharpe={m['sharpe']:>6.3f}  MaxDD={m['max_dd']:>7.2f}%  WinRate={m['win_rate']:>5.1f}%  N={m['n_months']}")

best_lb = max(lookback_results.items(), key=lambda x: x[1]['sharpe'])
print(f"\n  Best lookback by Sharpe: {best_lb[0]} months (Sharpe={best_lb[1]['sharpe']:.3f})")
best_lb_cagr = max(lookback_results.items(), key=lambda x: x[1]['cagr'])
print(f"  Best lookback by CAGR  : {best_lb_cagr[0]} months (CAGR={best_lb_cagr[1]['cagr']:.2f}%)")


# ─────────────────────────────────────────────
# 11. ADDITIONAL INSIGHTS
# ─────────────────────────────────────────────
print("\n" + "=" * 70)
print("ADDITIONAL INSIGHTS")
print("=" * 70)

# Monthly return distribution of top strategy
best_strat = table_df['Strategy'].iloc[0]
best_m = results[best_strat]
best_s = best_m['returns']

print(f"\n  Best strategy ({best_strat}) monthly return distribution:")
pcts = [1, 5, 10, 25, 50, 75, 90, 95, 99]
for p in pcts:
    print(f"    p{p:>2}: {np.percentile(best_s, p):.3f}%")

# Turnover analysis: how often do the top-5 funds completely change?
print(f"\n  Selection stability (S2 3m momentum) — avg fund overlap month to month:")
prev_sel = None
overlaps = []
for t in range(13, len(all_months)):
    sel = s2_select(t, all_months, ret_wide, funds_df, benchmark_monthly)
    if prev_sel and sel:
        overlap = len(set(sel) & set(prev_sel))
        overlaps.append(overlap)
    prev_sel = sel

if overlaps:
    print(f"    Mean overlap (out of 5): {np.mean(overlaps):.2f}")
    print(f"    Turnover (avg new funds per month): {5 - np.mean(overlaps):.2f}")
    print(f"    % months with full turnover (0 overlap): {(np.array(overlaps) == 0).mean() * 100:.1f}%")
    print(f"    % months with zero turnover (same 5): {(np.array(overlaps) == 5).mean() * 100:.1f}%")

# S5 Sharpe proxy — are top-Sharpe funds truly less volatile?
print(f"\n  S5 Sharpe proxy — characteristics of selected funds:")
s5_selected_all = []
for t in range(13, len(all_months)):
    sel = s5_select(t, all_months, ret_wide, funds_df, benchmark_monthly)
    month = all_months[t]
    if sel:
        rets = [ret_wide.loc[month, i] for i in sel if i in ret_wide.columns and not pd.isna(ret_wide.loc[month, i])]
        if rets:
            s5_selected_all.extend(rets)

if s5_selected_all:
    print(f"    Mean monthly return when held: {np.mean(s5_selected_all):.4f}%")
    print(f"    Std of those returns: {np.std(s5_selected_all):.4f}%")

# Show cumulative growth comparison
print(f"\n  CUMULATIVE RETURNS COMPARISON (final value of 100 invested):")
print(f"  {'Strategy':<25}  {'Final Value':>12}  {'vs Benchmark':>14}")
bench_final = 100 * np.prod(1 + bench_ref / 100)
print(f"  {'Benchmark (equal-wt)':<25}  {bench_final:>12.2f}  {'(base)':>14}")
for name in [r['Strategy'] for _, r in table_df.iterrows()]:
    s = results[name]['series']
    common = s.dropna().index.intersection(bench_ref.index)
    if len(common) < 5:
        continue
    final_val = 100 * np.prod(1 + s.loc[common] / 100)
    diff = final_val - bench_final
    print(f"  {name:<25}  {final_val:>12.2f}  {diff:>+14.2f}")

print("\n" + "=" * 70)
print("ANALYSIS COMPLETE")
print("=" * 70)
