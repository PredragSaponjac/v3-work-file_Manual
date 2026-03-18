#!/usr/bin/env python3
"""
Lightweight wrapper: runs spy-regime model and saves prediction as JSON.
Called from GitHub Actions before analyst.py.
"""
import sys
import json
import os

def main():
    # Add spy-regime-bot to path — check local paths too
    script_dir = os.path.dirname(os.path.abspath(__file__))
    possible_paths = [
        os.path.join(script_dir, "spy-regime-bot"),                  # GitHub Actions checkout
        os.path.join(os.path.dirname(script_dir), "spy-regime-bot"), # sibling directory
        os.path.expanduser("~/Downloads/spy-regime-bot"),            # local dev
    ]
    for p in possible_paths:
        if os.path.isdir(p):
            sys.path.insert(0, p)
            break

    try:
        from spy_regime_v3_1_9_final import (
            yf_download_safe, build_features, TermStructure, SkewAnalyzer,
            train_cutpoints, apply_buckets, train_signal_map, PredictionEngine,
            simple_to_log_threshold, log_to_simple_pct,
            SPY_TICKER, SKEW_TICKER, TS_TICKERS, EXTRAS,
            DEFAULT_START, DEFAULT_TRAIN_END, DEFAULT_TEST_START,
            RECENT_YEARS_DEFAULT, N_BOOTSTRAP_DEFAULT
        )
        import pandas as pd
        import numpy as np
    except ImportError as e:
        print(f"⚠ spy-regime-bot not available: {e}")
        # Write empty regime file so analyst.py doesn't crash
        with open("regime_prediction.json", "w") as f:
            json.dump({"error": str(e)}, f)
        return

    print("📊 Running SPY Regime Model v3.1.9...")

    tickers = [SPY_TICKER, SKEW_TICKER] + TS_TICKERS + EXTRAS
    print(f"  Downloading {len(tickers)} tickers...")
    px, dropped = yf_download_safe(tickers, DEFAULT_START, None, verbose=False)
    df = build_features(px)

    # Validate core data
    core_vix = [t for t in ["^VIX9D", "^VIX", "^VIX3M"] if t in df.columns]
    if len(core_vix) < 2:
        print(f"  ⚠ Not enough VIX data: {core_vix}")
        with open("regime_prediction.json", "w") as f:
            json.dump({"error": f"insufficient VIX data: {core_vix}"}, f)
        return

    core_required = ["SPY", SKEW_TICKER] + core_vix
    df_core = df.dropna(subset=core_required).copy().sort_index()
    train_end_ts = pd.Timestamp(DEFAULT_TRAIN_END)
    df_train_core = df_core[df_core.index <= train_end_ts].copy()

    # PCA on VIX term structure
    ts = TermStructure().fit_pca(df_train_core, n_components=3, min_rows=200)
    pca = ts.transform(df_core)
    for c in pca.columns:
        df_core[c] = pca[c]

    df_core_valid = df_core.dropna(subset=["PC2_slope"]).copy()
    df_train_valid = df_core_valid[df_core_valid.index <= train_end_ts].copy()

    # SKEW analysis
    skew = SkewAnalyzer().fit(df_train_core)
    df_core_valid = skew.enrich(df_core_valid)

    # Bucketing
    slope_cps = train_cutpoints(df_train_valid["PC2_slope"].dropna(), 3)
    slope_scale = float(df_train_valid["PC2_slope"].dropna().std())
    skew_cps = train_cutpoints(df_train_core[SKEW_TICKER].dropna(), 3)
    skew_scale = float(df_train_core[SKEW_TICKER].dropna().std())

    bucket_specs = [
        {"col": "PC2_slope", "cps": slope_cps, "prefix": "slope"},
        {"col": SKEW_TICKER, "cps": skew_cps, "prefix": "skew"},
    ]

    df_all = apply_buckets(df_core_valid, bucket_specs).dropna(subset=["state"]).copy()
    df_train = df_all[df_all.index <= train_end_ts].copy()
    if "TAIL5_2" not in df_all.columns:
        df_all["TAIL5_2"] = (df_all["fwd_5d"] < simple_to_log_threshold(-0.02)).astype(float)
        df_train["TAIL5_2"] = df_all.loc[df_train.index, "TAIL5_2"]

    signal_map = train_signal_map(df_train, min_cell=80)

    base_med15 = log_to_simple_pct(np.median(df_all["fwd_15d"].dropna().values))
    base_pup15 = float(np.mean(df_all["fwd_15d"].dropna().values > 0))
    base_tail2 = float(np.mean(df_all["TAIL5_2"].dropna().values))
    base_stats = {"med15": base_med15, "pup15": base_pup15, "tail2": base_tail2}

    latest = df_all.dropna(subset=["state"]).iloc[-1]
    engine = PredictionEngine(
        signal_map=signal_map, train_end=DEFAULT_TRAIN_END, test_start=DEFAULT_TEST_START,
        skew_analyzer=skew, slope_cps=slope_cps, slope_scale=slope_scale,
        skew_cps=skew_cps, skew_scale=skew_scale, base_stats=base_stats,
        recent_years=RECENT_YEARS_DEFAULT, n_boot=N_BOOTSTRAP_DEFAULT
    )

    pred = engine.predict(df_all, latest)

    # Extract the key fields for ORCA
    bias = pred.get("bias", {})
    skew_data = pred.get("skew", {})
    tails = pred.get("tails", {})

    # Get tail probabilities from the RECENT or ALL slice
    tail_probs = {}
    for slice_name in ["RECENT", "ALL", "TRAIN"]:
        if slice_name in tails:
            tail_probs = tails[slice_name]
            break

    # Extract multi-horizon forward returns (use RECENT first, fallback to TRAIN)
    distributions = pred.get("distributions", {})
    forward_returns = {}
    for horizon in [3, 5, 15, 30]:
        for slice_name in ["RECENT", "ALL", "TRAIN"]:
            if slice_name in distributions and horizon in distributions[slice_name]:
                d = distributions[slice_name][horizon]
                forward_returns[f"med_{horizon}d"] = d.get("median_pct", 0)
                forward_returns[f"pup_{horizon}d"] = d.get("p_up", 0)
                break

    regime_output = {
        "date": pred.get("date", ""),
        "state": pred.get("state", ""),
        "signal": pred.get("signal", "NEUTRAL"),
        "bias": bias.get("label", "NEUTRAL"),
        "bias_confidence": bias.get("confidence", 0),
        "conviction": pred.get("conviction", 0),
        "hedge": pred.get("hedge", "MODERATE"),
        "action": pred.get("action", ""),
        "train_med15": pred.get("train_med15", 0),
        "train_med30": pred.get("train_med30", 0),
        "forward_returns": forward_returns,
        "skew_level": skew_data.get("skew", 0),
        "skew_regime": skew_data.get("regime", "NORMAL"),
        "skew_z": skew_data.get("z_train", 0),
        "tail_prob_2pct_5d": tail_probs.get("-2%", 0),
        "tail_prob_5pct_5d": tail_probs.get("-5%", 0),
        "immediate_triggers": pred.get("immediate_triggers", []),
        "nearest_long": pred.get("nearest_long", []),
    }

    # Save to JSON
    with open("regime_prediction.json", "w") as f:
        json.dump(regime_output, f, indent=2, default=str)

    # Print summary
    print(f"  ✅ Regime: STATE={regime_output['state']} | "
          f"SIGNAL={regime_output['signal']} | "
          f"BIAS={regime_output['bias']}({regime_output['bias_confidence']}%) | "
          f"HEDGE={regime_output['hedge']} | "
          f"Med15d={regime_output['train_med15']:+.2f}%")
    print(f"  📁 Saved: regime_prediction.json")


if __name__ == "__main__":
    main()
