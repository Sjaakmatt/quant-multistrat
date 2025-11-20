// src/main.rs

use std::env;

use chrono::Utc;
use serde::Serialize;

use engine::risk::{
    GlobalRiskKernel,
    GlobalRiskKernelConfig,
    default_global_risk_kernel_config_usd_10k,
    aggressive_25k_global_risk_kernel_config,
    default_kernel_10k,
    aggressive_kernel_25k,
    SleeveId,
    SleeveRiskEnvelope,
    PortfolioState,
    SleeveState,
    MarginState,
    VolatilityRegime,
};

#[derive(Debug, Clone, Copy)]
enum RiskProfile {
    Starter10k,
    Aggressive25k,
}

impl RiskProfile {
    fn from_env() -> Self {
        match env::var("RISK_PROFILE") {
            Ok(val) => match val.as_str() {
                "aggressive_25k" => RiskProfile::Aggressive25k,
                // eventueel later uitbreiden met meer profielen
                _ => RiskProfile::Starter10k,
            },
            Err(_) => RiskProfile::Starter10k,
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            RiskProfile::Starter10k => "starter_10k",
            RiskProfile::Aggressive25k => "aggressive_25k",
        }
    }
}

fn build_kernel(profile: RiskProfile) -> (GlobalRiskKernel, GlobalRiskKernelConfig) {
    match profile {
        RiskProfile::Starter10k => {
            let cfg = default_global_risk_kernel_config_usd_10k();
            let kernel = default_kernel_10k();
            (kernel, cfg)
        }
        RiskProfile::Aggressive25k => {
            let cfg = aggressive_25k_global_risk_kernel_config();
            let kernel = aggressive_kernel_25k();
            (kernel, cfg)
        }
    }
}

#[derive(Serialize)]
struct EnvelopeSnapshot {
    ts_utc: i64,
    profile: &'static str,

    sleeve_id: String,
    max_position_size_usd: f64,
    exposure_remaining_usd: f64,
    margin_remaining_usd: f64,
    max_concurrent_positions: u32,
    portfolio_risk_state: String,
}

fn run_once_demo(profile: RiskProfile) -> Result<(), Box<dyn std::error::Error>> {
    // 1) Kernel + bijbehorende config uit profiel
    let (mut kernel, cfg) = build_kernel(profile);

    // 2) Portfolio state afleiden van config
    let eq = cfg.portfolio.initial_equity_usd;

    let portfolio_state = PortfolioState {
        cash_usd: eq,
        open_pnl_usd: 0.0,
        accrued_interest_usd: 0.0,
        peak_equity_usd: eq,
        total_notional_exposure: 0.0,
        current_leverage: 0.0,
    };

    // 3) Sleeves-state uit de config (één entry per SleeveRiskConfig)
    let mut sleeves_state: Vec<SleeveState> = cfg
        .sleeves
        .iter()
        .map(|s_cfg| SleeveState {
            sleeve_id: s_cfg.sleeve_id,
            equity_usd: s_cfg.capital_alloc_usd,
            realized_pnl_usd: 0.0,
            unrealized_pnl_usd: 0.0,
            peak_equity_usd: s_cfg.capital_alloc_usd,
            open_positions: 0,
        })
        .collect();

    // 4) Margin + vol-regime: neutrale start
    let margin_state = MarginState {
        internal_margin_req_usd: 0.0,
        broker_margin_req_usd: 0.0,
        equity_usd: eq,
    };

    let vol_regime = VolatilityRegime {
        rv10_annualized: 12.0,
        vix_level: 18.0,
        vix_term_slope: 0.3,
        regime_scalar: 1.0,
    };

    // 5) Kernel evalueren → envelopes
    let now_ts = Utc::now().timestamp();

    let envelopes: Vec<SleeveRiskEnvelope> = kernel.evaluate(
        now_ts,
        &portfolio_state,
        &mut sleeves_state,
        &margin_state,
        &vol_regime,
    );

    let env = envelopes
        .into_iter()
        .find(|e| e.sleeve_id == SleeveId::MicroFuturesMacroTrend)
        .ok_or("No envelope for SleeveId::MicroFuturesMacroTrend")?;

    // 6) Snapshot → JSON
    let snapshot = EnvelopeSnapshot {
        ts_utc: now_ts,
        profile: profile.as_str(),
        sleeve_id: format!("{:?}", env.sleeve_id),
        max_position_size_usd: env.max_position_size_usd,
        exposure_remaining_usd: env.exposure_remaining_usd,
        margin_remaining_usd: env.margin_remaining_usd,
        max_concurrent_positions: env.max_concurrent_positions,
        portfolio_risk_state: format!("{:?}", env.portfolio_risk_state),
    };

    let json = serde_json::to_string(&snapshot)?;
    println!("{json}");

    Ok(())
}

fn main() {
    let profile = RiskProfile::from_env();

    if let Err(err) = run_once_demo(profile) {
        eprintln!("run_once_demo error: {err}");
        std::process::exit(1);
    }
}
