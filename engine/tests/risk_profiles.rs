// tests/risk_profiles.rs

use engine::risk::{
    // aannames: via risk::mod.rs re-export je dit:
    default_global_risk_kernel_config_usd_10k,
    default_kernel_10k,
    GlobalRiskKernel,
    GlobalRiskKernelConfig,
    PortfolioRiskConfig,
    SleeveRiskConfig,
    SleeveId,
    aggressive_25k_global_risk_kernel_config,
    aggressive_kernel_25k,
};

fn approx_eq(a: f64, b: f64, eps: f64) -> bool {
    (a - b).abs() <= eps
}

#[test]
fn profile_10k_capital_alloc_equals_initial_equity() {
    let cfg: GlobalRiskKernelConfig = default_global_risk_kernel_config_usd_10k();

    let total_sleeve_capital: f64 = cfg
        .sleeves
        .iter()
        .map(|s: &SleeveRiskConfig| s.capital_alloc_usd)
        .sum();

    let initial = cfg.portfolio.initial_equity_usd;

    assert!(
        approx_eq(total_sleeve_capital, initial, 1e-6),
        "Sum of sleeve capital ({}) must equal portfolio.initial_equity_usd ({}).",
        total_sleeve_capital,
        initial
    );
}

#[test]
fn profile_10k_drawdown_limits_are_consistent() {
    let cfg: GlobalRiskKernelConfig = default_global_risk_kernel_config_usd_10k();
    let pcfg: &PortfolioRiskConfig = &cfg.portfolio;

    // Portfolio DD: kill < halt < 0
    assert!(
        pcfg.kill_dd_frac < pcfg.halt_dd_frac,
        "Expected kill_dd_frac ({}) < halt_dd_frac ({})",
        pcfg.kill_dd_frac,
        pcfg.halt_dd_frac
    );
    assert!(
        pcfg.halt_dd_frac < 0.0,
        "Expected halt_dd_frac to be negative, got {}",
        pcfg.halt_dd_frac
    );
    assert!(
        pcfg.kill_dd_frac < 0.0,
        "Expected kill_dd_frac to be negative, got {}",
        pcfg.kill_dd_frac
    );

    // Per-sleeve DD: zelfde logica
    for s in &cfg.sleeves {
        assert!(
            s.kill_dd_frac < s.halt_dd_frac,
            "Sleeve {:?}: expected kill_dd_frac ({}) < halt_dd_frac ({})",
            s.sleeve_id,
            s.kill_dd_frac,
            s.halt_dd_frac
        );
        assert!(
            s.halt_dd_frac < 0.0,
            "Sleeve {:?}: expected halt_dd_frac < 0, got {}",
            s.sleeve_id,
            s.halt_dd_frac
        );
        assert!(
            s.kill_dd_frac < 0.0,
            "Sleeve {:?}: expected kill_dd_frac < 0, got {}",
            s.sleeve_id,
            s.kill_dd_frac
        );
    }
}

#[test]
fn profile_10k_position_risk_fractions_and_concurrency_are_sane() {
    let cfg: GlobalRiskKernelConfig = default_global_risk_kernel_config_usd_10k();

    for s in &cfg.sleeves {
        // 0 < max_single_pos_risk_frac <= 5%
        assert!(
            s.max_single_pos_risk_frac > 0.0,
            "Sleeve {:?}: expected max_single_pos_risk_frac > 0, got {}",
            s.sleeve_id,
            s.max_single_pos_risk_frac
        );
        assert!(
            s.max_single_pos_risk_frac <= 0.05,
            "Sleeve {:?}: max_single_pos_risk_frac too large ({}), > 5%",
            s.sleeve_id,
            s.max_single_pos_risk_frac
        );

        // Concurrency moet > 0 zijn
        assert!(
            s.max_concurrent_positions > 0,
            "Sleeve {:?}: expected max_concurrent_positions > 0, got {}",
            s.sleeve_id,
            s.max_concurrent_positions
        );
    }

    // Portfolio leverage moet minimaal 1.0 zijn
    let pcfg = &cfg.portfolio;
    assert!(
        pcfg.max_leverage >= 1.0,
        "Expected portfolio.max_leverage >= 1.0, got {}",
        pcfg.max_leverage
    );
}

#[test]
fn default_kernel_10k_initial_state_matches_profile() {
    // Deze helper veronderstelt dat je een convenience hebt:
    // pub fn default_kernel_10k() -> GlobalRiskKernel
    let kernel: GlobalRiskKernel = default_kernel_10k();
    let cfg: &GlobalRiskKernelConfig = &kernel.config;

    // internal_portfolio_peak_equity moet gelijk zijn aan initial_equity_usd
    let initial = cfg.portfolio.initial_equity_usd;

    assert!(
        approx_eq(kernel.internal_portfolio_peak_equity, initial, 1e-6),
        "Kernel internal_portfolio_peak_equity ({}) must equal portfolio.initial_equity_usd ({})",
        kernel.internal_portfolio_peak_equity,
        initial
    );
}

#[test]
fn profile_10k_contains_micro_futures_sleeve() {
    let cfg: GlobalRiskKernelConfig = default_global_risk_kernel_config_usd_10k();

    let has_micro = cfg
        .sleeves
        .iter()
        .any(|s| s.sleeve_id == SleeveId::MicroFuturesMacroTrend);

    assert!(
        has_micro,
        "Expected default 10k profile to contain SleeveId::MicroFuturesMacroTrend"
    );
}

#[test]
fn profile_25k_capital_alloc_equals_initial_equity() {
    let cfg = aggressive_25k_global_risk_kernel_config();

    let total_alloc: f64 = cfg.sleeves.iter().map(|s| s.capital_alloc_usd).sum();

    assert!(
        (total_alloc - cfg.portfolio.initial_equity_usd).abs() < 1e-6,
        "Expected sum of sleeve capital allocs ({}) to equal portfolio initial_equity_usd ({}).",
        total_alloc,
        cfg.portfolio.initial_equity_usd
    );
}

#[test]
fn profile_25k_drawdown_limits_are_consistent() {
    let cfg = aggressive_25k_global_risk_kernel_config();

    // Portfolio DD moet strenger zijn dan de meest agressieve sleeve-kill
    assert!(
        cfg.portfolio.halt_dd_frac > cfg.portfolio.kill_dd_frac,
        "Portfolio halt_dd_frac must be > kill_dd_frac"
    );

    for s in &cfg.sleeves {
        assert!(
            s.halt_dd_frac > s.kill_dd_frac,
            "Sleeve {:?}: halt_dd_frac ({}) must be > kill_dd_frac ({})",
            s.sleeve_id,
            s.halt_dd_frac,
            s.kill_dd_frac
        );
    }
}

#[test]
fn profile_25k_position_risk_fractions_and_concurrency_are_sane() {
    let cfg = aggressive_25k_global_risk_kernel_config();

    for s in &cfg.sleeves {
        assert!(
            s.max_single_pos_risk_frac > 0.0 && s.max_single_pos_risk_frac <= 0.03,
            "Sleeve {:?}: max_single_pos_risk_frac out of sane range: {}",
            s.sleeve_id,
            s.max_single_pos_risk_frac
        );

        assert!(
            s.max_concurrent_positions > 0,
            "Sleeve {:?}: max_concurrent_positions must be > 0, got {}",
            s.sleeve_id,
            s.max_concurrent_positions
        );
    }
}

#[test]
fn aggressive_kernel_25k_initial_state_matches_profile() {
    let cfg = aggressive_25k_global_risk_kernel_config();
    let _kernel = aggressive_kernel_25k(); // alleen constructie checken

    assert_eq!(
        cfg.portfolio.initial_equity_usd,
        25_000.0,
        "Expected 25k initial equity in aggressive profile"
    );
}

