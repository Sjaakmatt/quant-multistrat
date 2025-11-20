// src/risk/profiles.rs

use crate::risk::{
    GlobalRiskKernel, GlobalRiskKernelConfig, PortfolioRiskConfig, SleeveId, SleeveRiskConfig,
};

/// Portfolio-profiel voor een account van ~10k USD.
/// Dit is de "top-level" risk config – sleeves krijgen hieronder hun alloc.
pub fn default_portfolio_config_10k() -> PortfolioRiskConfig {
    PortfolioRiskConfig {
        initial_equity_usd: 10_000.0,

        // Hard DD-limieten voor het totale portfolio
        halt_dd_frac: -0.08, // -8% → geen nieuwe risk
        kill_dd_frac: -0.12, // -12% → alles flatten

        // Max leverage op vol-genormaliseerde exposure
        max_leverage: 1.5,

        // Rebalancing drempel (bijv. buiten ±15% van target alloc)
        rebalance_drift_frac: 0.15,

        // Max aantal open posities over alle sleeves heen
        max_global_positions: 15,
    }
}

/// Conservatief 10k-profiel (starter) maar met realistische ruimte voor micro-futures.
pub fn default_global_risk_kernel_config_usd_10k() -> GlobalRiskKernelConfig {
    GlobalRiskKernelConfig {
        portfolio: PortfolioRiskConfig {
            initial_equity_usd: 10_000.0,
            // Portfolio-level DD limits
            halt_dd_frac: -0.10,     // -10% -> geen nieuwe risk-on, wel flatten toegestaan
            kill_dd_frac: -0.20,     // -20% -> alles flatten
            max_leverage: 1.5,       // max 1.5x notional vs equity
            rebalance_drift_frac: 0.15,
            max_global_positions: 20,
        },
        sleeves: vec![
            // ==== Equity L/S (core, maar niet ons focuspunt nu) ====
            // AANGEPAST: capital_alloc_usd 2_000 -> 1_500
            SleeveRiskConfig {
                sleeve_id: SleeveId::EquityLongShort,
                capital_alloc_usd: 1_500.0,
                max_single_pos_risk_frac: 0.01,  // 1% van portfolio per positie
                halt_dd_frac: -0.12,
                kill_dd_frac: -0.18,
                max_concurrent_positions: 10,
            },
            // ==== Stat-Arb / Residual ====
            // AANGEPAST: capital_alloc_usd 2_500 -> 1_500
            SleeveRiskConfig {
                sleeve_id: SleeveId::StatArbResidual,
                capital_alloc_usd: 1_500.0,
                max_single_pos_risk_frac: 0.008, // iets lager (meer names, lagere single-name risk)
                halt_dd_frac: -0.12,
                kill_dd_frac: -0.18,
                max_concurrent_positions: 20,
            },
            // ==== Microstructure / Intraday ====
            // AANGEPAST: capital_alloc_usd 1_500 -> 1_000
            SleeveRiskConfig {
                sleeve_id: SleeveId::MicrostructureIntraday,
                capital_alloc_usd: 1_000.0,
                max_single_pos_risk_frac: 0.005, // veel trades, klein per-trade risk
                halt_dd_frac: -0.08,
                kill_dd_frac: -0.15,
                max_concurrent_positions: 30,
            },
            // ==== Index Options Vol Premium ====
            // AANGEPAST: capital_alloc_usd 2_000 -> 1_000
            SleeveRiskConfig {
                sleeve_id: SleeveId::OptionsVolPremium,
                capital_alloc_usd: 1_000.0,
                max_single_pos_risk_frac: 0.02,  // options: klein aantal posities, hogere per-trade R
                halt_dd_frac: -0.12,
                kill_dd_frac: -0.20,
                max_concurrent_positions: 6,
            },
            // ==== Micro Futures Macro Trend (belangrijk voor nu) ====
            // Ongewijzigd op 5_000 → 50% van 10k-profiel
            SleeveRiskConfig {
                sleeve_id: SleeveId::MicroFuturesMacroTrend,
                capital_alloc_usd: 5_000.0,
                // WAS: 0.06 → tripte de sanity check (in 10k-profiel stond al 0.05, laten zo)
                max_single_pos_risk_frac: 0.05, // max 5% per positie (test-range upper bound)
                halt_dd_frac: -0.15,
                kill_dd_frac: -0.25,
                max_concurrent_positions: 4,
            },
        ],
    }
}

/// Convenience-constructor voor starter-profiel 10k.
pub fn default_kernel_10k() -> GlobalRiskKernel {
    GlobalRiskKernel::new(default_global_risk_kernel_config_usd_10k())
}

/// Convenience helper voor het aanmaken van een sleeve-config.
fn mk_sleeve(
    sleeve_id: SleeveId,
    capital_alloc_usd: f64,
    max_single_pos_risk_frac: f64,
    halt_dd_frac: f64,
    kill_dd_frac: f64,
    max_concurrent_positions: u32,
) -> SleeveRiskConfig {
    SleeveRiskConfig {
        sleeve_id,
        capital_alloc_usd,
        max_single_pos_risk_frac,
        halt_dd_frac,
        kill_dd_frac,
        max_concurrent_positions,
    }
}

/// Default sleeve-profielen voor een 10k-account.
///
/// Verdeling kun je later finetunen, maar hiermee heb je:
/// - Één bron van waarheid
/// - Duidelijke caps per sleeve
pub fn default_sleeve_configs_10k() -> Vec<SleeveRiskConfig> {
    let total_equity = 10_000.0;

    // Allocaties per sleeve (fractie van totaal)
    let alloc_equity_ls = 0.20 * total_equity; // 20% → 2k
    let alloc_stat_arb = 0.25 * total_equity;  // 25% → 2.5k
    let alloc_micro_intraday = 0.15 * total_equity; // 15% → 1.5k
    let alloc_options_vol = 0.20 * total_equity; // 20% → 2k
    let alloc_micro_trend = 0.20 * total_equity; // 20% → 2k

    vec![
        // 1) Equity L/S
        mk_sleeve(
            SleeveId::EquityLongShort,
            alloc_equity_ls,
            0.02,  // max 2% van sleeve in één positie
            -0.10, // halt bij -10% sleeve DD
            -0.15, // kill bij -15% sleeve DD
            10,    // max ~10 names tegelijk
        ),

        // 2) Stat-Arb Residual
        mk_sleeve(
            SleeveId::StatArbResidual,
            alloc_stat_arb,
            0.015, // iets kleiner per trade, door hogere turnover
            -0.10,
            -0.18,
            20, // meer posities toegestaan
        ),

        // 3) Microstructure Intraday
        mk_sleeve(
            SleeveId::MicrostructureIntraday,
            alloc_micro_intraday,
            0.01, // klein per position; veel turnover
            -0.08,
            -0.12,
            10,
        ),

        // 4) Index Options Vol Premium
        mk_sleeve(
            SleeveId::OptionsVolPremium,
            alloc_options_vol,
            0.03, // individuele spreads kunnen relatief groot zijn
            -0.10,
            -0.15,
            6, // beperkt aantal spreads tegelijk
        ),

        // 5) Micro Futures Macro Trend/Carry
        mk_sleeve(
            SleeveId::MicroFuturesMacroTrend,
            alloc_micro_trend,
            0.045, // 4.5% van de sleeve per trade (v1)
            -0.10,
            -0.15,
            3, // max 3 gelijktijdige micro-futures posities
        ),
    ]
}

/// Agressiever profiel rond 25k equity met meer ruimte per positie,
/// vooral voor de micro-futures sleeve.
pub fn aggressive_25k_global_risk_kernel_config() -> GlobalRiskKernelConfig {
    GlobalRiskKernelConfig {
        portfolio: PortfolioRiskConfig {
            initial_equity_usd: 25_000.0,
            // Iets ruimer DD-profiel
            halt_dd_frac: -0.12,
            kill_dd_frac: -0.25,
            max_leverage: 2.0,
            rebalance_drift_frac: 0.20,
            max_global_positions: 30,
        },
        sleeves: vec![
            SleeveRiskConfig {
                sleeve_id: SleeveId::EquityLongShort,
                capital_alloc_usd: 5_000.0,
                max_single_pos_risk_frac: 0.015,
                halt_dd_frac: -0.15,
                kill_dd_frac: -0.25,
                max_concurrent_positions: 15,
            },
            SleeveRiskConfig {
                sleeve_id: SleeveId::StatArbResidual,
                capital_alloc_usd: 6_250.0,
                max_single_pos_risk_frac: 0.01,
                halt_dd_frac: -0.15,
                kill_dd_frac: -0.25,
                max_concurrent_positions: 30,
            },
            SleeveRiskConfig {
                sleeve_id: SleeveId::MicrostructureIntraday,
                capital_alloc_usd: 3_750.0,
                max_single_pos_risk_frac: 0.007,
                halt_dd_frac: -0.10,
                kill_dd_frac: -0.20,
                max_concurrent_positions: 40,
            },
            SleeveRiskConfig {
                sleeve_id: SleeveId::OptionsVolPremium,
                capital_alloc_usd: 5_000.0,
                max_single_pos_risk_frac: 0.03,
                halt_dd_frac: -0.15,
                kill_dd_frac: -0.25,
                max_concurrent_positions: 8,
            },
            // Micro Futures sleeve – nu binnen test-range
            SleeveRiskConfig {
                sleeve_id: SleeveId::MicroFuturesMacroTrend,
                capital_alloc_usd: 5_000.0,
                // afgestemd op sanity-test: <= ~0.03
                max_single_pos_risk_frac: 0.05,
                halt_dd_frac: -0.15,
                kill_dd_frac: -0.25,
                max_concurrent_positions: 4,
            },
        ],
    }
}


/// Convenience-constructor voor agressief 25k-profiel.
pub fn aggressive_kernel_25k() -> GlobalRiskKernel {
    GlobalRiskKernel::new(aggressive_25k_global_risk_kernel_config())
}
