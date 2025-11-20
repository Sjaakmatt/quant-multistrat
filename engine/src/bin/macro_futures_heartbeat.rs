use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};

use engine::execution::{
    encode_heartbeat_log_event_json,
    encode_order_log_event_json,
    EngineHealth,
    InMemoryOrderSink,
    run_macro_futures_engine_heartbeat,
};
use engine::risk::{
    aggressive_kernel_25k,
    default_kernel_10k,
    GlobalRiskKernel,
    MarginState,
    PortfolioState,
    SleeveId,
    SleeveState,
    VolatilityRegime,
};
use engine::strategies::macro_futures_sleeve::{
    DailyFeatureBar,
    FutureInstrument,
    FxCarryFeatures,
    FuturesRiskBudget,
    InstrumentHistory,
    InstrumentRiskBudget,
    MacroFuturesSleeve,
    MacroFuturesSleeveConfig,
    MacroScalars,
};

fn main() {
    // ===== 1) Kies profiel op basis van RISK_PROFILE =====
    let profile = std::env::var("RISK_PROFILE").unwrap_or_else(|_| "starter_10k".to_string());

    let mut kernel: GlobalRiskKernel = match profile.as_str() {
        "aggressive_25k" => aggressive_kernel_25k(),
        _ => default_kernel_10k(), // fallback = starter_10k
    };

    // Lees config via de nieuwe accessor
    let cfg = kernel.config();

    // ===== 2) Portfolio- en sleeve-state uit profiel-config =====
    let initial_equity = cfg.portfolio.initial_equity_usd;

    let portfolio_state = PortfolioState {
        cash_usd: initial_equity,
        open_pnl_usd: 0.0,
        accrued_interest_usd: 0.0,
        peak_equity_usd: initial_equity,
        total_notional_exposure: 0.0,
        current_leverage: 0.0,
    };

    // Zoek MicroFuturesMacroTrend-sleeve in profiel
    let sleeve_cfg = cfg
        .sleeves
        .iter()
        .find(|s| s.sleeve_id == SleeveId::MicroFuturesMacroTrend)
        .expect("MicroFuturesMacroTrend sleeve must exist in profile");

    let mut sleeve_state = SleeveState {
        sleeve_id: SleeveId::MicroFuturesMacroTrend,
        equity_usd: sleeve_cfg.capital_alloc_usd,
        realized_pnl_usd: 0.0,
        unrealized_pnl_usd: 0.0,
        peak_equity_usd: sleeve_cfg.capital_alloc_usd,
        open_positions: 0,
    };

    let margin_state = MarginState {
        internal_margin_req_usd: 0.0,
        broker_margin_req_usd: 0.0,
        equity_usd: portfolio_state.cash_usd,
    };

    let vol_regime = VolatilityRegime {
        rv10_annualized: 12.0,
        vix_level: 18.0,
        vix_term_slope: 0.3,
        regime_scalar: 1.0,
    };

    // ===== 3) Macro futures sleeve + dummy histories (zoals in tests) =====
    let cfg_sleeve = MacroFuturesSleeveConfig::default();
    let sleeve = MacroFuturesSleeve::new(cfg_sleeve);

    let now = Utc::now();

    let mes_hist = make_history_for_demo(FutureInstrument::Mes, 100.0, now);
    let mnq_hist = make_history_for_demo(FutureInstrument::Mnq, 16_000.0, now);
    let sixe_hist = make_history_for_demo(FutureInstrument::SixE, 1.10, now);

    let mut histories: HashMap<FutureInstrument, InstrumentHistory> = HashMap::new();
    histories.insert(FutureInstrument::Mes, mes_hist);
    histories.insert(FutureInstrument::Mnq, mnq_hist);
    histories.insert(FutureInstrument::SixE, sixe_hist);

    let macro_scalars = MacroScalars {
        as_of: now,
        risk_on_scalar: 1.0,
        usd_scalar: 1.0,
    };

    let current_positions: HashMap<FutureInstrument, i32> = HashMap::new();

    let per_pos_cap_eur = 120.0;

    let risk_budget = FuturesRiskBudget {
        mes: InstrumentRiskBudget {
            max_risk_per_position_eur: per_pos_cap_eur,
            max_contracts: 5,
        },
        mnq: InstrumentRiskBudget {
            max_risk_per_position_eur: per_pos_cap_eur,
            max_contracts: 5,
        },
        sixe: InstrumentRiskBudget {
            max_risk_per_position_eur: 80.0,
            max_contracts: 3,
        },
        max_total_contracts: 4,
    };



    // Neem grootste per-positie-risk uit het budget
    let per_pos_cap_eur = risk_budget
        .mes
        .max_risk_per_position_eur
        .max(risk_budget.mnq.max_risk_per_position_eur)
        .max(risk_budget.sixe.max_risk_per_position_eur);

    // Sta bijv. max 3 posities toe aan full risk → v1 ≈ 270 EUR
    let max_sleeve_risk_eur = 5.0 * per_pos_cap_eur;


    let mut sink = InMemoryOrderSink::new();
    let ts_utc = now.timestamp();

    // ===== 4) Eén heartbeat draaien =====
    let result = run_macro_futures_engine_heartbeat(
        ts_utc,
        &mut kernel,
        &portfolio_state,
        &mut sleeve_state,
        &margin_state,
        &vol_regime,
        &sleeve,
        histories,
        macro_scalars,
        current_positions,
        1.0, // eur_per_usd (demo)
        &risk_budget,
        max_sleeve_risk_eur,
        &mut sink,
    );

    // ===== 5) Heartbeat + orders als JSON naar stdout =====
    let hb_json = encode_heartbeat_log_event_json(ts_utc, &result, EngineHealth::Healthy);
    println!("{}", hb_json);

    for order in &result.engine_orders {
        let line = encode_order_log_event_json(order, ts_utc);
        println!("{}", line);
    }
}

/// Dummy historiek zoals in de tests, voor demo/heartbeat.
fn make_history_for_demo(
    inst: FutureInstrument,
    base_price: f64,
    now: DateTime<Utc>,
) -> InstrumentHistory {
    let mut bars = Vec::new();

    // 130 dagen dummy data (genoeg voor MIN_BARS = 120)
    for i in 0..130 {
        let ts = now - Duration::days((129 - i) as i64);
        let price = base_price * (1.0 + 0.0005 * i as f64); // lichte uptrend

        let fx_carry = if let FutureInstrument::SixE = inst {
            Some(FxCarryFeatures {
                carry_rate_annualized: 0.02,
                carry_rate_vol_252d: 0.01,
            })
        } else {
            None
        };

        let bar = DailyFeatureBar {
            ts,
            open: price,
            high: price * 1.001,
            low: price * 0.999,
            close: price,
            volume: 1_000.0,

            atr_14: price * 0.005,
            ret_20d: 0.05,
            ret_60d: 0.10,
            ret_120d: 0.20,

            vol_20d: 0.01,
            vol_60d: 0.012,
            vol_120d: 0.015,

            highest_close_50d: price * 1.01,
            lowest_close_50d: price * 0.97,

            fx_carry,
        };

        bars.push(bar);
    }

    InstrumentHistory { instrument: inst, bars }
}
