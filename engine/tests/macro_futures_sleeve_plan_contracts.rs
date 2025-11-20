use std::collections::HashMap;
use std::env;
use std::fs;
use std::io::Cursor;
use std::rc::Rc;
use std::cell::RefCell;


use chrono::{Utc, Duration, DateTime, TimeZone};

use engine::strategies::macro_futures_sleeve::{
    MacroFuturesSleeve,
    MacroFuturesSleeveConfig,
    FuturesSleeveContext,
    FuturesRiskBudget,
    InstrumentRiskBudget,
    FutureInstrument,
    FxCarryFeatures,
    DailyFeatureBar,
    demo_macro_futures_sleeve,
    MacroScalars,
    SleeveRiskSanity,
    EngineOrderSide,
    EngineOrder,
};

use engine::execution::{
    OrderSink,
    InMemoryOrderSink,
    FileOrderSink,
    run_macro_futures_engine_heartbeat,
    encode_order_log_event_json,
    encode_heartbeat_log_event_json,
    HeartbeatLogSink,
    StdoutHeartbeatLogger,
    run_macro_futures_engine_heartbeat_with_logging,
    BatchingHeartbeatLogger,
    FileHeartbeatLogger,
    HeartbeatSupervisor,
    EngineHealth,
    HeartbeatSupervisorEvent,
    encode_supervisor_event_json,
};

use engine::risk::{
    SleeveId,
    SleeveRiskEnvelope,
    HaltState,
    PortfolioRiskState,
    GlobalRiskKernel,
    GlobalRiskKernelConfig,
    SleeveRiskConfig,
    PortfolioRiskConfig,
    SleeveState,
    PortfolioState,
    MarginState,
    VolatilityRegime,
};

fn fixed_as_of() -> DateTime<Utc> {
    // Vast timestamp zodat tests deterministisch zijn
    Utc
        .with_ymd_and_hms(2024, 1, 2, 10, 0, 0)
        .single()
        .expect("valid test datetime")
}

fn base_risk_envelope() -> SleeveRiskEnvelope {
    SleeveRiskEnvelope {
        sleeve_id: SleeveId::MicroFuturesMacroTrend,
        sleeve_halt: HaltState::None,
        portfolio_halt: HaltState::None,

        max_position_size_usd: 2_000.0,
        max_concurrent_positions: 3,

        exposure_remaining_usd: 100_000.0,
        margin_remaining_usd: 100_000.0,

        volatility_regime_scalar: 1.0,
        leverage_scalar: 1.0,

        portfolio_risk_state: PortfolioRiskState::Normal,
    }
}

/// Super-minimal context helper die overal bruikbaar is in tests
fn make_minimal_ctx() -> FuturesSleeveContext {
    let as_of = fixed_as_of();

    let macro_scalars = MacroScalars {
        as_of,
        risk_on_scalar: 1.0,
        usd_scalar: 1.0,
    };

    let risk_envelope = base_risk_envelope();

    FuturesSleeveContext {
        as_of,
        histories: HashMap::new(),
        macro_scalars,
        risk_envelope,
        current_positions: HashMap::new(),
        eur_per_usd: 1.0,
        engine_health: EngineHealth::Healthy,
    }
}

fn minimal_risk_budget() -> FuturesRiskBudget {
    FuturesRiskBudget {
        mes: InstrumentRiskBudget {
            max_risk_per_position_eur: 1_000_000.0,
            max_contracts: 100,
        },
        mnq: InstrumentRiskBudget {
            max_risk_per_position_eur: 1_000_000.0,
            max_contracts: 100,
        },
        sixe: InstrumentRiskBudget {
            max_risk_per_position_eur: 1_000_000.0,
            max_contracts: 100,
        },
        max_total_contracts: 300,
    }
}


fn make_history_for_test(
    inst: FutureInstrument,
    base_price: f64,
    now: DateTime<Utc>,
) -> engine::strategies::macro_futures_sleeve::InstrumentHistory {
    let mut bars = Vec::new();

    // 130 dagen dummy data (genoeg voor MIN_BARS = 120)
    for i in 0..130 {
        let ts = now - Duration::days((129 - i) as i64);

        // Simpele lichte uptrend
        let price = base_price * (1.0 + 0.0005 * i as f64);

        let fx_carry = if let FutureInstrument::SixE = inst {
            Some(FxCarryFeatures {
                carry_rate_annualized: 0.02,  // 2% carry
                carry_rate_vol_252d: 0.01,    // 1% vol
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

    engine::strategies::macro_futures_sleeve::InstrumentHistory {
        instrument: inst,
        bars,
    }
}

#[test]
fn smoke_macro_futures_sleeve_demo() {
    // Oude smoke-test uit de module zelf
    demo_macro_futures_sleeve();
}

#[test]
fn risk_budget_blocks_position_when_one_contract_exceeds_risk() {
    let cfg = MacroFuturesSleeveConfig::default();
    let sleeve = MacroFuturesSleeve::new(cfg);
    let now = Utc::now();

    // MES met prijs ~100 → contract_notional ≈ 100 * 5 = 500 USD/EUR
    let mes_hist = make_history_for_test(FutureInstrument::Mes, 100.0, now);

    let mut histories = HashMap::new();
    histories.insert(FutureInstrument::Mes, mes_hist);

    let macro_scalars = engine::strategies::macro_futures_sleeve::MacroScalars {
        as_of: now,
        risk_on_scalar: 1.0,
        usd_scalar: 1.0,
    };

    let risk_envelope = SleeveRiskEnvelope {
        sleeve_id: SleeveId::MicroFuturesMacroTrend,
        sleeve_halt: HaltState::None,
        portfolio_halt: HaltState::None,

        max_position_size_usd: 10_000.0,
        max_concurrent_positions: 1,

        exposure_remaining_usd: 10_000.0,
        margin_remaining_usd: 10_000.0,

        volatility_regime_scalar: 1.0,
        leverage_scalar: 1.0,

        portfolio_risk_state: PortfolioRiskState::Normal,
    };

    let current_positions: HashMap<FutureInstrument, i32> = HashMap::new();

    let ctx = FuturesSleeveContext {
        as_of: now,
        histories,
        macro_scalars,
        risk_envelope,
        current_positions,
        eur_per_usd: 0.92,
        engine_health: EngineHealth::Healthy, // default
    };



    // Eén contract ≈ 500 EUR risk; budget = 50 → max_by_risk = floor(50/500) = 0 → geen positie
    let risk_budget = FuturesRiskBudget {
        mes: InstrumentRiskBudget {
            max_risk_per_position_eur: 50.0, // lager dan risk van 1 contract
            max_contracts: 10,
        },
        mnq: InstrumentRiskBudget {
            max_risk_per_position_eur: 1_000_000.0,
            max_contracts: 10,
        },
        sixe: InstrumentRiskBudget {
            max_risk_per_position_eur: 1_000_000.0,
            max_contracts: 10,
        },
        max_total_contracts: 10,
    };

    let planned = sleeve.plan_contracts(&ctx, &risk_budget);

    assert!(
        planned.is_empty(),
        "Expected no contracts when one contract already exceeds risk budget, got: {:?}",
        planned
    );
}

#[test]
fn risk_budget_trims_contracts_to_risk_cap() {
    let cfg = MacroFuturesSleeveConfig::default();
    let sleeve = MacroFuturesSleeve::new(cfg);
    let as_of = fixed_as_of();

    let mes_hist = make_history_for_test(FutureInstrument::Mes, 100.0, as_of);

    let mut histories = HashMap::new();
    histories.insert(FutureInstrument::Mes, mes_hist);

    let macro_scalars = MacroScalars {
        as_of,
        risk_on_scalar: 1.0,
        usd_scalar: 1.0,
    };

    let risk_envelope = SleeveRiskEnvelope {
        sleeve_id: SleeveId::MicroFuturesMacroTrend,
        sleeve_halt: HaltState::None,
        portfolio_halt: HaltState::None,

        max_position_size_usd: 10_000.0,
        max_concurrent_positions: 1,

        exposure_remaining_usd: 10_000.0,
        margin_remaining_usd: 10_000.0,

        volatility_regime_scalar: 1.0,
        leverage_scalar: 1.0,

        portfolio_risk_state: PortfolioRiskState::Normal,
    };

    let current_positions: HashMap<FutureInstrument, i32> = HashMap::new();

    let ctx = FuturesSleeveContext {
        as_of,
        histories,
        macro_scalars,
        risk_envelope,
        current_positions,
        eur_per_usd: 0.92,
        engine_health: EngineHealth::Healthy,
    };

    // MES:
    // last_price = 100
    // contract_notional_usd = 100 * 5 = 500
    // risk_per_contract_eur = 500 * 0.92 = 460
    //
    // max_risk_per_position_eur = 5_000
    // → max_by_risk = floor(5_000 / 460) = 10
    //
    // max_contracts = 100, max_total_contracts = 100
    // → finale cap = 10 contracts.
    let risk_budget = FuturesRiskBudget {
        mes: InstrumentRiskBudget {
            max_risk_per_position_eur: 5_000.0,
            max_contracts: 100,
        },
        mnq: InstrumentRiskBudget {
            max_risk_per_position_eur: 1_000_000.0,
            max_contracts: 100,
        },
        sixe: InstrumentRiskBudget {
            max_risk_per_position_eur: 1_000_000.0,
            max_contracts: 100,
        },
        max_total_contracts: 100,
    };

    let planned = sleeve.plan_contracts(&ctx, &risk_budget);

    let mes_plan = planned
        .iter()
        .find(|p| p.instrument == FutureInstrument::Mes)
        .expect("Expected a MES planned position");

    assert_eq!(
        mes_plan.target_contracts.abs(),
        10,
        "Expected MES contracts to be trimmed to 10 by risk-cap (given last price and risk budget), got: {}",
        mes_plan.target_contracts
    );
}


#[test]
fn fx_factor_changes_allowed_contracts_in_eur_terms() {
    let cfg = MacroFuturesSleeveConfig::default();
    let sleeve = MacroFuturesSleeve::new(cfg);

    // Gebruik een vaste timestamp voor determinisme
    let as_of = fixed_as_of();

    // MES historie zoals in de andere tests
    let mes_hist = make_history_for_test(FutureInstrument::Mes, 100.0, as_of);

    let mut histories = HashMap::new();
    histories.insert(FutureInstrument::Mes, mes_hist);

    let macro_scalars = MacroScalars {
        as_of,
        risk_on_scalar: 1.0,
        usd_scalar: 1.0,
    };

    // Groot genoeg base-position zodat risk-cap (niet max_contracts) bindt
    let risk_envelope = SleeveRiskEnvelope {
        sleeve_id: SleeveId::MicroFuturesMacroTrend,
        sleeve_halt: HaltState::None,
        portfolio_halt: HaltState::None,

        max_position_size_usd: 20_000.0, // groot genoeg zodat risk-cap echt bindt
        max_concurrent_positions: 1,

        exposure_remaining_usd: 20_000.0,
        margin_remaining_usd: 20_000.0,

        volatility_regime_scalar: 1.0,
        leverage_scalar: 1.0,

        portfolio_risk_state: PortfolioRiskState::Normal,
    };

    let current_positions: HashMap<FutureInstrument, i32> = HashMap::new();

    // Basiscontext met MES history + envelope
    let ctx_base = FuturesSleeveContext {
        as_of,
        histories,
        macro_scalars,
        risk_envelope,
        current_positions,
        eur_per_usd: 1.0, // default, we variëren dit zo
        engine_health: EngineHealth::Healthy,
    };

    // Case A: eur_per_usd = 1.0  → hogere EUR-risk per contract
    let mut ctx_eur_1 = ctx_base.clone();
    ctx_eur_1.eur_per_usd = 1.0;

    // Case B: eur_per_usd = 0.5 → lagere EUR-risk per contract
    let mut ctx_eur_0_5 = ctx_base.clone();
    ctx_eur_0_5.eur_per_usd = 0.5;

    // Risk-budget: zo gekozen dat risk-cap de beperkende factor is
    let risk_budget = FuturesRiskBudget {
        mes: InstrumentRiskBudget {
            max_risk_per_position_eur: 5_000.0,
            max_contracts: 100,
        },
        mnq: InstrumentRiskBudget {
            max_risk_per_position_eur: 1_000_000.0,
            max_contracts: 100,
        },
        sixe: InstrumentRiskBudget {
            max_risk_per_position_eur: 1_000_000.0,
            max_contracts: 100,
        },
        max_total_contracts: 100,
    };

    let planned_eur_1 = sleeve.plan_contracts(&ctx_eur_1, &risk_budget);
    let planned_eur_0_5 = sleeve.plan_contracts(&ctx_eur_0_5, &risk_budget);

    let mes_eur_1 = planned_eur_1
        .iter()
        .find(|p| p.instrument == FutureInstrument::Mes)
        .expect("Expected a MES planned position for eur_per_usd=1.0");

    let mes_eur_0_5 = planned_eur_0_5
        .iter()
        .find(|p| p.instrument == FutureInstrument::Mes)
        .expect("Expected a MES planned position for eur_per_usd=0.5");

    let n_1 = mes_eur_1.target_contracts.abs();
    let n_0_5 = mes_eur_0_5.target_contracts.abs();

    assert!(
        n_0_5 > n_1,
        "Expected more MES contracts when EUR-per-USD is lower (0.5) because EUR risk per contract is smaller; got eur_per_usd=1.0 -> {}, eur_per_usd=0.5 -> {}",
        n_1,
        n_0_5
    );
}


#[test]
fn risk_report_matches_contracts_and_notional() {
    let cfg = MacroFuturesSleeveConfig::default();
    let sleeve = MacroFuturesSleeve::new(cfg);
    let now = Utc::now();

    // MES met base_price ~100 → laatste prijs iets >100
    let mes_hist = make_history_for_test(FutureInstrument::Mes, 100.0, now);

    let mut histories = HashMap::new();
    histories.insert(FutureInstrument::Mes, mes_hist);

    let macro_scalars = MacroScalars {
        as_of: now,
        risk_on_scalar: 1.0,
        usd_scalar: 1.0,
    };

    let risk_envelope = SleeveRiskEnvelope {
        sleeve_id: SleeveId::MicroFuturesMacroTrend,
        sleeve_halt: HaltState::None,
        portfolio_halt: HaltState::None,

        // Genoeg ruimte zodat sizing niet door env wordt begrensd
        max_position_size_usd: 5_000.0,
        max_concurrent_positions: 1,

        exposure_remaining_usd: 5_000.0,
        margin_remaining_usd: 5_000.0,

        volatility_regime_scalar: 1.0,
        leverage_scalar: 1.0,

        portfolio_risk_state: PortfolioRiskState::Normal,
    };

    let current_positions: HashMap<FutureInstrument, i32> = HashMap::new();

    let as_of = fixed_as_of();

    let ctx = FuturesSleeveContext {
        as_of,
        histories,
        macro_scalars,
        risk_envelope,
        current_positions,
        eur_per_usd: 0.92,
        engine_health: EngineHealth::Healthy, // default
    };


    // Risk-budget: zorg dat er meerdere contracts kunnen komen,
    // maar dat risk-cap wel bindt.
    let risk_budget = FuturesRiskBudget {
        mes: InstrumentRiskBudget {
            max_risk_per_position_eur: 2_000.0,
            max_contracts: 10,
        },
        mnq: InstrumentRiskBudget {
            max_risk_per_position_eur: 1_000_000.0,
            max_contracts: 100,
        },
        sixe: InstrumentRiskBudget {
            max_risk_per_position_eur: 1_000_000.0,
            max_contracts: 100,
        },
        max_total_contracts: 100,
    };

    // Haal contracts én risk-report op
    let planned_contracts = sleeve.plan_contracts(&ctx, &risk_budget);
    let risk_report = sleeve.plan_risk_report(&ctx, &risk_budget);

    let mes_contracts = planned_contracts
        .iter()
        .find(|p| p.instrument == FutureInstrument::Mes)
        .expect("Expected MES in planned_contracts");

    let mes_risk = risk_report
        .iter()
        .find(|r| r.instrument == FutureInstrument::Mes)
        .expect("Expected MES in risk_report");

    // 1) target_contracts moet overeenkomen
    assert_eq!(
        mes_contracts.target_contracts,
        mes_risk.target_contracts,
        "target_contracts in risk_report moet gelijk zijn aan plan_contracts"
    );

    // 2) total_risk_eur moet risk_per_contract_eur * |contracts| zijn
    let expected_total = mes_risk.risk_per_contract_eur * (mes_risk.target_contracts.abs() as f64);

    let diff = (mes_risk.total_risk_eur - expected_total).abs();
    assert!(
        diff < 1e-6,
        "total_risk_eur moet gelijk zijn aan risk_per_contract_eur * |contracts|; expected {}, got {} (diff={})",
        expected_total,
        mes_risk.total_risk_eur,
        diff
    );
}

#[test]
fn sleeve_exposure_and_margin_headroom_cap_notional_in_usd() {
    let cfg = MacroFuturesSleeveConfig::default();
    let sleeve = MacroFuturesSleeve::new(cfg);
    let now = Utc::now();

    // MES historie zoals in de andere tests
    let mes_hist = make_history_for_test(FutureInstrument::Mes, 100.0, now);

    let mut histories = HashMap::new();
    histories.insert(FutureInstrument::Mes, mes_hist);

    let macro_scalars = MacroScalars {
        as_of: now,
        risk_on_scalar: 1.0,
        usd_scalar: 1.0,
    };

    // Hier forceren we dat env-headroom (exposure/margin) de beperkende factor is.
    // max_position_size_usd is groot, maar exposure_remaining_usd en margin_remaining_usd zijn klein.
    let exposure_cap: f64 = 1_000.0;
    let margin_cap: f64 = 600.0;
    let allowed_notional = exposure_cap.min(margin_cap);

    let risk_envelope = SleeveRiskEnvelope {
        sleeve_id: SleeveId::MicroFuturesMacroTrend,
        sleeve_halt: HaltState::None,
        portfolio_halt: HaltState::None,

        max_position_size_usd: 10_000.0,
        max_concurrent_positions: 1,

        exposure_remaining_usd: exposure_cap,
        margin_remaining_usd: margin_cap,

        volatility_regime_scalar: 1.0,
        leverage_scalar: 1.0,

        portfolio_risk_state: PortfolioRiskState::Normal,
    };

    let current_positions: HashMap<FutureInstrument, i32> = HashMap::new();

    let as_of = fixed_as_of();

    let ctx = FuturesSleeveContext {
        as_of,
        histories,
        macro_scalars,
        risk_envelope,
        current_positions,
        eur_per_usd: 0.92,
        engine_health: EngineHealth::Healthy, // default
    };


    // Risk-budget zo zetten dat risico NIET de beperkende factor is (alleen env-headroom).
    let risk_budget = FuturesRiskBudget {
        mes: InstrumentRiskBudget {
            max_risk_per_position_eur: 1_000_000.0,
            max_contracts: 10_000,
        },
        mnq: InstrumentRiskBudget {
            max_risk_per_position_eur: 1_000_000.0,
            max_contracts: 10_000,
        },
        sixe: InstrumentRiskBudget {
            max_risk_per_position_eur: 1_000_000.0,
            max_contracts: 10_000,
        },
        max_total_contracts: 10_000,
    };

    // Gebruik de risk-report om de feitelijke USD-notional te reconstrueren:
    // total_risk_eur = |contracts| * contract_notional_usd * eur_per_usd
    // → |contracts| * contract_notional_usd = total_risk_eur / eur_per_usd = absolute USD-exposure
    let risk_report = sleeve.plan_risk_report(&ctx, &risk_budget);

    let mes_risk = risk_report
        .iter()
        .find(|r| r.instrument == FutureInstrument::Mes)
        .expect("Expected MES in risk_report");

    let abs_notional_usd = mes_risk.total_risk_eur / ctx.eur_per_usd;

    // We verwachten:
    // 1) Dat er überhaupt exposure is (dus > 1 USD, door onze filter in plan_positions)
    assert!(
        abs_notional_usd > 1.0,
        "Expected some USD notional exposure, got {}",
        abs_notional_usd
    );

    // 2) Dat exposure/margin-headroom een harde cap is:
    assert!(
        abs_notional_usd <= allowed_notional + 1e-6,
        "Expected USD notional to be capped by min(exposure_remaining, margin_remaining) = {}, got {}",
        allowed_notional,
        abs_notional_usd
    );
}

#[test]
fn concurrency_limit_blocks_opening_new_instrument() {
    let cfg = MacroFuturesSleeveConfig::default();
    let sleeve = MacroFuturesSleeve::new(cfg);
    let now = Utc::now();

    // Historie voor MES en MNQ (beiden geven normaal een long-signaal)
    let mes_hist = make_history_for_test(FutureInstrument::Mes, 100.0, now);
    let mnq_hist = make_history_for_test(FutureInstrument::Mnq, 16000.0, now);

    let mut histories = HashMap::new();
    histories.insert(FutureInstrument::Mes, mes_hist);
    histories.insert(FutureInstrument::Mnq, mnq_hist);

    let macro_scalars = MacroScalars {
        as_of: now,
        risk_on_scalar: 1.0,
        usd_scalar: 1.0,
    };

    // Concurrency-cap = 1 slot.
    // We doen alsof MES al een open positie heeft, MNQ nog niet.
    let risk_envelope = SleeveRiskEnvelope {
        sleeve_id: SleeveId::MicroFuturesMacroTrend,
        sleeve_halt: HaltState::None,
        portfolio_halt: HaltState::None,

        max_position_size_usd: 10_000.0,
        max_concurrent_positions: 1,

        exposure_remaining_usd: 10_000.0,
        margin_remaining_usd: 10_000.0,

        volatility_regime_scalar: 1.0,
        leverage_scalar: 1.0,

        portfolio_risk_state: PortfolioRiskState::Normal,
    };

    let mut current_positions: HashMap<FutureInstrument, i32> = HashMap::new();
    current_positions.insert(FutureInstrument::Mes, 1); // MES al open

    let as_of = fixed_as_of();
    // MNQ blijft impliciet flat (0)

    let ctx = FuturesSleeveContext {
        as_of,
        histories,
        macro_scalars,
        risk_envelope,
        current_positions,
        eur_per_usd: 0.92,
        engine_health: EngineHealth::Healthy, // default
    };


    // Risk-budget ruim zetten zodat alleen concurrency/headroom bindt
    let risk_budget = FuturesRiskBudget {
        mes: InstrumentRiskBudget {
            max_risk_per_position_eur: 1_000_000.0,
            max_contracts: 10_000,
        },
        mnq: InstrumentRiskBudget {
            max_risk_per_position_eur: 1_000_000.0,
            max_contracts: 10_000,
        },
        sixe: InstrumentRiskBudget {
            max_risk_per_position_eur: 1_000_000.0,
            max_contracts: 10_000,
        },
        max_total_contracts: 10_000,
    };

    let planned = sleeve.plan_contracts(&ctx, &risk_budget);

    let has_mes = planned
        .iter()
        .any(|p| p.instrument == FutureInstrument::Mes);
    let has_mnq = planned
        .iter()
        .any(|p| p.instrument == FutureInstrument::Mnq);

    assert!(
        has_mes,
        "Expected existing MES position to still get a target under concurrency cap"
    );

    assert!(
        !has_mnq,
        "Expected concurrency cap to block opening a new MNQ position when max_concurrent_positions is already filled by MES"
    );
}

#[test]
fn halt_or_kill_flattens_existing_positions_and_opens_nothing_new() {
    let cfg = MacroFuturesSleeveConfig::default();
    let sleeve = MacroFuturesSleeve::new(cfg);
    let now = Utc::now();

    // Historie voor MES en MNQ zodat beide normaal een signaal zouden genereren.
    let mes_hist = make_history_for_test(FutureInstrument::Mes, 100.0, now);
    let mnq_hist = make_history_for_test(FutureInstrument::Mnq, 16_000.0, now);

    let mut histories = HashMap::new();
    histories.insert(FutureInstrument::Mes, mes_hist);
    histories.insert(FutureInstrument::Mnq, mnq_hist);

    let macro_scalars = MacroScalars {
        as_of: now,
        risk_on_scalar: 1.0,
        usd_scalar: 1.0,
    };

    // Sleeve staat in HALT/KILL → we verwachten:
    // - geen nieuwe posities
    // - bestaande posities moeten naar 0 (flatten)
    let risk_envelope = SleeveRiskEnvelope {
        sleeve_id: SleeveId::MicroFuturesMacroTrend,
        sleeve_halt: HaltState::Kill,          // of HaltState::Halt, beide moeten flatten
        portfolio_halt: HaltState::None,

        max_position_size_usd: 10_000.0,       // wordt in plan_positions genegeerd door halt-check
        max_concurrent_positions: 3,

        exposure_remaining_usd: 10_000.0,
        margin_remaining_usd: 10_000.0,

        volatility_regime_scalar: 1.0,
        leverage_scalar: 1.0,

        portfolio_risk_state: PortfolioRiskState::Normal,
    };

    // MES heeft al een positie, MNQ niet.
    let mut current_positions: HashMap<FutureInstrument, i32> = HashMap::new();
    current_positions.insert(FutureInstrument::Mes, 3);
    current_positions.insert(FutureInstrument::Mnq, 0);

    let as_of = fixed_as_of();

    let ctx = FuturesSleeveContext {
        as_of,
        histories,
        macro_scalars,
        risk_envelope,
        current_positions,
        eur_per_usd: 0.92,
        engine_health: EngineHealth::Healthy, // default
    };


    // Risk-budget ruim → halt moet bepalend zijn, niet risk-budget.
    let risk_budget = FuturesRiskBudget {
        mes: InstrumentRiskBudget {
            max_risk_per_position_eur: 1_000_000.0,
            max_contracts: 10_000,
        },
        mnq: InstrumentRiskBudget {
            max_risk_per_position_eur: 1_000_000.0,
            max_contracts: 10_000,
        },
        sixe: InstrumentRiskBudget {
            max_risk_per_position_eur: 1_000_000.0,
            max_contracts: 10_000,
        },
        max_total_contracts: 10_000,
    };

    let order_intents = sleeve.plan_order_intents(&ctx, &risk_budget);

    // 1) Er mag geen nieuwe MNQ-positie geopend worden
    let has_mnq_open = order_intents
        .iter()
        .any(|oi| oi.instrument == FutureInstrument::Mnq && oi.delta_contracts != 0);
    assert!(
        !has_mnq_open,
        "Expected no new MNQ orders when sleeve is in HALT/KILL"
    );

    // 2) MES moet volledig gesloten worden (delta = -current)
    let mes_flat_order = order_intents
        .iter()
        .find(|oi| oi.instrument == FutureInstrument::Mes);

    assert!(
        mes_flat_order.is_some(),
        "Expected an order intent to flatten existing MES position under HALT/KILL"
    );

    let mes_order = mes_flat_order.unwrap();
    assert_eq!(
        mes_order.delta_contracts,
        -3,
        "Expected MES flatten order of -3 contracts, got {}",
        mes_order.delta_contracts
    );
}

#[test]
fn aggregate_sleeve_risk_computes_correct_totals() {
    let cfg = MacroFuturesSleeveConfig::default();
    let sleeve = MacroFuturesSleeve::new(cfg);
    let now = Utc::now();

    // MES + MNQ geven beide duidelijk signaal
    let mes_hist = make_history_for_test(FutureInstrument::Mes, 100.0, now);
    let mnq_hist = make_history_for_test(FutureInstrument::Mnq, 16000.0, now);

    let mut histories = HashMap::new();
    histories.insert(FutureInstrument::Mes, mes_hist);
    histories.insert(FutureInstrument::Mnq, mnq_hist);

    let macro_scalars = MacroScalars {
        as_of: now,
        risk_on_scalar: 1.0,
        usd_scalar: 1.0,
    };

    let risk_envelope = SleeveRiskEnvelope {
        sleeve_id: SleeveId::MicroFuturesMacroTrend,
        sleeve_halt: HaltState::None,
        portfolio_halt: HaltState::None,

        max_position_size_usd: 10_000.0,
        max_concurrent_positions: 3,

        exposure_remaining_usd: 10_000.0,
        margin_remaining_usd: 10_000.0,

        volatility_regime_scalar: 1.0,
        leverage_scalar: 1.0,

        portfolio_risk_state: PortfolioRiskState::Normal,
    };

    let current_positions: HashMap<FutureInstrument, i32> = HashMap::new();

    let as_of = fixed_as_of();

    let ctx = FuturesSleeveContext {
        as_of,
        histories,
        macro_scalars,
        risk_envelope,
        current_positions,
        eur_per_usd: 0.92,
        engine_health: EngineHealth::Healthy, // default
    };


    // Risk-budget ruim (risk-cap mag niet binden)
    let risk_budget = FuturesRiskBudget {
        mes: InstrumentRiskBudget { max_risk_per_position_eur: 1_000_000.0, max_contracts: 100 },
        mnq: InstrumentRiskBudget { max_risk_per_position_eur: 1_000_000.0, max_contracts: 100 },
        sixe: InstrumentRiskBudget { max_risk_per_position_eur: 1_000_000.0, max_contracts: 100 },
        max_total_contracts: 100,
    };

    let agg = sleeve.aggregate_sleeve_risk(&ctx, &risk_budget);

    assert!(
        agg.instrument_count >= 1,
        "Expected at least one instrument in sleeve aggregate"
    );

    assert!(
        agg.total_risk_eur > 0.0,
        "Expected positive total EUR risk, got {}",
        agg.total_risk_eur
    );

    assert!(
        agg.total_notional_usd > 0.0,
        "Expected positive total USD notional, got {}",
        agg.total_notional_usd
    );

    assert!(
        agg.total_contracts_abs > 0,
        "Expected at least one contract in aggregate"
    );
}

#[test]
fn check_sleeve_risk_sanity_flags_when_above_cap() {
    let cfg = MacroFuturesSleeveConfig::default();
    let sleeve = MacroFuturesSleeve::new(cfg);
    let now = Utc::now();

    // MES + MNQ met normale signaallogica
    let mes_hist = make_history_for_test(FutureInstrument::Mes, 100.0, now);
    let mnq_hist = make_history_for_test(FutureInstrument::Mnq, 16_000.0, now);

    let mut histories = HashMap::new();
    histories.insert(FutureInstrument::Mes, mes_hist);
    histories.insert(FutureInstrument::Mnq, mnq_hist);

    let macro_scalars = MacroScalars {
        as_of: now,
        risk_on_scalar: 1.0,
        usd_scalar: 1.0,
    };

    let risk_envelope = SleeveRiskEnvelope {
        sleeve_id: SleeveId::MicroFuturesMacroTrend,
        sleeve_halt: HaltState::None,
        portfolio_halt: HaltState::None,

        max_position_size_usd: 10_000.0,
        max_concurrent_positions: 3,

        exposure_remaining_usd: 10_000.0,
        margin_remaining_usd: 10_000.0,

        volatility_regime_scalar: 1.0,
        leverage_scalar: 1.0,

        portfolio_risk_state: PortfolioRiskState::Normal,
    };

    let current_positions: HashMap<FutureInstrument, i32> = HashMap::new();

    let as_of = fixed_as_of();

    let ctx = FuturesSleeveContext {
        as_of,
        histories,
        macro_scalars,
        risk_envelope,
        current_positions,
        eur_per_usd: 0.92,
        engine_health: EngineHealth::Healthy, // default
    };


    // Ruime risk-budget → aggregate risk > 0
    let risk_budget = FuturesRiskBudget {
        mes: InstrumentRiskBudget { max_risk_per_position_eur: 1_000_000.0, max_contracts: 100 },
        mnq: InstrumentRiskBudget { max_risk_per_position_eur: 1_000_000.0, max_contracts: 100 },
        sixe: InstrumentRiskBudget { max_risk_per_position_eur: 1_000_000.0, max_contracts: 100 },
        max_total_contracts: 100,
    };

    let agg = sleeve.aggregate_sleeve_risk(&ctx, &risk_budget);
    assert!(
        agg.total_risk_eur > 0.0,
        "Precondition: expected positive total risk"
    );

    // 1) Hoge cap → Ok
    let status_ok = sleeve.check_sleeve_risk_sanity(
        &ctx,
        &risk_budget,
        agg.total_risk_eur * 2.0,
    );
    assert_eq!(
        status_ok,
        SleeveRiskSanity::Ok,
        "Expected Ok when max_sleeve_risk_eur is above current total risk"
    );

    // 2) Lage cap → ExceedsCap
    let status_exceeds = sleeve.check_sleeve_risk_sanity(
        &ctx,
        &risk_budget,
        agg.total_risk_eur * 0.5,
    );
    assert_eq!(
        status_exceeds,
        SleeveRiskSanity::ExceedsCap,
        "Expected ExceedsCap when max_sleeve_risk_eur is below current total risk"
    );
}

#[test]
fn plan_sleeve_consistent_with_existing_apis_and_flags_sanity() {
    let cfg = MacroFuturesSleeveConfig::default();
    let sleeve = MacroFuturesSleeve::new(cfg);
    let now = Utc::now();

    // MES en MNQ met normale signaallogica
    let mes_hist = make_history_for_test(FutureInstrument::Mes, 100.0, now);
    let mnq_hist = make_history_for_test(FutureInstrument::Mnq, 16_000.0, now);

    let mut histories = HashMap::new();
    histories.insert(FutureInstrument::Mes, mes_hist);
    histories.insert(FutureInstrument::Mnq, mnq_hist);

    let macro_scalars = MacroScalars {
        as_of: now,
        risk_on_scalar: 1.0,
        usd_scalar: 1.0,
    };

    let risk_envelope = SleeveRiskEnvelope {
        sleeve_id: SleeveId::MicroFuturesMacroTrend,
        sleeve_halt: HaltState::None,
        portfolio_halt: HaltState::None,

        max_position_size_usd: 10_000.0,
        max_concurrent_positions: 3,

        exposure_remaining_usd: 10_000.0,
        margin_remaining_usd: 10_000.0,

        volatility_regime_scalar: 1.0,
        leverage_scalar: 1.0,

        portfolio_risk_state: PortfolioRiskState::Normal,
    };

    let current_positions: HashMap<FutureInstrument, i32> = HashMap::new();

    let as_of = fixed_as_of();

    let ctx = FuturesSleeveContext {
    as_of,
    histories,
    macro_scalars,
    risk_envelope,
    current_positions,
    eur_per_usd: 0.92,
    engine_health: EngineHealth::Healthy, // default
};


    // Ruim risk-budget zodat er posities worden gepland
    let risk_budget = FuturesRiskBudget {
        mes: InstrumentRiskBudget { max_risk_per_position_eur: 1_000_000.0, max_contracts: 100 },
        mnq: InstrumentRiskBudget { max_risk_per_position_eur: 1_000_000.0, max_contracts: 100 },
        sixe: InstrumentRiskBudget { max_risk_per_position_eur: 1_000_000.0, max_contracts: 100 },
        max_total_contracts: 100,
    };

    // Referentie: losse API-calls
    let ref_contracts = sleeve.plan_contracts(&ctx, &risk_budget);
    let ref_report = sleeve.plan_risk_report(&ctx, &risk_budget);
    let ref_agg = sleeve.aggregate_sleeve_risk(&ctx, &risk_budget);

    // Gebruik plan_sleeve met een cap die duidelijk boven de huidige risk ligt
    let plan = sleeve.plan_sleeve(&ctx, &risk_budget, ref_agg.total_risk_eur * 2.0);

    // 1) planned_contracts moet overeenkomen
    assert_eq!(
        ref_contracts.len(),
        plan.planned_contracts.len(),
        "planned_contracts length from plan_sleeve must match direct plan_contracts"
    );

    // 2) risk_report moet overeenkomen (zelfde instrumenten)
    assert_eq!(
        ref_report.len(),
        plan.risk_report.len(),
        "risk_report length from plan_sleeve must match direct plan_risk_report"
    );

    // 3) aggregate moet identiek zijn
    assert_eq!(
        ref_agg.total_contracts_abs,
        plan.aggregate.total_contracts_abs,
        "aggregate.total_contracts_abs mismatch between direct call and plan_sleeve"
    );
    assert!(
        (ref_agg.total_risk_eur - plan.aggregate.total_risk_eur).abs() < 1e-6,
        "aggregate.total_risk_eur mismatch: direct={} via_plan={}",
        ref_agg.total_risk_eur,
        plan.aggregate.total_risk_eur
    );

    // 4) sanity moet Ok zijn als cap ruim boven de huidige risk ligt
    assert_eq!(
        plan.sanity,
        SleeveRiskSanity::Ok,
        "Expected SleeveRiskSanity::Ok when cap is above total risk"
    );
}

#[test]
fn global_risk_kernel_and_macro_futures_sleeve_integrate_consistently() {
    let now = Utc::now();

    // === 1) Global risk kernel config & state ===

    // Portfolio-config: 10k USD, max leverage 1.5, 1 sleeve.
    let portfolio_cfg = PortfolioRiskConfig {
        initial_equity_usd: 10_000.0,
        halt_dd_frac: -0.08,
        kill_dd_frac: -0.12,
        max_leverage: 1.5,
        rebalance_drift_frac: 0.15,
        max_global_positions: 10,
    };

    // Eén sleeve-config voor MicroFuturesMacroTrend
    let sleeve_cfg = SleeveRiskConfig {
        sleeve_id: SleeveId::MicroFuturesMacroTrend,
        capital_alloc_usd: 2_000.0,
        max_single_pos_risk_frac: 0.01, // 1% van 2k = 20 USD base size
        halt_dd_frac: -0.10,
        kill_dd_frac: -0.15,
        max_concurrent_positions: 3,
    };

    let gcfg = GlobalRiskKernelConfig {
        portfolio: portfolio_cfg,
        sleeves: vec![sleeve_cfg],
    };

    let mut kernel = GlobalRiskKernel::new(gcfg);

    // Portfolio-state: volledig in cash, geen exposure, geen PnL.
    let portfolio_state = PortfolioState {
        cash_usd: 10_000.0,
        open_pnl_usd: 0.0,
        accrued_interest_usd: 0.0,
        peak_equity_usd: 10_000.0,
        total_notional_exposure: 0.0,
        current_leverage: 0.0,
    };

    // Sleeve-state: equity gelijk aan allocatie, geen drawdown, geen open posities.
    let mut sleeves_state = vec![SleeveState {
        sleeve_id: SleeveId::MicroFuturesMacroTrend,
        equity_usd: 2_000.0,
        realized_pnl_usd: 0.0,
        unrealized_pnl_usd: 0.0,
        peak_equity_usd: 2_000.0,
        open_positions: 0,
    }];

    // Margin-state: geen binding constraint.
    let margin_state = MarginState {
        internal_margin_req_usd: 0.0,
        broker_margin_req_usd: 0.0,
        equity_usd: 10_000.0,
    };

    // Vol-regime: normaal.
    let vol_regime = VolatilityRegime {
        rv10_annualized: 12.0,
        vix_level: 18.0,
        vix_term_slope: 0.3,
        regime_scalar: 1.0,
    };

    // Kernel-evaluatie → per-sleeve envelope
    let envelopes = kernel.evaluate(
        now.timestamp(),
        &portfolio_state,
        &mut sleeves_state,
        &margin_state,
        &vol_regime,
    );

    let env = envelopes
        .iter()
        .find(|e| e.sleeve_id == SleeveId::MicroFuturesMacroTrend)
        .expect("Expected envelope for MicroFuturesMacroTrend");

    assert!(
        env.max_position_size_usd > 0.0,
        "Precondition: expected positive max_position_size_usd from kernel"
    );
    assert!(
        env.exposure_remaining_usd > 0.0,
        "Precondition: expected positive exposure_remaining_usd from kernel"
    );

    // === 2) Macro futures sleeve context opbouwen ===

    let cfg = MacroFuturesSleeveConfig::default();
    let sleeve = MacroFuturesSleeve::new(cfg);

    // Historie voor alle drie instrumenten (MES, MNQ, 6E)
    let mes_hist = make_history_for_test(FutureInstrument::Mes, 100.0, now);
    let mnq_hist = make_history_for_test(FutureInstrument::Mnq, 16_000.0, now);
    let sixe_hist = make_history_for_test(FutureInstrument::SixE, 1.10, now);

    let mut histories = HashMap::new();
    histories.insert(FutureInstrument::Mes, mes_hist);
    histories.insert(FutureInstrument::Mnq, mnq_hist);
    histories.insert(FutureInstrument::SixE, sixe_hist);

    let macro_scalars = MacroScalars {
        as_of: now,
        risk_on_scalar: 1.0,
        usd_scalar: 1.0,
    };

    // Geen open posities in deze integratietest
    let current_positions: HashMap<FutureInstrument, i32> = HashMap::new();

    let as_of = fixed_as_of();
    let risk_envelope = base_risk_envelope();


    let ctx = FuturesSleeveContext {
        as_of,
        histories,
        macro_scalars,
        risk_envelope,
        current_positions,
        eur_per_usd: 0.92,
        engine_health: EngineHealth::Healthy, // default
    };


    // Risk-budget redelijk ruim; globale risk-kernel headroom moet bindend zijn.
    let risk_budget = FuturesRiskBudget {
        mes: InstrumentRiskBudget {
            max_risk_per_position_eur: 1_000.0,
            max_contracts: 10,
        },
        mnq: InstrumentRiskBudget {
            max_risk_per_position_eur: 1_000.0,
            max_contracts: 10,
        },
        sixe: InstrumentRiskBudget {
            max_risk_per_position_eur: 1_000.0,
            max_contracts: 10,
        },
        max_total_contracts: 10,
    };

    // Sleeve-plan met een ruime EUR-risk-cap (2x capital alloc)
    let max_sleeve_risk_eur = sleeve_cfg.capital_alloc_usd * 2.0;
    let plan = sleeve.plan_sleeve(&ctx, &risk_budget, max_sleeve_risk_eur);

    // Sanity: cap mag niet getriggerd worden in dit scenario.
    assert_eq!(
        plan.sanity,
        SleeveRiskSanity::Ok,
        "Expected sanity Ok when max_sleeve_risk_eur >> actual total risk"
    );

    // 1) Aggregate notional mag kernel-headroom niet overschrijden
    assert!(
        plan.aggregate.total_notional_usd <= env.exposure_remaining_usd + 1e-6,
        "Sleeve aggregate notional {} must not exceed exposure_remaining_usd {} from GlobalRiskKernel",
        plan.aggregate.total_notional_usd,
        env.exposure_remaining_usd
    );

    // 2) Aantal instrumenten mag concurrency-limit niet overschrijden
    assert!(
        (plan.aggregate.instrument_count as u32) <= env.max_concurrent_positions,
        "Instrument count {} must not exceed max_concurrent_positions {} from GlobalRiskKernel",
        plan.aggregate.instrument_count,
        env.max_concurrent_positions
    );
}

#[test]
fn run_heartbeat_matches_plan_and_order_intents() {
    let cfg = MacroFuturesSleeveConfig::default();
    let sleeve = MacroFuturesSleeve::new(cfg);
    let now = Utc::now();

    // MES + MNQ + 6E met normale signaallogica
    let mes_hist = make_history_for_test(FutureInstrument::Mes, 100.0, now);
    let mnq_hist = make_history_for_test(FutureInstrument::Mnq, 16_000.0, now);
    let sixe_hist = make_history_for_test(FutureInstrument::SixE, 1.10, now);

    let mut histories = HashMap::new();
    histories.insert(FutureInstrument::Mes, mes_hist);
    histories.insert(FutureInstrument::Mnq, mnq_hist);
    histories.insert(FutureInstrument::SixE, sixe_hist);

    let macro_scalars = MacroScalars {
        as_of: now,
        risk_on_scalar: 1.0,
        usd_scalar: 1.0,
    };

    let risk_envelope = SleeveRiskEnvelope {
        sleeve_id: SleeveId::MicroFuturesMacroTrend,
        sleeve_halt: HaltState::None,
        portfolio_halt: HaltState::None,

        max_position_size_usd: 10_000.0,
        max_concurrent_positions: 3,

        exposure_remaining_usd: 10_000.0,
        margin_remaining_usd: 10_000.0,

        volatility_regime_scalar: 1.0,
        leverage_scalar: 1.0,

        portfolio_risk_state: PortfolioRiskState::Normal,
    };

    let current_positions: HashMap<FutureInstrument, i32> = HashMap::new();

    let as_of = fixed_as_of();

    let ctx = FuturesSleeveContext {
    as_of,
    histories,
    macro_scalars,
    risk_envelope,
    current_positions,
    eur_per_usd: 0.92,
    engine_health: EngineHealth::Healthy, // default
};


    let risk_budget = FuturesRiskBudget {
        mes: InstrumentRiskBudget { max_risk_per_position_eur: 1_000_000.0, max_contracts: 100 },
        mnq: InstrumentRiskBudget { max_risk_per_position_eur: 1_000_000.0, max_contracts: 100 },
        sixe: InstrumentRiskBudget { max_risk_per_position_eur: 1_000_000.0, max_contracts: 100 },
        max_total_contracts: 100,
    };

    // Referentie: losse API’s
    let ref_contracts = sleeve.plan_contracts(&ctx, &risk_budget);
    let ref_report = sleeve.plan_risk_report(&ctx, &risk_budget);
    let ref_agg = sleeve.aggregate_sleeve_risk(&ctx, &risk_budget);
    let ref_sanity = sleeve.check_sleeve_risk_sanity(&ctx, &risk_budget, ref_agg.total_risk_eur * 2.0);
    let ref_orders = sleeve.plan_order_intents(&ctx, &risk_budget);

    // Heartbeat-output
    let hb = sleeve.run_heartbeat(&ctx, &risk_budget, ref_agg.total_risk_eur * 2.0);

    // 1) Contracts consistent
    assert_eq!(
        ref_contracts.len(),
        hb.sleeve_plan.planned_contracts.len(),
        "Heartbeat planned_contracts length must equal direct plan_contracts"
    );

    // 2) Risk report consistent
    assert_eq!(
        ref_report.len(),
        hb.sleeve_plan.risk_report.len(),
        "Heartbeat risk_report length must equal direct plan_risk_report"
    );

    // 3) Aggregate consistent
    assert_eq!(
        ref_agg.total_contracts_abs,
        hb.sleeve_plan.aggregate.total_contracts_abs,
        "Heartbeat aggregate.total_contracts_abs mismatch"
    );
    let diff_risk = (ref_agg.total_risk_eur - hb.sleeve_plan.aggregate.total_risk_eur).abs();
    assert!(
        diff_risk < 1e-6,
        "Heartbeat aggregate.total_risk_eur mismatch: direct={} via_hb={} diff={}",
        ref_agg.total_risk_eur,
        hb.sleeve_plan.aggregate.total_risk_eur,
        diff_risk
    );

    // 4) Sanity consistent
    assert_eq!(
        ref_sanity,
        hb.sleeve_plan.sanity,
        "Heartbeat sanity must match direct check_sleeve_risk_sanity"
    );

    // 5) Orders consistent
    assert_eq!(
        ref_orders.len(),
        hb.order_intents.len(),
        "Heartbeat order_intents length must equal direct plan_order_intents"
    );
}

#[test]
fn map_heartbeat_to_engine_orders_respects_side_quantity_and_metadata() {
    let cfg = MacroFuturesSleeveConfig::default();
    let sleeve = MacroFuturesSleeve::new(cfg);
    let now = Utc::now();

    // Historie voor alle drie instrumenten
    let mes_hist = make_history_for_test(FutureInstrument::Mes, 100.0, now);
    let mnq_hist = make_history_for_test(FutureInstrument::Mnq, 16_000.0, now);
    let sixe_hist = make_history_for_test(FutureInstrument::SixE, 1.10, now);

    let mut histories = HashMap::new();
    histories.insert(FutureInstrument::Mes, mes_hist);
    histories.insert(FutureInstrument::Mnq, mnq_hist);
    histories.insert(FutureInstrument::SixE, sixe_hist);

    let macro_scalars = MacroScalars {
        as_of: now,
        risk_on_scalar: 1.0,
        usd_scalar: 1.0,
    };

    let risk_envelope = SleeveRiskEnvelope {
        sleeve_id: SleeveId::MicroFuturesMacroTrend,
        sleeve_halt: HaltState::None,
        portfolio_halt: HaltState::None,

        max_position_size_usd: 10_000.0,
        max_concurrent_positions: 3,

        exposure_remaining_usd: 10_000.0,
        margin_remaining_usd: 10_000.0,

        volatility_regime_scalar: 1.0,
        leverage_scalar: 1.0,

        portfolio_risk_state: PortfolioRiskState::Normal,
    };

    let current_positions: HashMap<FutureInstrument, i32> = HashMap::new();

    let as_of = fixed_as_of();

    let ctx = FuturesSleeveContext {
    as_of,
    histories,
    macro_scalars,
    risk_envelope,
    current_positions,
    eur_per_usd: 0.92,
    engine_health: EngineHealth::Healthy, // default
};


    let risk_budget = FuturesRiskBudget {
        mes: InstrumentRiskBudget { max_risk_per_position_eur: 1_000_000.0, max_contracts: 100 },
        mnq: InstrumentRiskBudget { max_risk_per_position_eur: 1_000_000.0, max_contracts: 100 },
        sixe: InstrumentRiskBudget { max_risk_per_position_eur: 1_000_000.0, max_contracts: 100 },
        max_total_contracts: 100,
    };

    // Heartbeat draaien als referentie
    let agg = sleeve.aggregate_sleeve_risk(&ctx, &risk_budget);
    let hb = sleeve.run_heartbeat(&ctx, &risk_budget, agg.total_risk_eur * 2.0);

    // Engine-orders mappen
    let engine_orders = sleeve.map_heartbeat_to_engine_orders(
        SleeveId::MicroFuturesMacroTrend,
        &hb,
    );

    // 1) Zelfde aantal entries als order_intents met delta != 0
    let non_zero_order_intents: Vec<_> = hb
        .order_intents
        .iter()
        .filter(|oi| oi.delta_contracts != 0)
        .collect();

    assert_eq!(
        engine_orders.len(),
        non_zero_order_intents.len(),
        "EngineOrders count must match non-zero FuturesOrderIntents"
    );

    // 2) Elke EngineOrder is consistent met de bijbehorende FuturesOrderIntent
    for eo in &engine_orders {
        let oi = non_zero_order_intents
            .iter()
            .find(|oi| oi.instrument == eo.instrument)
            .expect("Missing matching FuturesOrderIntent for EngineOrder");

        let delta = oi.delta_contracts;
        let expected_side = if delta > 0 {
            EngineOrderSide::Buy
        } else {
            EngineOrderSide::Sell
        };

        assert_eq!(
            eo.side, expected_side,
            "EngineOrder side should match sign of delta_contracts"
        );

        assert_eq!(
            eo.quantity,
            delta.abs(),
            "EngineOrder quantity should be abs(delta_contracts)"
        );

        // Symbol/venue mapping check
        match eo.instrument {
            FutureInstrument::Mes => {
                assert_eq!(eo.symbol, "MES");
                assert_eq!(eo.venue, "CME");
            }
            FutureInstrument::Mnq => {
                assert_eq!(eo.symbol, "MNQ");
                assert_eq!(eo.venue, "CME");
            }
            FutureInstrument::SixE => {
                assert_eq!(eo.symbol, "6E");
                assert_eq!(eo.venue, "CME");
            }
        }

        // Sleeve-id moet door-gemapped zijn
        assert_eq!(
            eo.sleeve_id,
            SleeveId::MicroFuturesMacroTrend,
            "EngineOrder must carry the correct sleeve_id"
        );
    }
}

#[test]
fn in_memory_order_sink_collects_engine_orders() {
    let cfg = MacroFuturesSleeveConfig::default();
    let sleeve = MacroFuturesSleeve::new(cfg);
    let now = Utc::now();

    // Historie voor alle drie instrumenten
    let mes_hist = make_history_for_test(FutureInstrument::Mes, 100.0, now);
    let mnq_hist = make_history_for_test(FutureInstrument::Mnq, 16_000.0, now);
    let sixe_hist = make_history_for_test(FutureInstrument::SixE, 1.10, now);

    let mut histories = HashMap::new();
    histories.insert(FutureInstrument::Mes, mes_hist);
    histories.insert(FutureInstrument::Mnq, mnq_hist);
    histories.insert(FutureInstrument::SixE, sixe_hist);

    let macro_scalars = MacroScalars {
        as_of: now,
        risk_on_scalar: 1.0,
        usd_scalar: 1.0,
    };

    let risk_envelope = SleeveRiskEnvelope {
        sleeve_id: SleeveId::MicroFuturesMacroTrend,
        sleeve_halt: HaltState::None,
        portfolio_halt: HaltState::None,

        max_position_size_usd: 10_000.0,
        max_concurrent_positions: 3,

        exposure_remaining_usd: 10_000.0,
        margin_remaining_usd: 10_000.0,

        volatility_regime_scalar: 1.0,
        leverage_scalar: 1.0,

        portfolio_risk_state: PortfolioRiskState::Normal,
    };

    let current_positions: HashMap<FutureInstrument, i32> = HashMap::new();

    let as_of = fixed_as_of();

    let ctx = FuturesSleeveContext {
    as_of,
    histories,
    macro_scalars,
    risk_envelope,
    current_positions,
    eur_per_usd: 0.92,
    engine_health: EngineHealth::Healthy, // default
};


    let risk_budget = FuturesRiskBudget {
        mes: InstrumentRiskBudget { max_risk_per_position_eur: 1_000_000.0, max_contracts: 100 },
        mnq: InstrumentRiskBudget { max_risk_per_position_eur: 1_000_000.0, max_contracts: 100 },
        sixe: InstrumentRiskBudget { max_risk_per_position_eur: 1_000_000.0, max_contracts: 100 },
        max_total_contracts: 100,
    };

    // Heartbeat + mapping naar EngineOrders
    let agg = sleeve.aggregate_sleeve_risk(&ctx, &risk_budget);
    let hb = sleeve.run_heartbeat(&ctx, &risk_budget, agg.total_risk_eur * 2.0);
    let engine_orders = sleeve.map_heartbeat_to_engine_orders(
        SleeveId::MicroFuturesMacroTrend,
        &hb,
    );

    // In-memory sink
    let mut sink = InMemoryOrderSink::new();

    for order in &engine_orders {
        sink.submit(order);
    }

    // 1) Aantal orders moet identiek zijn
    assert_eq!(
        sink.orders.len(),
        engine_orders.len(),
        "InMemoryOrderSink should store the same number of EngineOrders as submitted"
    );

    // 2) Inhoud moet één-op-één overeenkomen (dankzij PartialEq/Eq)
    for (expected, stored) in engine_orders.iter().zip(sink.orders.iter()) {
        assert_eq!(
            stored, expected,
            "Stored EngineOrder in sink must equal submitted EngineOrder"
        );
    }
}

#[test]
fn run_macro_futures_engine_heartbeat_end_to_end() {
    let now = Utc::now();

    // === 1) Global risk kernel config & state ===
    let portfolio_cfg = PortfolioRiskConfig {
        initial_equity_usd: 10_000.0,
        halt_dd_frac: -0.08,
        kill_dd_frac: -0.12,
        max_leverage: 1.5,
        rebalance_drift_frac: 0.15,
        max_global_positions: 10,
    };

    let sleeve_cfg = SleeveRiskConfig {
        sleeve_id: SleeveId::MicroFuturesMacroTrend,
        capital_alloc_usd: 2_000.0,
        max_single_pos_risk_frac: 0.01,
        halt_dd_frac: -0.10,
        kill_dd_frac: -0.15,
        max_concurrent_positions: 3,
    };

    let gcfg = GlobalRiskKernelConfig {
        portfolio: portfolio_cfg,
        sleeves: vec![sleeve_cfg],
    };

    let mut kernel = GlobalRiskKernel::new(gcfg);

    let portfolio_state = PortfolioState {
        cash_usd: 10_000.0,
        open_pnl_usd: 0.0,
        accrued_interest_usd: 0.0,
        peak_equity_usd: 10_000.0,
        total_notional_exposure: 0.0,
        current_leverage: 0.0,
    };

    let mut sleeve_state = SleeveState {
        sleeve_id: SleeveId::MicroFuturesMacroTrend,
        equity_usd: 2_000.0,
        realized_pnl_usd: 0.0,
        unrealized_pnl_usd: 0.0,
        peak_equity_usd: 2_000.0,
        open_positions: 0,
    };

    let margin_state = MarginState {
        internal_margin_req_usd: 0.0,
        broker_margin_req_usd: 0.0,
        equity_usd: 10_000.0,
    };

    let vol_regime = VolatilityRegime {
        rv10_annualized: 12.0,
        vix_level: 18.0,
        vix_term_slope: 0.3,
        regime_scalar: 1.0,
    };

    // === 2) Macro futures sleeve & context ===
    let cfg = MacroFuturesSleeveConfig::default();
    let sleeve = MacroFuturesSleeve::new(cfg);

    let mes_hist = make_history_for_test(FutureInstrument::Mes, 100.0, now);
    let mnq_hist = make_history_for_test(FutureInstrument::Mnq, 16_000.0, now);
    let sixe_hist = make_history_for_test(FutureInstrument::SixE, 1.10, now);

    let mut histories = HashMap::new();
    histories.insert(FutureInstrument::Mes, mes_hist);
    histories.insert(FutureInstrument::Mnq, mnq_hist);
    histories.insert(FutureInstrument::SixE, sixe_hist);

    let macro_scalars = MacroScalars {
        as_of: now,
        risk_on_scalar: 1.0,
        usd_scalar: 1.0,
    };

    let current_positions: HashMap<FutureInstrument, i32> = HashMap::new();

    let risk_budget = FuturesRiskBudget {
        mes: InstrumentRiskBudget { max_risk_per_position_eur: 1_000.0, max_contracts: 10 },
        mnq: InstrumentRiskBudget { max_risk_per_position_eur: 1_000.0, max_contracts: 10 },
        sixe: InstrumentRiskBudget { max_risk_per_position_eur: 1_000.0, max_contracts: 10 },
        max_total_contracts: 10,
    };

    // Sleeve-risk cap ruim boven allocatie
    let max_sleeve_risk_eur = 4_000.0;

    let mut sink = InMemoryOrderSink::new();

    // === 3) End-to-end heartbeat call ===
    let result = run_macro_futures_engine_heartbeat(
        now.timestamp(),
        &mut kernel,
        &portfolio_state,
        &mut sleeve_state,
        &margin_state,
        &vol_regime,
        &sleeve,
        histories,
        macro_scalars,
        current_positions,
        1.0,            // eur_per_usd
        &risk_budget,
        max_sleeve_risk_eur,
        &mut sink,
    );

    // 1) Envelope moet een niet-nul max_position_size_usd hebben
    assert!(
        result.envelope.max_position_size_usd > 0.0,
        "Expected positive max_position_size_usd in heartbeat result"
    );

    // 2) Sleeve sanity moet OK zijn onder ruime cap
    assert_eq!(
        result.heartbeat.sleeve_plan.sanity,
        SleeveRiskSanity::Ok,
        "Expected SleeveRiskSanity::Ok under generous sleeve risk cap"
    );

    // 3) EngineOrders moeten consistent zijn met sink
    assert_eq!(
        sink.orders.len(),
        result.engine_orders.len(),
        "Sink must receive exactly the EngineOrders produced by the heartbeat"
    );

    // 4) Er moet in elk geval óf geen orders zijn (flat regime) óf positieve qty's
    for order in &result.engine_orders {
        assert!(
            order.quantity > 0,
            "EngineOrder quantity must be > 0; got {}",
            order.quantity
        );
    }
}

#[test]
fn encode_order_log_event_json_contains_core_fields() {
    let now = Utc::now();

    // 1) Klein scenario: hergebruik end-to-end heartbeat setup
    let portfolio_cfg = PortfolioRiskConfig {
        initial_equity_usd: 10_000.0,
        halt_dd_frac: -0.08,
        kill_dd_frac: -0.12,
        max_leverage: 1.5,
        rebalance_drift_frac: 0.15,
        max_global_positions: 10,
    };

    let sleeve_cfg = SleeveRiskConfig {
        sleeve_id: SleeveId::MicroFuturesMacroTrend,
        capital_alloc_usd: 2_000.0,
        max_single_pos_risk_frac: 0.01,
        halt_dd_frac: -0.10,
        kill_dd_frac: -0.15,
        max_concurrent_positions: 3,
    };

    let gcfg = GlobalRiskKernelConfig {
        portfolio: portfolio_cfg,
        sleeves: vec![sleeve_cfg],
    };

    let mut kernel = GlobalRiskKernel::new(gcfg);

    let portfolio_state = PortfolioState {
        cash_usd: 10_000.0,
        open_pnl_usd: 0.0,
        accrued_interest_usd: 0.0,
        peak_equity_usd: 10_000.0,
        total_notional_exposure: 0.0,
        current_leverage: 0.0,
    };

    let mut sleeve_state = SleeveState {
        sleeve_id: SleeveId::MicroFuturesMacroTrend,
        equity_usd: 2_000.0,
        realized_pnl_usd: 0.0,
        unrealized_pnl_usd: 0.0,
        peak_equity_usd: 2_000.0,
        open_positions: 0,
    };

    let margin_state = MarginState {
        internal_margin_req_usd: 0.0,
        broker_margin_req_usd: 0.0,
        equity_usd: 10_000.0,
    };

    let vol_regime = VolatilityRegime {
        rv10_annualized: 12.0,
        vix_level: 18.0,
        vix_term_slope: 0.3,
        regime_scalar: 1.0,
    };

    let cfg = MacroFuturesSleeveConfig::default();
    let sleeve = MacroFuturesSleeve::new(cfg);

    let mes_hist = make_history_for_test(FutureInstrument::Mes, 100.0, now);
    let mnq_hist = make_history_for_test(FutureInstrument::Mnq, 16_000.0, now);
    let sixe_hist = make_history_for_test(FutureInstrument::SixE, 1.10, now);

    let mut histories = HashMap::new();
    histories.insert(FutureInstrument::Mes, mes_hist);
    histories.insert(FutureInstrument::Mnq, mnq_hist);
    histories.insert(FutureInstrument::SixE, sixe_hist);

    let macro_scalars = MacroScalars {
        as_of: now,
        risk_on_scalar: 1.0,
        usd_scalar: 1.0,
    };

    let current_positions: HashMap<FutureInstrument, i32> = HashMap::new();

    let risk_budget = FuturesRiskBudget {
        mes: InstrumentRiskBudget { max_risk_per_position_eur: 1_000.0, max_contracts: 10 },
        mnq: InstrumentRiskBudget { max_risk_per_position_eur: 1_000.0, max_contracts: 10 },
        sixe: InstrumentRiskBudget { max_risk_per_position_eur: 1_000.0, max_contracts: 10 },
        max_total_contracts: 10,
    };

    let max_sleeve_risk_eur = 4_000.0;
    let mut sink = InMemoryOrderSink::new();

    let result = run_macro_futures_engine_heartbeat(
        now.timestamp(),
        &mut kernel,
        &portfolio_state,
        &mut sleeve_state,
        &margin_state,
        &vol_regime,
        &sleeve,
        histories,
        macro_scalars,
        current_positions,
        1.0,
        &risk_budget,
        max_sleeve_risk_eur,
        &mut sink,
    );

    if result.engine_orders.is_empty() {
        // In extreem geval van flat regime: niks te checken
        return;
    }

    let first_order = &result.engine_orders[0];

    let json = encode_order_log_event_json(first_order, now.timestamp());

    // 2) Basis sanity: valide JSON + kernvelden aanwezig
    assert!(
        json.contains("\"symbol\""),
        "JSON moet symbol veld bevatten, got: {}",
        json
    );
    assert!(
        json.contains(&format!("\"{}\"", first_order.symbol)),
        "JSON moet het symbool bevatten ({})",
        first_order.symbol
    );
    assert!(
        json.contains(&format!("\"quantity\":{}", first_order.quantity)),
        "JSON moet de quantity bevatten ({})",
        first_order.quantity
    );
}

#[test]
fn file_order_sink_writes_json_lines() {
    // Maak tijdelijke path
    let mut path = env::temp_dir();
    path.push("macro_futures_file_order_sink_test.jsonl");

    // Zorg dat we schoon starten (ignoreren fout als file niet bestaat)
    let _ = fs::remove_file(&path);

    let mut sink = FileOrderSink::new(&path);

    // Simpele EngineOrder
    let order = EngineOrder {
        sleeve_id: SleeveId::MicroFuturesMacroTrend,
        instrument: FutureInstrument::Mes,
        symbol: "MES",
        venue: "CME",
        side: EngineOrderSide::Buy,
        quantity: 3,
    };

    sink.submit(&order);

    // File moet nu bestaan en minstens één regel bevatten met JSON
    let contents = fs::read_to_string(&path)
        .expect("Expected log file to be created by FileOrderSink");

    let lines: Vec<&str> = contents.lines().collect();
    assert!(
        !lines.is_empty(),
        "Expected at least one JSON line in log file"
    );

    let first_line = lines[0];
    assert!(
        first_line.contains("\"symbol\":\"MES\""),
        "First JSON line should contain MES symbol, got: {}",
        first_line
    );
    assert!(
        first_line.contains("\"quantity\":3"),
        "First JSON line should contain quantity 3, got: {}",
        first_line
    );

    // Cleanup (best-effort)
    let _ = fs::remove_file(&path);
}

#[test]
fn encode_heartbeat_log_event_json_contains_risk_and_orders() {
    let now = Utc::now();

    // === 1) Kernel config & state (zelfde patroon als andere integratietests) ===
    let portfolio_cfg = PortfolioRiskConfig {
        initial_equity_usd: 10_000.0,
        halt_dd_frac: -0.08,
        kill_dd_frac: -0.12,
        max_leverage: 1.5,
        rebalance_drift_frac: 0.15,
        max_global_positions: 10,
    };

    let sleeve_cfg = SleeveRiskConfig {
        sleeve_id: SleeveId::MicroFuturesMacroTrend,
        capital_alloc_usd: 2_000.0,
        max_single_pos_risk_frac: 0.01,
        halt_dd_frac: -0.10,
        kill_dd_frac: -0.15,
        max_concurrent_positions: 3,
    };

    let gcfg = GlobalRiskKernelConfig {
        portfolio: portfolio_cfg,
        sleeves: vec![sleeve_cfg],
    };

    let mut kernel = GlobalRiskKernel::new(gcfg);

    let portfolio_state = PortfolioState {
        cash_usd: 10_000.0,
        open_pnl_usd: 0.0,
        accrued_interest_usd: 0.0,
        peak_equity_usd: 10_000.0,
        total_notional_exposure: 0.0,
        current_leverage: 0.0,
    };

    let mut sleeve_state = SleeveState {
        sleeve_id: SleeveId::MicroFuturesMacroTrend,
        equity_usd: 2_000.0,
        realized_pnl_usd: 0.0,
        unrealized_pnl_usd: 0.0,
        peak_equity_usd: 2_000.0,
        open_positions: 0,
    };

    let margin_state = MarginState {
        internal_margin_req_usd: 0.0,
        broker_margin_req_usd: 0.0,
        equity_usd: 10_000.0,
    };

    let vol_regime = VolatilityRegime {
        rv10_annualized: 12.0,
        vix_level: 18.0,
        vix_term_slope: 0.3,
        regime_scalar: 1.0,
    };

    // === 2) Macro futures sleeve & context ===
    let cfg = MacroFuturesSleeveConfig::default();
    let sleeve = MacroFuturesSleeve::new(cfg);

    let mes_hist = make_history_for_test(FutureInstrument::Mes, 100.0, now);
    let mnq_hist = make_history_for_test(FutureInstrument::Mnq, 16_000.0, now);
    let sixe_hist = make_history_for_test(FutureInstrument::SixE, 1.10, now);

    let mut histories = HashMap::new();
    histories.insert(FutureInstrument::Mes, mes_hist);
    histories.insert(FutureInstrument::Mnq, mnq_hist);
    histories.insert(FutureInstrument::SixE, sixe_hist);

    let macro_scalars = MacroScalars {
        as_of: now,
        risk_on_scalar: 1.0,
        usd_scalar: 1.0,
    };

    let current_positions: HashMap<FutureInstrument, i32> = HashMap::new();

    let risk_budget = FuturesRiskBudget {
        mes: InstrumentRiskBudget { max_risk_per_position_eur: 1_000.0, max_contracts: 10 },
        mnq: InstrumentRiskBudget { max_risk_per_position_eur: 1_000.0, max_contracts: 10 },
        sixe: InstrumentRiskBudget { max_risk_per_position_eur: 1_000.0, max_contracts: 10 },
        max_total_contracts: 10,
    };

    let max_sleeve_risk_eur = 4_000.0;

    let mut sink = InMemoryOrderSink::new();

    let result = run_macro_futures_engine_heartbeat(
        now.timestamp(),
        &mut kernel,
        &portfolio_state,
        &mut sleeve_state,
        &margin_state,
        &vol_regime,
        &sleeve,
        histories,
        macro_scalars,
        current_positions,
        1.0,
        &risk_budget,
        max_sleeve_risk_eur,
        &mut sink,
    );

    let now_ts: i64 = 1_700_000_000;
    let json = encode_heartbeat_log_event_json(now_ts, &result, EngineHealth::Healthy);
    // 1) JSON moet baseline velden bevatten
    assert!(
        json.contains("\"sleeve_id\""),
        "Heartbeat JSON moet sleeve_id bevatten, got: {}",
        json
    );
    assert!(
        json.contains("\"portfolio_risk_state\""),
        "Heartbeat JSON moet portfolio_risk_state bevatten, got: {}",
        json
    );
    assert!(
        json.contains("\"total_risk_eur\""),
        "Heartbeat JSON moet total_risk_eur bevatten, got: {}",
        json
    );
    assert!(
        json.contains("\"orders\""),
        "Heartbeat JSON moet orders veld bevatten, got: {}",
        json
    );
    assert!(
        json.contains("\"engine_health\""),
        "expected engine_health field in heartbeat json, got: {}",
        json
    );
    assert!(
        json.contains("\"Healthy\""),
        "expected engine_health=Healthy in heartbeat json, got: {}",
        json
    );


    // 2) Als er orders zijn, moet de eerste order ook in de JSON terugkomen
    if !result.engine_orders.is_empty() {
        let first = &result.engine_orders[0];
        assert!(
            json.contains(&format!("\"symbol\":\"{}\"", first.symbol)),
            "Heartbeat JSON moet het symbool van de eerste order bevatten ({})",
            first.symbol
        );
    }
}

#[test]
fn stdout_heartbeat_logger_writes_exact_line_plus_newline() {
    // Arrange: in-memory buffer als fake-stdout
    let buffer: Vec<u8> = Vec::new();
    let cursor = Cursor::new(buffer);
    let mut logger = StdoutHeartbeatLogger::with_writer(cursor);

    let json_line = r#"{"ts_utc":123,"sleeve_id":"MicroFuturesMacroTrend"}"#;

    // Act
    logger.log(json_line);

    // Assert
    let cursor = logger.into_inner();
    let written_bytes = cursor.into_inner();
    let written = String::from_utf8(written_bytes).expect("valid utf8");
    assert_eq!(written, format!("{}\n", json_line));
}

#[test]
fn stdout_heartbeat_logger_flush_does_not_change_buffer() {
    let buffer: Vec<u8> = Vec::new();
    let cursor = Cursor::new(buffer);
    let mut logger = StdoutHeartbeatLogger::with_writer(cursor);

    // mag simpelweg niet panic'en
    logger.flush();

    let cursor = logger.into_inner();
    let written_bytes = cursor.into_inner();
    assert!(written_bytes.is_empty());
}

#[test]
fn run_macro_futures_engine_heartbeat_with_logging_emits_single_json_line() {
    let now = Utc::now();

    // === 1) Global risk kernel config & state ===
    let portfolio_cfg = PortfolioRiskConfig {
        initial_equity_usd: 10_000.0,
        halt_dd_frac: -0.08,
        kill_dd_frac: -0.12,
        max_leverage: 1.5,
        rebalance_drift_frac: 0.15,
        max_global_positions: 10,
    };

    let sleeve_cfg = SleeveRiskConfig {
        sleeve_id: SleeveId::MicroFuturesMacroTrend,
        capital_alloc_usd: 2_000.0,
        max_single_pos_risk_frac: 0.01, // 1% van 2k = 20 USD base size
        halt_dd_frac: -0.10,
        kill_dd_frac: -0.15,
        max_concurrent_positions: 3,
    };

    let gcfg = GlobalRiskKernelConfig {
        portfolio: portfolio_cfg,
        sleeves: vec![sleeve_cfg],
    };

    let mut kernel = GlobalRiskKernel::new(gcfg);

    let portfolio_state = PortfolioState {
        cash_usd: 10_000.0,
        open_pnl_usd: 0.0,
        accrued_interest_usd: 0.0,
        peak_equity_usd: 10_000.0,
        total_notional_exposure: 0.0,
        current_leverage: 0.0,
    };

    let mut sleeve_state = SleeveState {
        sleeve_id: SleeveId::MicroFuturesMacroTrend,
        equity_usd: 2_000.0,
        realized_pnl_usd: 0.0,
        unrealized_pnl_usd: 0.0,
        peak_equity_usd: 2_000.0,
        open_positions: 0,
    };

    let margin_state = MarginState {
        internal_margin_req_usd: 0.0,
        broker_margin_req_usd: 0.0,
        equity_usd: 10_000.0,
    };

    let vol_regime = VolatilityRegime {
        rv10_annualized: 12.0,
        vix_level: 18.0,
        vix_term_slope: 0.3,
        regime_scalar: 1.0,
    };

    // === 2) Macro futures sleeve & histories ===
    let cfg = MacroFuturesSleeveConfig::default();
    let sleeve = MacroFuturesSleeve::new(cfg);

    let mes_hist = make_history_for_test(FutureInstrument::Mes, 100.0, now);
    let mnq_hist = make_history_for_test(FutureInstrument::Mnq, 16_000.0, now);
    let sixe_hist = make_history_for_test(FutureInstrument::SixE, 1.10, now);

    let mut histories = HashMap::new();
    histories.insert(FutureInstrument::Mes, mes_hist);
    histories.insert(FutureInstrument::Mnq, mnq_hist);
    histories.insert(FutureInstrument::SixE, sixe_hist);

    let macro_scalars = MacroScalars {
        as_of: now,
        risk_on_scalar: 1.0,
        usd_scalar: 1.0,
    };

    let current_positions: HashMap<FutureInstrument, i32> = HashMap::new();

    let risk_budget = FuturesRiskBudget {
        mes: InstrumentRiskBudget {
            max_risk_per_position_eur: 1_000.0,
            max_contracts: 10,
        },
        mnq: InstrumentRiskBudget {
            max_risk_per_position_eur: 1_000.0,
            max_contracts: 10,
        },
        sixe: InstrumentRiskBudget {
            max_risk_per_position_eur: 1_000.0,
            max_contracts: 10,
        },
        max_total_contracts: 10,
    };

    // Sleeve-risk cap ruim boven allocatie
    let max_sleeve_risk_eur = 4_000.0;

    let mut sink = InMemoryOrderSink::new();

    let buffer: Vec<u8> = Vec::new();
    let cursor = Cursor::new(buffer);
    let mut logger = StdoutHeartbeatLogger::with_writer(cursor);

    // ✅ Supervisor toevoegen
    let mut supervisor = HeartbeatSupervisor::new(65);

    // === 3) Heartbeat + logging wrapper ===
    let result = run_macro_futures_engine_heartbeat_with_logging(
        now.timestamp(),
        &mut supervisor,          // <--- nieuw
        &mut kernel,
        &portfolio_state,
        &mut sleeve_state,
        &margin_state,
        &vol_regime,
        &sleeve,
        histories,
        macro_scalars,
        current_positions,
        1.0, // eur_per_usd
        &risk_budget,
        max_sleeve_risk_eur,
        &mut sink,
        &mut logger,
    );


    // === 4) Inhoudelijke sanity, zelfde lijn als end-to-end test ===

    // Envelope moet een niet-nul max_position_size_usd hebben
    assert!(
        result.envelope.max_position_size_usd > 0.0,
        "Expected positive max_position_size_usd in heartbeat result"
    );

    // Sleeve sanity moet OK zijn onder ruime cap
    assert_eq!(
        result.heartbeat.sleeve_plan.sanity,
        SleeveRiskSanity::Ok,
        "Expected SleeveRiskSanity::Ok under generous sleeve risk cap"
    );

    // EngineOrders moeten consistent zijn met sink
    assert_eq!(
        sink.orders.len(),
        result.engine_orders.len(),
        "Sink must receive exactly the EngineOrders produced by the heartbeat"
    );

    // === 5) Logging-check: exact één JSON-regel, met kernvelden ===
    let cursor = logger.into_inner();
    let written_bytes = cursor.into_inner();
    let written = String::from_utf8(written_bytes).expect("valid utf8 from logger");

    assert!(
        written.ends_with('\n'),
        "expected newline-terminated JSON line, got: {:?}",
        written
    );

    let line = written.trim_end(); // strip '\n'

    assert!(
        line.starts_with('{') && line.ends_with('}'),
        "expected JSON object line, got: {}",
        line
    );

    // basisvelden uit HeartbeatLogEvent
    assert!(
        line.contains("\"sleeve_id\""),
        "expected sleeve_id field in heartbeat json, got: {}",
        line
    );
    assert!(
        line.contains("\"max_position_size_usd\""),
        "expected max_position_size_usd field in heartbeat json, got: {}",
        line
    );
    assert!(
        line.contains("\"orders\""),
        "expected orders field in heartbeat json, got: {}",
        line
    );
    assert!(
        line.contains("\"engine_health\""),
        "expected engine_health field in heartbeat json, got: {}",
        line
    );
}

struct SpySink {
    pub lines: RefCell<Vec<String>>,
}

impl SpySink {
    fn new() -> Self {
        Self { lines: RefCell::new(Vec::new()) }
    }
}

impl HeartbeatLogSink for SpySink {
    fn log(&mut self, line: &str) {
        self.lines.borrow_mut().push(line.to_string());
    }

    fn flush(&mut self) {
        // no-op
    }
}



#[test]
fn batching_heartbeat_logger_buffers_until_flush() {
    // 1) Deelbare SpySink die we kunnen inspecteren
    let spy = Rc::new(RefCell::new(SpySink::new()));

    // Maak een Box<dyn HeartbeatLogSink> dat dezelfde SpySink refereert
    let spy_box: Box<dyn HeartbeatLogSink> = {
        // Clone Rc en verpak in Box, *maar belangrijk: we moeten het Rc uitpakken naar &mut SpySink*.
        // Dus gebruiken we een dedicated wrapper:
        struct SpyWrapper(Rc<RefCell<SpySink>>);

        impl HeartbeatLogSink for SpyWrapper {
            fn log(&mut self, line: &str) {
                self.0.borrow_mut().log(line);
            }

            fn flush(&mut self) {
                self.0.borrow_mut().flush();
            }
        }

        Box::new(SpyWrapper(spy.clone()))
    };

    // 2) Batching-logger met capaciteit 3
    let mut logger = BatchingHeartbeatLogger::new(spy_box, 3);

    // 3) Log twee items -> geen flush
    logger.log("{\"a\":1}");
    logger.log("{\"b\":2}");

    assert_eq!(logger.buffered_len(), 2);

    // 4) Flush -> alles naar SpySink
    logger.flush();

    // 5) Inspecteer SpySink direct (geen downcast!)
    let spy_ref = spy.borrow();                 // Ref<SpySink>
    let lines_ref = spy_ref.lines.borrow();     // Ref<Vec<String>>

    assert_eq!(lines_ref.len(), 2);
    assert_eq!(lines_ref[0], "{\"a\":1}");
    assert_eq!(lines_ref[1], "{\"b\":2}");
}

#[test]
fn file_heartbeat_logger_rotates_and_writes_jsonl() {
    use chrono::{TimeZone, Datelike};

    // 1) Temp dir
    let mut dir = std::env::temp_dir();
    let unique = format!("engine_test_{}", chrono::Utc::now().timestamp_nanos());
    dir.push(unique);
    fs::create_dir_all(&dir).expect("cannot create test dir");

    let mut logger = FileHeartbeatLogger::new(&dir);

    // 2) Log op dag 1
    let d1 = chrono::Utc.ymd(2025, 11, 17).and_hms(10, 0, 0);
    logger.log_with_datetime(d1, "{\"ts\":1,\"msg\":\"A\"}");
    logger.log_with_datetime(d1, "{\"ts\":2,\"msg\":\"B\"}");
    logger.flush();

    let fname1 = format!("heartbeat-{:04}{:02}{:02}.jsonl",
        d1.year(), d1.month(), d1.day());
    let f1 = dir.join(&fname1);

    assert!(f1.exists(), "expected {} to exist", f1.display());

    let c1 = fs::read_to_string(&f1).unwrap();
    let lines1: Vec<_> = c1.trim_end().lines().collect();
    assert_eq!(lines1.len(), 2);
    assert_eq!(lines1[0], "{\"ts\":1,\"msg\":\"A\"}");
    assert_eq!(lines1[1], "{\"ts\":2,\"msg\":\"B\"}");

    // 3) Log op dag 2 → moet nieuwe file aanmaken
    let d2 = chrono::Utc.ymd(2025, 11, 18).and_hms(10, 0, 0);
    logger.log_with_datetime(d2, "{\"ts\":3,\"msg\":\"C\"}");
    logger.flush();

    let fname2 = format!("heartbeat-{:04}{:02}{:02}.jsonl",
        d2.year(), d2.month(), d2.day());
    let f2 = dir.join(&fname2);

    assert!(f2.exists(), "expected {} to exist", f2.display());

    let c2 = fs::read_to_string(&f2).unwrap();
    assert!(c2.contains("\"msg\":\"C\""));

    // Cleanup
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn supervisor_stays_healthy_when_no_gap() {
    let mut sup = HeartbeatSupervisor::new(60);
    sup.register_tick(1000);
    sup.register_tick(1050);
    assert_eq!(sup.health(), EngineHealth::Healthy);
}

#[test]
fn supervisor_flags_degraded_on_large_gap() {
    let mut sup = HeartbeatSupervisor::new(60);
    sup.register_tick(1000);
    sup.register_tick(2000); // 1000 sec gap
    assert_eq!(sup.health(), EngineHealth::Degraded);
}

#[test]
fn supervisor_recovers_to_healthy_when_gap_normalizes() {
    let mut sup = HeartbeatSupervisor::new(60);
    sup.register_tick(1000);
    sup.register_tick(2000); // degraded
    assert_eq!(sup.health(), EngineHealth::Degraded);

    sup.register_tick(2050); // gap = 50 sec
    assert_eq!(sup.health(), EngineHealth::Healthy);
}

#[test]
fn encode_supervisor_event_json_basic() {
    let ev = HeartbeatSupervisorEvent {
        ts_utc: 1234,
        status: EngineHealth::Degraded,
        msg: "heartbeat_gap_detected",
    };

    let s = encode_supervisor_event_json(&ev);

    assert!(s.contains("\"ts_utc\":1234"));
    assert!(s.contains("\"status\":\"Degraded\""));
    assert!(s.contains("\"heartbeat_gap_detected\""));
}

#[test]
fn heartbeat_supervisor_stays_healthy_on_small_gaps() {
    let mut sup = HeartbeatSupervisor::new(60); // max 60s gap

    sup.register_tick(1_000);
    assert_eq!(sup.health(), EngineHealth::Healthy);

    // gap = 30s -> nog steeds ok
    sup.register_tick(1_030);
    assert_eq!(sup.health(), EngineHealth::Healthy);

    // gap = 59s -> nog steeds ok
    sup.register_tick(1_089);
    assert_eq!(sup.health(), EngineHealth::Healthy);
}

#[test]
fn heartbeat_supervisor_flags_degraded_on_large_gap() {
    let mut sup = HeartbeatSupervisor::new(60);

    sup.register_tick(1_000);
    assert_eq!(sup.health(), EngineHealth::Healthy);

    // gap = 120s -> moet Degraded worden
    sup.register_tick(1_120);
    assert_eq!(sup.health(), EngineHealth::Degraded);
}

#[test]
fn encode_supervisor_event_json_contains_core_fields() {
    let ev = HeartbeatSupervisorEvent {
        ts_utc: 1_234_567,
        status: EngineHealth::Degraded,
        msg: "heartbeat_gap_detected",
    };

    let s = encode_supervisor_event_json(&ev);

    assert!(
        s.contains("\"ts_utc\":1234567"),
        "expected ts_utc field in supervisor json, got: {}",
        s
    );
    assert!(
        s.contains("\"status\":\"Degraded\""),
        "expected status=Degraded in supervisor json, got: {}",
        s
    );
    assert!(
        s.contains("heartbeat_gap_detected"),
        "expected msg field in supervisor json, got: {}",
        s
    );
}

#[test]
fn test_degraded_blocks_new_long() {
    let mut ctx = make_minimal_ctx();
    ctx.engine_health = EngineHealth::Degraded;

    // geen open positie
    ctx.current_positions.insert(FutureInstrument::Mes, 0);

    let sleeve = MacroFuturesSleeve::new(MacroFuturesSleeveConfig::default());
    let risk_budget = minimal_risk_budget();

    let planned = sleeve.plan_contracts(&ctx, &risk_budget);
    assert!(planned.is_empty());

    let intents = sleeve.plan_order_intents(&ctx, &risk_budget);
    assert!(intents.is_empty());
}

#[test]
fn test_degraded_allows_flatten() {
    let mut ctx = make_minimal_ctx();
    ctx.engine_health = EngineHealth::Degraded;

    // open positie -> moet geflattend worden
    ctx.current_positions.insert(FutureInstrument::Mes, 2);

    let sleeve = MacroFuturesSleeve::new(MacroFuturesSleeveConfig::default());
    let risk_budget = minimal_risk_budget();

    let intents = sleeve.plan_order_intents(&ctx, &risk_budget);

    assert_eq!(intents.len(), 1);
    let oi = &intents[0];
    assert_eq!(oi.instrument, FutureInstrument::Mes);
    assert_eq!(oi.delta_contracts, -2); // full flatten
}
