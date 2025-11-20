use std::collections::HashMap;
use chrono::{DateTime, Utc};

use crate::risk::{SleeveRiskEnvelope, HaltState, SleeveId};
use crate::execution::EngineHealth;

// bv: use crate::risk::risk_kernel::SleeveRiskEnvelope;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FutureInstrument {
    Mes,   // Micro E-mini S&P 500
    Mnq,   // Micro E-mini Nasdaq 100
    SixE,  // 6E (Euro FX future)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SleeveRiskSanity {
    Ok,
    ExceedsCap,
}

#[derive(Debug, Clone, Copy)]
pub struct FxCarryFeatures {
    /// rate_EUR - rate_USD (in procentpunten)
    pub carry_rate_annualized: f64,
    /// stdev van daily (rate_EUR - rate_USD) over ~252d
    pub carry_rate_vol_252d: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct DailyFeatureBar {
    pub ts: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,

    pub atr_14: f64,
    pub ret_20d: f64,
    pub ret_60d: f64,
    pub ret_120d: f64,

    pub vol_20d: f64,      // stdev log-returns 20d
    pub vol_60d: f64,      // stdev log-returns 60d
    pub vol_120d: f64,     // stdev log-returns 120d

    pub highest_close_50d: f64,
    pub lowest_close_50d: f64,

    /// Alleen Some voor 6E, None voor MES/MNQ
    pub fx_carry: Option<FxCarryFeatures>,
}

#[derive(Debug, Clone, Copy)]
pub struct FuturesSleeveAggregate {
    pub total_contracts_signed: i32,
    pub total_contracts_abs: i32,
    pub total_risk_eur: f64,
    pub total_notional_usd: f64,
    pub instrument_count: usize,
}

#[derive(Debug, Clone)]
pub struct FuturesSleevePlan {
    pub planned_contracts: Vec<FuturesPlannedContracts>,
    pub risk_report: Vec<FuturesPlannedRisk>,
    pub aggregate: FuturesSleeveAggregate,
    pub sanity: SleeveRiskSanity,
}


#[derive(Debug, Clone)]
pub struct InstrumentHistory {
    pub instrument: FutureInstrument,
    /// Oplopende tijd; laatste element = meest recente bar
    pub bars: Vec<DailyFeatureBar>,
}

#[derive(Debug, Clone, Copy)]
pub struct MacroScalars {
    pub as_of: DateTime<Utc>,
    pub risk_on_scalar: f64, // 0.7 .. 1.3
    pub usd_scalar: f64,     // 0.7 .. 1.3
}

#[derive(Debug, Clone, Copy)]
pub struct InstrumentRiskBudget {
    pub max_risk_per_position_eur: f64,
    pub max_contracts: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct FuturesRiskBudget {
    pub mes: InstrumentRiskBudget,   // v1: 90 EUR, 3 contracts
    pub mnq: InstrumentRiskBudget,   // v1: 90 EUR, 3 contracts
    pub sixe: InstrumentRiskBudget,  // v1: 60 EUR, 3 contracts
    pub max_total_contracts: u32,    // v1: 3 contracts totaal
}


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalReason {
    Normal,
    InsufficientHistory,
    InvalidData,
    BelowThreshold,
}

#[derive(Debug, Clone, Copy)]
pub struct RawSignal {
    pub trend_score: f64,  // -3 .. +3
    pub carry_score: f64,  // -2 .. +2 (6E), 0 anders
}

#[derive(Debug, Clone, Copy)]
pub struct MacroAdjustedSignal {
    pub trend_macro_adjusted: f64,
    pub carry_macro_adjusted: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct FinalTradeSignal {
    pub direction: i8,        // -1, 0, +1
    pub conviction: f64,      // 0.0 .. 1.0
    pub effective_score: f64, // na macro+carry, geclamped [-5, 5]
}

#[derive(Debug, Clone)]
pub struct InstrumentSignal {
    pub instrument: FutureInstrument,
    pub final_signal: FinalTradeSignal,
    pub raw: RawSignal,
    pub macro_adj: MacroAdjustedSignal,
    pub reason: SignalReason,
}

#[derive(Debug, Clone, Copy)]
pub struct FuturesPlannedPosition {
    /// Welk instrument (MES / MNQ / 6E)
    pub instrument: FutureInstrument,
    /// Gewenste richting -1 / 0 / +1
    pub target_direction: i8,
    /// Gewenste USD-notional (signed: + = long, - = short)
    pub target_notional_usd: f64,
}


#[derive(Debug, Clone)]
pub struct FuturesSleeveContext {
    pub as_of: DateTime<Utc>,
    pub histories: HashMap<FutureInstrument, InstrumentHistory>,
    pub macro_scalars: MacroScalars,
    pub risk_envelope: SleeveRiskEnvelope,
    pub current_positions: HashMap<FutureInstrument, i32>, // signed contracts

    /// EUR per 1 USD (account in EUR, futures in USD)
    /// Voor MES/MNQ/6E is contract_notional_usd in USD;
    /// risk in EUR = contract_notional_usd * eur_per_usd.
    pub eur_per_usd: f64,
    pub engine_health: EngineHealth,
}

#[derive(Debug, Clone)]
pub struct MacroFuturesHeartbeatOutput {
    pub sleeve_plan: FuturesSleevePlan,
    pub order_intents: Vec<FuturesOrderIntent>,
}


#[derive(Debug, Clone)]
pub struct InstrumentRiskIntent {
    /// Welk instrument
    pub instrument: FutureInstrument,
    /// -1.0 .. +1.0: fractie van het instrument-budget die we willen gebruiken.
    /// direction * conviction
    pub desired_risk_frac: f64,
    /// Volledige signal stack voor logging / beslissingen hogerop.
    pub signal: InstrumentSignal,
}

#[derive(Debug, Clone, Copy)]
pub struct FuturesPlannedContracts {
    pub instrument: FutureInstrument,
    /// Signed target: +3 = long 3 contracts, -2 = short 2 contracts
    pub target_contracts: i32,
}

#[derive(Debug, Clone, Copy)]
pub struct FuturesPlannedRisk {
    pub instrument: FutureInstrument,
    /// Signed target contracts (identiek aan FuturesPlannedContracts)
    pub target_contracts: i32,
    /// Risk per contract in EUR (altijd positief)
    pub risk_per_contract_eur: f64,
    /// Totaal risico in EUR voor deze positie (altijd positief)
    pub total_risk_eur: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct FuturesOrderIntent {
    pub instrument: FutureInstrument,
    /// Signed delta: +3 = koop 3 contracts, -2 = verkoop 2 contracts
    pub delta_contracts: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineOrderSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineOrder {
    pub sleeve_id: SleeveId,
    pub instrument: FutureInstrument,
    pub symbol: &'static str,
    pub venue: &'static str,
    pub side: EngineOrderSide,
    /// Absolute aantal contracts (altijd > 0)
    pub quantity: i32,
}


#[derive(Debug, Clone)]
pub struct MacroFuturesSleeveConfig {
    // Trend scoring
    pub trend_weight_20d: f64,   // 0.45
    pub trend_weight_60d: f64,   // 0.30
    pub trend_weight_120d: f64,  // 0.15
    pub breakout_weight: f64,    // 0.10
    pub trend_score_clip: f64,   // 3.0

    // Carry
    pub carry_score_clip: f64,   // 2.0
    pub carry_vol_floor: f64,    // 0.25
    pub carry_weight_6e: f64,    // 0.5

    // Effective score
    pub effective_score_clip: f64, // 5.0

    // Logistic mapping
    pub logistic_k: f64,         // 1.1
    pub logistic_m: f64,         // 1.6

    // Flat thresholds
    pub min_effective_score: f64, // 1.2
    pub min_conviction: f64,      // 0.35

    // ATR-gebaseerde stop-risk per contract
    pub atr_stop_multiple_index: f64, // bijv. 0.25 * ATR voor index futures
    pub atr_stop_multiple_fx: f64,    // bijv. 0.5 * ATR voor 6E
}


impl Default for MacroFuturesSleeveConfig {
    fn default() -> Self {
        Self {
            trend_weight_20d: 0.45,
            trend_weight_60d: 0.30,
            trend_weight_120d: 0.15,
            breakout_weight: 0.10,
            trend_score_clip: 3.0,
            carry_score_clip: 2.0,
            carry_vol_floor: 0.25,
            carry_weight_6e: 0.5,
            effective_score_clip: 5.0,

            // 🔧 AANPASSINGEN HIER:
            logistic_k: 1.3,        // steilere curve
            logistic_m: 1.3,        // iets naar links → sneller hoge conviction

            min_effective_score: 1.0, // sneller “trade ok”
            min_conviction: 0.30, 

            // V1 calibratie:
            // - index: 0.25 * ATR * multiplier → relatief conservatief
            // - FX:   0.5  * ATR * 125k
            atr_stop_multiple_index: 0.25,
            atr_stop_multiple_fx: 0.5,
        }
    }
}


#[derive(Debug, Clone)]
pub struct MacroFuturesSleeve {
    pub cfg: MacroFuturesSleeveConfig,
}

impl MacroFuturesSleeve {
    pub fn new(cfg: MacroFuturesSleeveConfig) -> Self {
        Self { cfg }
    }

    pub fn evaluate_signals(
        &self,
        ctx: &FuturesSleeveContext,
        _risk_budget: &FuturesRiskBudget,
    ) -> Vec<InstrumentSignal> {
        let mut out = Vec::new();

        for (inst, hist) in &ctx.histories {
            let signal = self.evaluate_instrument(*inst, hist, &ctx.macro_scalars);
            out.push(signal);
        }

        out
    }

        /// Hoog-niveau API voor de risk-kernel:
    /// - draait de volledige signal pipeline
    /// - vertaalt naar een gewenste risk-fractie per instrument (-1.0 .. +1.0)
    ///
    /// Contract-sizes / euro-risk worden later door de risk-kernel bepaald.
    pub fn evaluate_risk_intents(
        &self,
        ctx: &FuturesSleeveContext,
        risk_budget: &FuturesRiskBudget,
    ) -> Vec<InstrumentRiskIntent> {
        // 1) Eerst de ruwe signals berekenen
        let signals = self.evaluate_signals(ctx, risk_budget);

        // 2) Map elke signal naar een risk-intent
        signals
            .into_iter()
            .map(|signal| {
                let dir = signal.final_signal.direction as f64;
                let conv = signal.final_signal.conviction;

                // direction ∈ {-1,0,1}, conviction ∈ [0,1]
                // → desired_risk_frac ∈ [-1,1]
                let mut desired_risk_frac = dir * conv;

                // defensief clampen, mocht er ooit iets geks gebeuren
                if !desired_risk_frac.is_finite() {
                    desired_risk_frac = 0.0;
                } else {
                    desired_risk_frac = desired_risk_frac.clamp(-1.0, 1.0);
                }

                InstrumentRiskIntent {
                    instrument: signal.instrument,
                    desired_risk_frac,
                    signal,
                }
            })
            .collect()
    }

        pub fn plan_positions(
        &self,
        ctx: &FuturesSleeveContext,
        risk_budget: &FuturesRiskBudget,
    ) -> Vec<FuturesPlannedPosition> {
        let env = &ctx.risk_envelope;

        // 1) Als risk-kernel zegt "stop", max size = 0 ...
        if matches!(env.portfolio_halt, HaltState::Halt | HaltState::Kill)
            || matches!(env.sleeve_halt, HaltState::Halt | HaltState::Kill)
            || env.max_position_size_usd <= 0.0
            || env.max_concurrent_positions == 0
        {
            return Vec::new();
        }

        // 1b) EngineHealth degraded → geen nieuwe posities (alleen flatten via order_intents)
        if let EngineHealth::Degraded = ctx.engine_health {
            return Vec::new();
        }


        // 2) Headroom in USD voor deze sleeve (exposure + margin)
        let mut exposure_remaining = env.exposure_remaining_usd.max(0.0);
        let mut margin_remaining = env.margin_remaining_usd.max(0.0);

        // 3) Eerst de intents ophalen (direction * conviction per instrument)
        let intents = self.evaluate_risk_intents(ctx, risk_budget);

        // 4) Concurrency: hoeveel instrumenten hebben NU een niet-0 positie?
        let current_open = ctx
            .current_positions
            .values()
            .filter(|&&v| v != 0)
            .count() as u32;

        let max_slots = env.max_concurrent_positions;
        let mut used_slots = current_open.min(max_slots);

        // 5) Map intents → geplande USD-notional per instrument,
        //    met headroom (exposure/margin) + concurrency-limiet
        intents
            .into_iter()
            .filter_map(move |intent| {
                let dir = intent.signal.final_signal.direction;
                let conv = intent.signal.final_signal.conviction;
                let frac = intent.desired_risk_frac; // direction * conviction

                // Flat of geen conviction? Dan plannen we niks.
                if dir == 0 || conv <= 0.0 || frac == 0.0 {
                    return None;
                }

                // Geen headroom meer? Dan plannen we niets meer in deze sleeve.
                if exposure_remaining <= 0.0 || margin_remaining <= 0.0 {
                    return None;
                }

                // Bepaal of dit een nieuw instrument is (nu flat, straks non-zero)
                let current_pos = ctx
                    .current_positions
                    .get(&intent.instrument)
                    .copied()
                    .unwrap_or(0);

                let is_new_instrument = current_pos == 0;

                // Concurrency-cap: geen nieuwe instrument-slots als we al vol zitten
                if is_new_instrument && used_slots >= max_slots {
                    return None;
                }

                let base = env.max_position_size_usd;

                // desired_risk_frac ∈ [-1,1] → scale van 0 tot base
                let mut target_notional = frac * base;
                if !target_notional.is_finite() {
                    return None;
                }

                let mut abs_target = target_notional.abs();

                // mini-filter: < $1 exposure = negeren
                if abs_target < 1.0 {
                    return None;
                }

                // Headroom-cap in USD (exposure + margin)
                let allowed_notional = exposure_remaining.min(margin_remaining);
                if allowed_notional <= 0.0 {
                    return None;
                }

                // Indien nodig terugschalen tot binnen headroom
                if abs_target > allowed_notional {
                    let scale = allowed_notional / abs_target;
                    if !scale.is_finite() || scale <= 0.0 {
                        return None;
                    }

                    target_notional *= scale;
                    abs_target = target_notional.abs();

                    // Na scaling kan het < $1 zijn → dan alsnog skippen
                    if abs_target < 1.0 {
                        return None;
                    }
                }

                // Headroom verbruiken (USD-notional ~ exposure & margin)
                exposure_remaining = (exposure_remaining - abs_target).max(0.0);
                margin_remaining = (margin_remaining - abs_target).max(0.0);

                // Als we effectief een nieuwe positie openen op een instrument
                // dat eerder flat was, telt dat als extra concurrency-slot
                if is_new_instrument {
                    used_slots = used_slots.saturating_add(1);
                }

                Some(FuturesPlannedPosition {
                    instrument: intent.instrument,
                    target_direction: dir,
                    target_notional_usd: target_notional,
                })
            })
            .collect()
    }



        /// Interne helper: berekent zowel target contracts als risk in EUR per instrument.
        /// Interne helper: berekent zowel target contracts als risk in EUR per instrument.
    ///
    /// Nieuwe V1-logica:
    /// - `plan_positions` gebruikt de risk-envelope voor:
    ///   - halts
    ///   - concurrency
    ///   - headroom
    /// - Hier vertalen we:
    ///   - conviction → gewenste risk-fractie per instrument
    ///   - ATR-stop → per-contract risk in USD/EUR
    ///   - profielen + envelope → harde caps in contracts
    fn plan_contracts_with_risk_internal(
        &self,
        ctx: &FuturesSleeveContext,
        risk_budget: &FuturesRiskBudget,
    ) -> Vec<(FuturesPlannedContracts, FuturesPlannedRisk)> {
        // 1) Eerst de USD-notional plannen (om de relatieve intensiteit te lezen)
        let planned = self.plan_positions(ctx, risk_budget);

        let mut out: Vec<(FuturesPlannedContracts, FuturesPlannedRisk)> = Vec::new();

        // Globale contract-cap op sleeve-niveau
        let mut remaining_total: i32 = risk_budget.max_total_contracts as i32;

        // Base-USD die de risk-kernel ons geeft
        let base = ctx.risk_envelope.max_position_size_usd;
        if !base.is_finite() || base <= 0.0 {
            return Vec::new();
        }

        for p in planned {
            if remaining_total <= 0 {
                break;
            }

            // Per-instrument budget
            let inst_budget = match p.instrument {
                FutureInstrument::Mes => risk_budget.mes,
                FutureInstrument::Mnq => risk_budget.mnq,
                FutureInstrument::SixE => risk_budget.sixe,
            };

            let inst_max_contracts: i32 = inst_budget.max_contracts as i32;
            if inst_max_contracts <= 0 {
                continue;
            }

            // |frac| = |target_notional| / base ∈ (0,1]
            let abs_frac = (p.target_notional_usd.abs() / base).clamp(0.0, 1.0);
            if abs_frac <= 0.0 {
                continue;
            }

            // direction is i8 → cast expliciet naar i32
            let sign_i32: i32 = p.target_direction as i32;
            if sign_i32 == 0 {
                continue;
            }

            // Ruwe contracts o.b.v. frac van inst_max_contracts
            let mut abs_contracts: i32 =
                (inst_max_contracts as f64 * abs_frac).round() as i32;

            // Zorg dat een niet-triviale frac altijd minstens 1 contract geeft
            if abs_contracts <= 0 {
                abs_contracts = 1;
            }

            // Caps toepassen: per instrument + globale max_total_contracts
            abs_contracts = abs_contracts
                .min(inst_max_contracts)
                .min(remaining_total.max(0));

            if abs_contracts <= 0 {
                continue;
            }

            let final_target: i32 = sign_i32 * abs_contracts;

            // Risk-per-contract in EUR:
            // bij inst_max_contracts vol → max_risk_per_position_eur
            // dus per contract = max_risk / inst_max_contracts
            let risk_per_contract_eur = if inst_max_contracts > 0
                && inst_budget.max_risk_per_position_eur.is_finite()
            {
                inst_budget.max_risk_per_position_eur / inst_max_contracts as f64
            } else {
                0.0
            };

            if risk_per_contract_eur <= 0.0 {
                continue;
            }

            let total_risk_eur = risk_per_contract_eur * (abs_contracts as f64);

            let planned_contracts = FuturesPlannedContracts {
                instrument: p.instrument,
                target_contracts: final_target,
            };

            let planned_risk = FuturesPlannedRisk {
                instrument: p.instrument,
                target_contracts: final_target,
                risk_per_contract_eur,
                total_risk_eur,
            };

            out.push((planned_contracts, planned_risk));

            remaining_total -= abs_contracts;
        }

        out
    }

    /// Bestaande API: alleen target contracts per instrument.
    pub fn plan_contracts(
        &self,
        ctx: &FuturesSleeveContext,
        risk_budget: &FuturesRiskBudget,
    ) -> Vec<FuturesPlannedContracts> {
        self.plan_contracts_with_risk_internal(ctx, risk_budget)
            .into_iter()
            .map(|(contracts, _risk)| contracts)
            .collect()
    }

    /// Nieuwe API: risk-report per instrument (geschikt voor logging / UI).
    pub fn plan_risk_report(
        &self,
        ctx: &FuturesSleeveContext,
        risk_budget: &FuturesRiskBudget,
    ) -> Vec<FuturesPlannedRisk> {
        self.plan_contracts_with_risk_internal(ctx, risk_budget)
            .into_iter()
            .map(|(_contracts, risk)| risk)
            .collect()
    }

    pub fn aggregate_sleeve_risk(
        &self,
        ctx: &FuturesSleeveContext,
        risk_budget: &FuturesRiskBudget,
    ) -> FuturesSleeveAggregate {
        let report = self.plan_risk_report(ctx, risk_budget);

        let mut total_signed = 0i32;
        let mut total_abs = 0i32;
        let mut total_risk_eur = 0.0f64;
        let mut total_notional_usd = 0.0f64;

        let mut instrument_count = 0usize;

        for r in report {
            if r.target_contracts == 0 {
                continue;
            }

            total_signed += r.target_contracts;
            total_abs += r.target_contracts.abs();
            total_risk_eur += r.total_risk_eur;

            // V1: reconstrueer USD-risk uit EUR-risk (niet notional).
            let notional_usd = r.total_risk_eur / ctx.eur_per_usd;
            total_notional_usd += notional_usd;

            instrument_count += 1;
        }

        FuturesSleeveAggregate {
            total_contracts_signed: total_signed,
            total_contracts_abs: total_abs,
            total_risk_eur,
            total_notional_usd,
            instrument_count,
        }
    }


    pub fn check_sleeve_risk_sanity(
        &self,
        ctx: &FuturesSleeveContext,
        risk_budget: &FuturesRiskBudget,
        max_sleeve_risk_eur: f64,
    ) -> SleeveRiskSanity {
        // Geen zinnige cap → beschouw als "geen limiet"
        if !max_sleeve_risk_eur.is_finite() || max_sleeve_risk_eur <= 0.0 {
            return SleeveRiskSanity::Ok;
        }

        let agg = self.aggregate_sleeve_risk(ctx, risk_budget);

        if agg.total_risk_eur > max_sleeve_risk_eur {
            SleeveRiskSanity::ExceedsCap
        } else {
            SleeveRiskSanity::Ok
        }
    }

        /// High-level helper: één call die alles voor de sleeve plant + sanity checkt.
    ///
    /// - gebruikt de bestaande pipelines:
    ///   - plan_contracts
    ///   - plan_risk_report
    ///   - aggregate_sleeve_risk
    ///   - check_sleeve_risk_sanity
    ///
    /// - wijzigt GEEN eerder gedrag; dit is puur een convenience layer.
    pub fn plan_sleeve(
        &self,
        ctx: &FuturesSleeveContext,
        risk_budget: &FuturesRiskBudget,
        max_sleeve_risk_eur: f64,
    ) -> FuturesSleevePlan {
        let planned_contracts = self.plan_contracts(ctx, risk_budget);
        let risk_report = self.plan_risk_report(ctx, risk_budget);
        let aggregate = self.aggregate_sleeve_risk(ctx, risk_budget);
        let sanity = self.check_sleeve_risk_sanity(ctx, risk_budget, max_sleeve_risk_eur);

        FuturesSleevePlan {
            planned_contracts,
            risk_report,
            aggregate,
            sanity,
        }
    }

        /// Convenience heartbeat voor deze sleeve:
    /// - bouwt een volledige sleeve-plan (contracts + risk + aggregate + sanity)
    /// - bouwt de bijbehorende order-intents
    ///
    /// Gedrag van bestaande API's blijft ongewijzigd; dit is alleen een bundeling.
    pub fn run_heartbeat(
        &self,
        ctx: &FuturesSleeveContext,
        risk_budget: &FuturesRiskBudget,
        max_sleeve_risk_eur: f64,
    ) -> MacroFuturesHeartbeatOutput {
        let sleeve_plan = self.plan_sleeve(ctx, risk_budget, max_sleeve_risk_eur);
        let order_intents = self.plan_order_intents(ctx, risk_budget);

        MacroFuturesHeartbeatOutput {
            sleeve_plan,
            order_intents,
        }
    }

        /// Map een heartbeat-output naar generieke EngineOrders
    /// voor downstream execution/routing.
    pub fn map_heartbeat_to_engine_orders(
        &self,
        sleeve_id: SleeveId,
        hb: &MacroFuturesHeartbeatOutput,
    ) -> Vec<EngineOrder> {
        hb.order_intents
            .iter()
            .filter_map(|oi| {
                let delta = oi.delta_contracts;
                if delta == 0 {
                    return None;
                }

                let side = if delta > 0 {
                    EngineOrderSide::Buy
                } else {
                    EngineOrderSide::Sell
                };

                let quantity = delta.abs();
                if quantity <= 0 {
                    return None;
                }

                let (symbol, venue) = instrument_metadata(oi.instrument);

                Some(EngineOrder {
                    sleeve_id,
                    instrument: oi.instrument,
                    symbol,
                    venue,
                    side,
                    quantity,
                })
            })
            .collect()
    }


    pub fn plan_order_intents(
        &self,
        ctx: &FuturesSleeveContext,
        risk_budget: &FuturesRiskBudget,
    ) -> Vec<FuturesOrderIntent> {
        // 1) Bepaal de gewenste target contracts per instrument
        let planned_contracts = self.plan_contracts(ctx, risk_budget);

        // 2) Maak het makkelijk om target per instrument op te zoeken
        //    (max 3 instrumenten, dus gewoon een kleine Vec scan is prima)
        let mut out = Vec::new();

        // 2a) Eerst: instrumenten waarvoor we een target hebben
        for p in &planned_contracts {
            let current = ctx
                .current_positions
                .get(&p.instrument)
                .copied()
                .unwrap_or(0);

            let delta = p.target_contracts - current;

            if delta != 0 {
                out.push(FuturesOrderIntent {
                    instrument: p.instrument,
                    delta_contracts: delta,
                });
            }
        }

        // 2b) Daarna: instrumenten die nu een positie hebben,
        //     maar géén target meer (die moeten flat → volledig sluiten)
        for (&inst, &current) in &ctx.current_positions {
            if current == 0 {
                continue;
            }

            let has_target = planned_contracts
                .iter()
                .any(|p| p.instrument == inst);

            if !has_target {
                // Geen target meer, maar wel current → sluit alles
                out.push(FuturesOrderIntent {
                    instrument: inst,
                    delta_contracts: -current,
                });
            }
        }

        out
    }

    fn apply_macro(
        &self,
        inst: FutureInstrument,
        raw: &RawSignal,
        macros: &MacroScalars,
    ) -> MacroAdjustedSignal {
        // Trend-scalar per instrument
        let trend_scalar = match inst {
            FutureInstrument::Mes | FutureInstrument::Mnq => {
                // Equity indices → vooral risk-on sentiment
                macros.risk_on_scalar
            }
            FutureInstrument::SixE => {
                // FX future → combinatie van risk-on & USD-thema
                macros.risk_on_scalar * macros.usd_scalar
            }
        };

        // Carry-scalar per instrument
        let carry_scalar = match inst {
            FutureInstrument::SixE => {
                // 6E carry wordt deels gewogen en afhankelijk van USD-thema
                self.cfg.carry_weight_6e * macros.usd_scalar
            }
            _ => 0.0, // MES/MNQ hebben geen carry-component
        };

        let trend_macro_adjusted = raw.trend_score * trend_scalar;
        let carry_macro_adjusted = raw.carry_score * carry_scalar;

        MacroAdjustedSignal {
            trend_macro_adjusted,
            carry_macro_adjusted,
        }
    }

    fn compute_effective_score(
        &self,
        inst: FutureInstrument,
        macro_adj: &MacroAdjustedSignal,
    ) -> f64 {
        // Basis: macro-adjusted trend
        let mut eff = macro_adj.trend_macro_adjusted;

        // 6E krijgt bovenop trend ook carry mee
        if let FutureInstrument::SixE = inst {
            eff += macro_adj.carry_macro_adjusted;
        }

        // Hard clamp op globale bandbreedte
        let clip = self.cfg.effective_score_clip.abs();
        eff.clamp(-clip, clip)
    }

    fn evaluate_instrument(
        &self,
        inst: FutureInstrument,
        hist: &InstrumentHistory,
        macros: &MacroScalars,
    ) -> InstrumentSignal {

        // 1) History length check
        if let Err(reason) = self.validate_history(hist) {
            return self.flat_signal(inst, reason);
        }

        // 2) Pak laatste bar en valideer features
        let last_bar = match hist.bars.last() {
            Some(b) => b,
            None => return self.flat_signal(inst, SignalReason::InsufficientHistory),
        };

        if let Err(reason) = self.validate_features(last_bar) {
            return self.flat_signal(inst, reason);
        }

        // 3) Compute raw trend score
        let trend_score = self.compute_trend_raw(&hist.bars, inst);

        // 4) Compute raw carry score (alleen 6E, anders 0.0)
        let carry_score = self.compute_carry_raw(inst, last_bar);

        let raw = RawSignal {
            trend_score,
            carry_score,
        };

        // 5) Macro-adjust (risk-on + USD, instrument-specifiek)
        let macro_adj = self.apply_macro(inst, &raw, macros);

        // 6) Combineer naar één effectieve score
        let effective_score = self.compute_effective_score(inst, &macro_adj);

        // 7) Map effectieve score naar conviction [0,1]
        let conviction = self.compute_conviction(effective_score);

        // 8) Bouw de definitieve tradesignal + reason o.b.v. thresholds
        let (final_signal, reason) = self.build_final_signal(effective_score, conviction);

        InstrumentSignal {
            instrument: inst,
            final_signal,
            raw,
            macro_adj,
            reason,
        }

    }


    fn compute_conviction(&self, effective_score: f64) -> f64 {
        if !effective_score.is_finite() {
            debug_assert!(false, "non-finite effective_score in compute_conviction");
            return 0.0;
        }

        let x = effective_score.abs();
        let k = self.cfg.logistic_k;
        let m = self.cfg.logistic_m;

        // z = k * (x - m)
        let z = k * (x - m);

        // standaard logistische functie: 1 / (1 + e^{-z})
        let c = 1.0 / (1.0 + (-z).exp());

        // theoretisch al in (0,1), maar we clampen defensief
        c.clamp(0.0, 1.0)
    }


    fn flat_signal(&self, inst: FutureInstrument, reason: SignalReason) -> InstrumentSignal {
        InstrumentSignal {
            instrument: inst,
            final_signal: FinalTradeSignal {
                direction: 0,
                conviction: 0.0,
                effective_score: 0.0,
            },
            raw: RawSignal {
                trend_score: 0.0,
                carry_score: 0.0,
            },
            macro_adj: MacroAdjustedSignal {
                trend_macro_adjusted: 0.0,
                carry_macro_adjusted: 0.0,
            },
            reason,
        }
    }


    fn validate_history(&self, hist: &InstrumentHistory) -> Result<(), SignalReason> {
        const MIN_BARS: usize = 120;

        if hist.bars.len() < MIN_BARS {
            return Err(SignalReason::InsufficientHistory);
        }

        Ok(())
    }


    fn validate_features(&self, bar: &DailyFeatureBar) -> Result<(), SignalReason> {
        fn pos(x: f64) -> bool {
            x.is_finite() && x > 0.0
        }

        fn finite(x: f64) -> bool {
            x.is_finite()
        }

        // Prijzen
        if !pos(bar.open) || !pos(bar.high) || !pos(bar.low) || !pos(bar.close) {
            return Err(SignalReason::InvalidData);
        }

        // Volume mag 0 zijn, maar niet negatief of NaN/inf
        if !finite(bar.volume) || bar.volume < 0.0 {
            return Err(SignalReason::InvalidData);
        }

        // ATR & volatilities moeten > 0 en finite zijn
        if !pos(bar.atr_14)
            || !pos(bar.vol_20d)
            || !pos(bar.vol_60d)
            || !pos(bar.vol_120d)
        {
            return Err(SignalReason::InvalidData);
        }

        // Returns mogen negatief zijn, maar niet NaN/inf
        if !finite(bar.ret_20d) || !finite(bar.ret_60d) || !finite(bar.ret_120d) {
            return Err(SignalReason::InvalidData);
        }

        // Breakout-basis moet zinvol zijn
        if !pos(bar.highest_close_50d) || !pos(bar.lowest_close_50d) {
            return Err(SignalReason::InvalidData);
        }

        // FX carry (alleen als aanwezig)
        if let Some(fx) = bar.fx_carry {
            if !finite(fx.carry_rate_annualized)
                || !finite(fx.carry_rate_vol_252d)
                || fx.carry_rate_vol_252d <= 0.0
            {
                return Err(SignalReason::InvalidData);
            }
        }

        Ok(())
    }


    fn compute_carry_raw(
        &self,
        inst: FutureInstrument,
        last: &DailyFeatureBar,
    ) -> f64 {
        match inst {
            FutureInstrument::SixE => {
                let fx = match last.fx_carry {
                    Some(fx) => fx,
                    // Geen carry-features beschikbaar → conservatief 0.0
                    None => return 0.0,
                };

                let carry_rate = fx.carry_rate_annualized;
                let carry_vol = fx.carry_rate_vol_252d;

                debug_assert!(carry_rate.is_finite());
                debug_assert!(carry_vol.is_finite() && carry_vol > 0.0);

                let vol_floor = self.cfg.carry_vol_floor.max(f64::EPSILON);
                let denom = carry_vol.max(vol_floor);

                let z = carry_rate / denom;

                let clip = self.cfg.carry_score_clip.abs(); // defensief
                z.clamp(-clip, clip)
            }
            // MES / MNQ (en evt. andere) → geen carry-premie in deze sleeve
            _ => 0.0,
        }
    }


    fn compute_trend_raw(
        &self,
        bars: &[DailyFeatureBar],
        _inst: FutureInstrument,
    ) -> f64 {
        let last = match bars.last() {
            Some(b) => b,
            None => return 0.0, // zou niet mogen gebeuren door validate_history, maar fail-safe
        };

        let z20 = last.ret_20d / last.vol_20d;
        let z60 = last.ret_60d / last.vol_60d;
        let z120 = last.ret_120d / last.vol_120d;

        let brk = if last.close > last.highest_close_50d {
            1.0
        } else if last.close < last.lowest_close_50d {
            -1.0
        } else {
            0.0
        };

        let raw =
            self.cfg.trend_weight_20d * z20 +
            self.cfg.trend_weight_60d * z60 +
            self.cfg.trend_weight_120d * z120 +
            self.cfg.breakout_weight * brk;

        raw.clamp(-self.cfg.trend_score_clip, self.cfg.trend_score_clip)
    }


    fn build_final_signal(
        &self,
        effective_score: f64,
        conviction: f64,
    ) -> (FinalTradeSignal, SignalReason) {
        // Defensief: zorg dat we nooit non-finite in de output hebben
        if !effective_score.is_finite() || !conviction.is_finite() {
            debug_assert!(false, "non-finite inputs in build_final_signal");
            let flat = FinalTradeSignal {
                direction: 0,
                conviction: 0.0,
                effective_score: 0.0,
            };
            return (flat, SignalReason::InvalidData);
        }

        let abs_eff = effective_score.abs();
        let eff_threshold = self.cfg.min_effective_score;
        let conv_threshold = self.cfg.min_conviction;

        // Beslis of we überhaupt mogen handelen
        let below_effective = abs_eff < eff_threshold;
        let below_conviction = conviction < conv_threshold;

        if below_effective || below_conviction {
            // We houden de informatie (effective_score, conviction),
            // maar direction blijft 0 en reason verklaart waarom.
            let flat = FinalTradeSignal {
                direction: 0,
                conviction,
                effective_score,
            };
            return (flat, SignalReason::BelowThreshold);
        }

        // We hebben voldoende edge én conviction → kies richting
        let direction = if effective_score > 0.0 {
            1
        } else {
            -1
        };

        let final_signal = FinalTradeSignal {
            direction,
            conviction,
            effective_score,
        };

        (final_signal, SignalReason::Normal)
    }

}

fn instrument_metadata(inst: FutureInstrument) -> (&'static str, &'static str) {
    match inst {
        FutureInstrument::Mes => ("MES", "CME"),
        FutureInstrument::Mnq => ("MNQ", "CME"),
        FutureInstrument::SixE => ("6E", "CME"),
    }
}


pub fn demo_macro_futures_sleeve() {
    use chrono::Duration;
    use crate::risk::{SleeveId, PortfolioRiskState};

    // 1) Config + sleeve
    let cfg = MacroFuturesSleeveConfig::default();
    let sleeve = MacroFuturesSleeve::new(cfg);

    let now = Utc::now();

    // 2) Kleine helper om dummy-historie te maken
    fn make_history(inst: FutureInstrument, base_price: f64, now: DateTime<Utc>) -> InstrumentHistory {
        let mut bars = Vec::new();

        // 130 dagen dummy data (genoeg voor onze MIN_BARS = 120)
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
                volume: 1000.0,

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

        InstrumentHistory {
            instrument: inst,
            bars,
        }
    }

    // 3) Dummy histories voor alle drie de instrumenten
    let mes_hist = make_history(FutureInstrument::Mes, 5000.0, now);
    let mnq_hist = make_history(FutureInstrument::Mnq, 16000.0, now);
    let sixe_hist = make_history(FutureInstrument::SixE, 1.10, now);

    let mut histories = HashMap::new();
    histories.insert(FutureInstrument::Mes, mes_hist);
    histories.insert(FutureInstrument::Mnq, mnq_hist);
    histories.insert(FutureInstrument::SixE, sixe_hist);

    // 4) Macro-scalar dummy (neutraal regime)
    let macro_scalars = MacroScalars {
        as_of: now,
        risk_on_scalar: 1.0,
        usd_scalar: 1.0,
    };

    // 5) Dummy risk-envelope alsof de risk-kernel dit heeft berekend
    let risk_envelope = SleeveRiskEnvelope {
        sleeve_id: SleeveId::MicroFuturesMacroTrend,
        sleeve_halt: HaltState::None,
        portfolio_halt: HaltState::None,

        max_position_size_usd: 2_000.0,
        max_concurrent_positions: 3,

        exposure_remaining_usd: 10_000.0,
        margin_remaining_usd: 10_000.0,

        volatility_regime_scalar: 1.0,
        leverage_scalar: 1.0,

        portfolio_risk_state: PortfolioRiskState::Normal,
    };

    // 6) Geen open posities in deze demo
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


    // 7) Dummy risk-budget voor de sleeve (nog niet echt gebruikt in de pipeline)
    let risk_budget = FuturesRiskBudget {
        // Micro E-mini S&P 500
        mes: InstrumentRiskBudget {
            max_risk_per_position_eur: 120.0, // cap ≈ 120 EUR per MES-trade
            max_contracts: 5,                 // genoeg ruimte zodat risk-cap, niet contracts-cap, bindt
        },
        // Micro E-mini Nasdaq 100
        mnq: InstrumentRiskBudget {
            max_risk_per_position_eur: 120.0, // idem voor MNQ
            max_contracts: 5,
        },
        // 6E is veel groter qua notional → iets lager cap
        sixe: InstrumentRiskBudget {
            max_risk_per_position_eur: 80.0,  // conservatiever vanwege grote contract-size
            max_contracts: 3,
        },
        // Sleeve-breed: max aantal contracts
        max_total_contracts: 4, // bijv. max 4 contracts totaal
    };


    // 8) Volledige pipeline → order-intents
    let order_intents = sleeve.plan_order_intents(&ctx, &risk_budget);

    println!("=== Macro Futures Sleeve Demo ===");
    for oi in order_intents {
        println!(
            "Instrument: {:?}, delta_contracts: {}",
            oi.instrument, oi.delta_contracts
        );
    }
}
