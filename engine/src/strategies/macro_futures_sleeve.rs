use std::collections::HashMap;
use chrono::{DateTime, Utc};

// Pas deze import aan naar jouw echte risk-kernel pad:
#[derive(Debug, Clone, Copy)]
pub struct SleeveRiskEnvelope;

// bv: use crate::risk::risk_kernel::SleeveRiskEnvelope;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FutureInstrument {
    Mes,   // Micro E-mini S&P 500
    Mnq,   // Micro E-mini Nasdaq 100
    SixE,  // 6E (Euro FX future)
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
    pub mes: InstrumentRiskBudget,
    pub mnq: InstrumentRiskBudget,
    pub sixe: InstrumentRiskBudget,
    pub max_total_contracts: u32,
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

#[derive(Debug, Clone)]
pub struct FuturesSleeveContext {
    pub as_of: DateTime<Utc>,
    pub histories: HashMap<FutureInstrument, InstrumentHistory>,
    pub macro_scalars: MacroScalars,
    pub risk_envelope: SleeveRiskEnvelope,
    pub current_positions: HashMap<FutureInstrument, i32>, // signed contracts
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
            logistic_k: 1.1,
            logistic_m: 1.6,
            min_effective_score: 1.2,
            min_conviction: 0.35,
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
