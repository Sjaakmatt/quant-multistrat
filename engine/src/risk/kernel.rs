// risk_kernel.rs

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SleeveId {
    EquityLongShort,
    StatArbResidual,
    MicrostructureIntraday,
    OptionsVolPremium,
    MicroFuturesMacroTrend,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortfolioRiskState {
    Normal,
    Caution,
    Stress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HaltState {
    None,
    Halt, // geen nieuwe trades, bestaande mogen volgens rules uitlopen
    Kill, // alles liquideren, geen nieuwe trades
}

// ====== Config structs (hard limits) ======

#[derive(Debug, Clone, Copy)]
pub struct SleeveRiskConfig {
    pub sleeve_id: SleeveId,
    pub capital_alloc_usd: f64,        // bij start: 2000, 2500, etc.
    pub max_single_pos_risk_frac: f64, // bijv. 0.01 = 1% van sleeve
    pub halt_dd_frac: f64,             // bijv. -0.10
    pub kill_dd_frac: f64,             // bijv. -0.15
    pub max_concurrent_positions: u32, // bij options/futures = spreads/contracts
}

#[derive(Debug, Clone, Copy)]
pub struct PortfolioRiskConfig {
    pub initial_equity_usd: f64,   // 10_000
    pub halt_dd_frac: f64,         // -0.08
    pub kill_dd_frac: f64,         // -0.12
    pub max_leverage: f64,         // 1.5
    pub rebalance_drift_frac: f64, // 0.15 (±15% threshold)
    pub max_global_positions: u32, // 15
}

// ====== State snapshots ======

#[derive(Debug, Clone, Copy)]
pub struct SleeveState {
    pub sleeve_id: SleeveId,
    pub equity_usd: f64,          // huidige waarde van de sleeve
    pub realized_pnl_usd: f64,    // sinds inception / reset
    pub unrealized_pnl_usd: f64,  // open PnL
    pub peak_equity_usd: f64,     // high-water mark voor DD
    pub open_positions: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct PortfolioState {
    pub cash_usd: f64,
    pub open_pnl_usd: f64,
    pub accrued_interest_usd: f64,
    pub peak_equity_usd: f64,         // mag als afgeleide/legacy blijven bestaan
    pub total_notional_exposure: f64, // vol-genormaliseerde exposure
    pub current_leverage: f64,        // exposure / equity
}

#[derive(Debug, Clone, Copy)]
pub struct MarginState {
    pub internal_margin_req_usd: f64, // eigen model
    pub broker_margin_req_usd: f64,   // IBKR real-time (indien beschikbaar)
    pub equity_usd: f64,              // redundante check
}

#[derive(Debug, Clone, Copy)]
pub struct VolatilityRegime {
    pub rv10_annualized: f64, // realized vol
    pub vix_level: f64,
    pub vix_term_slope: f64,  // term-structure info
    pub regime_scalar: f64,   // 0.5 - 1.5 (hybrid logic)
}

// ====== Kernel output per sleeve ======

#[derive(Debug, Clone, Copy)]
pub struct SleeveRiskEnvelope {
    pub sleeve_id: SleeveId,

    // Hard limits
    pub sleeve_halt: HaltState,
    pub portfolio_halt: HaltState,

    // Position & concurrency
    pub max_position_size_usd: f64,
    pub max_concurrent_positions: u32,

    // Remaining capacity
    pub exposure_remaining_usd: f64,
    pub margin_remaining_usd: f64,

    // Adaptive scalars
    pub volatility_regime_scalar: f64, // 0.5 - 1.5
    pub leverage_scalar: f64,          // e.g. 0.7 - 1.0 - 1.2

    // Global portfolio state
    pub portfolio_risk_state: PortfolioRiskState,
}

// risk decision layer
#[derive(Debug, Clone, Copy)]
pub enum RiskDecisionReason {
    Ok,
    PortfolioHalt,
    SleeveHalt,
    NoMarginHeadroom,
    NoExposureHeadroom,
    ConcurrencyLimit,
    PositionSizeZero,
}

#[derive(Debug, Clone, Copy)]
pub struct RiskDecision {
    pub allow_new_position: bool,
    pub max_new_positions: u32,
    pub max_order_notional_usd: f64,
    pub reason: RiskDecisionReason,
}


// ====== Kernel config & struct ======

pub struct GlobalRiskKernelConfig {
    pub portfolio: PortfolioRiskConfig,
    pub sleeves: Vec<SleeveRiskConfig>,
}

pub struct GlobalRiskKernel {
    pub config: GlobalRiskKernelConfig,

    // interne HWM voor portfolio DD (closed-end, met 20% cashflow-reset-regel)
    pub internal_portfolio_peak_equity: f64,
    
}

fn derive_volatility_scalar(vol: &VolatilityRegime) -> f64 {
    let rv = vol.rv10_annualized;
    let vix = vol.vix_level;
    let slope = vol.vix_term_slope;

    // 1) STRESS regime
    if vix >= 35.0 || rv >= 30.0 || slope < 0.0 {
        return 0.55_f64.max(0.5).min(1.3);
    }

    // 2) ELEVATED regime
    if vix >= 25.0 || rv >= 20.0 {
        return 0.80_f64.max(0.5).min(1.3);
    }

    // 3) LOW VOL regime
    if vix < 15.0 && rv < 12.0 && slope > 0.5 {
        return 1.25_f64.max(0.5).min(1.3);
    }

    // 4) NORMAL regime
    1.0
}

fn derive_leverage_scalar(portfolio: &PortfolioState, pcfg: &PortfolioRiskConfig) -> f64 {
    let max_lev = pcfg.max_leverage.max(0.1); // defensief

    let cur_lev = portfolio.current_leverage.max(0.0);
    let x = cur_lev / max_lev; // relatieve leverage: 0.0 = flat, 1.0 = op max

    if x >= 1.0 {
        // boven of op max: geen extra risk meer
        return 0.0;
    }

    // Piecewise profiel:
    // - x in [0.0, 0.3]: lichte boost tot ~1.1
    // - x in (0.3, 0.7]: rond 1.0
    // - x in (0.7, 1.0): lineair omlaag naar ~0.3

    let scalar = if x <= 0.3 {
        // 0 → 1.10, 0.3 → ~1.03
        1.10 - 0.25 * x
    } else if x <= 0.7 {
        // 0.3 → ~1.03, 0.7 → ~0.95
        1.03 - 0.20 * (x - 0.3)
    } else {
        // 0.7 → ~0.95, 1.0 → ~0.30
        0.95 - 2.17 * (x - 0.7) // 0.95 - 2.17*0.3 ≈ 0.30
    };

    scalar.clamp(0.0, 1.10)
}

pub fn evaluate_new_position_risk(
    sleeve_state: &SleeveState,
    env: &SleeveRiskEnvelope,
) -> RiskDecision {
    // 1) Hard halts (portfolio of sleeve)
    if matches!(env.portfolio_halt, HaltState::Halt | HaltState::Kill) {
        return RiskDecision {
            allow_new_position: false,
            max_new_positions: 0,
            max_order_notional_usd: 0.0,
            reason: RiskDecisionReason::PortfolioHalt,
        };
    }

    if matches!(env.sleeve_halt, HaltState::Halt | HaltState::Kill) {
        return RiskDecision {
            allow_new_position: false,
            max_new_positions: 0,
            max_order_notional_usd: 0.0,
            reason: RiskDecisionReason::SleeveHalt,
        };
    }

    // 2) Headroom checks
    if env.margin_remaining_usd <= 0.0 {
        return RiskDecision {
            allow_new_position: false,
            max_new_positions: 0,
            max_order_notional_usd: 0.0,
            reason: RiskDecisionReason::NoMarginHeadroom,
        };
    }

    if env.exposure_remaining_usd <= 0.0 {
        return RiskDecision {
            allow_new_position: false,
            max_new_positions: 0,
            max_order_notional_usd: 0.0,
            reason: RiskDecisionReason::NoExposureHeadroom,
        };
    }

    if env.max_position_size_usd <= 0.0 {
        return RiskDecision {
            allow_new_position: false,
            max_new_positions: 0,
            max_order_notional_usd: 0.0,
            reason: RiskDecisionReason::PositionSizeZero,
        };
    }

    // 3) Concurrency limit voor deze sleeve
    let open = sleeve_state.open_positions;
    if open >= env.max_concurrent_positions {
        return RiskDecision {
            allow_new_position: false,
            max_new_positions: 0,
            max_order_notional_usd: 0.0,
            reason: RiskDecisionReason::ConcurrencyLimit,
        };
    }

    let max_new_positions = env.max_concurrent_positions - open;

    RiskDecision {
        allow_new_position: true,
        max_new_positions,
        max_order_notional_usd: env.max_position_size_usd,
        reason: RiskDecisionReason::Ok,
    }
}


impl GlobalRiskKernel {
    pub fn new(config: GlobalRiskKernelConfig) -> Self {
        Self {
            internal_portfolio_peak_equity: config.portfolio.initial_equity_usd,
            config,
        }
    }

    pub fn config(&self) -> &GlobalRiskKernelConfig {
        &self.config
    }

    /// Hoofdfunctie: wordt aangeroepen op elke risk-heartbeat.
    pub fn evaluate(
        &mut self,
        _now_ts: i64,
        portfolio: &PortfolioState,
        sleeves: &mut [SleeveState],
        margin: &MarginState,
        vol: &VolatilityRegime,
    ) -> Vec<SleeveRiskEnvelope> {
        let pcfg = &self.config.portfolio;

        // ===== 1) Portfolio equity & DD =====
        let equity_now =
            portfolio.cash_usd + portfolio.open_pnl_usd + portfolio.accrued_interest_usd;

        // interne HWM-update
        if equity_now > self.internal_portfolio_peak_equity {
            self.internal_portfolio_peak_equity = equity_now;
        }

        let dd_frac = if self.internal_portfolio_peak_equity > 0.0 {
            (equity_now / self.internal_portfolio_peak_equity) - 1.0
        } else {
            0.0
        };

        let portfolio_halt_state = if dd_frac <= pcfg.kill_dd_frac {
            HaltState::Kill
        } else if dd_frac <= pcfg.halt_dd_frac {
            HaltState::Halt
        } else {
            HaltState::None
        };

        let portfolio_risk_state = if dd_frac <= pcfg.kill_dd_frac {
            PortfolioRiskState::Stress
        } else if dd_frac <= pcfg.halt_dd_frac {
            PortfolioRiskState::Caution
        } else {
            PortfolioRiskState::Normal
        };

        // ===== 2) Exposure & margin headroom =====

        // max toelaatbare (vol-genormaliseerde) exposure o.b.v. leverage
        let max_exposure_allowed = pcfg.max_leverage * equity_now;
        let exposure_remaining_usd =
            (max_exposure_allowed - portfolio.total_notional_exposure).max(0.0);

        // conservatief: broker-req override internal model
        let binding_margin_req = margin
            .internal_margin_req_usd
            .max(margin.broker_margin_req_usd);
        let margin_remaining_usd = (equity_now - binding_margin_req).max(0.0);

        // ===== 3) Volatility- & leverage-scalar =====
        let volatility_regime_scalar = derive_volatility_scalar(vol);
        let leverage_scalar = derive_leverage_scalar(portfolio, pcfg);

        // ===== 4) Global concurrency headroom =====
        let total_open_positions: u32 = sleeves.iter().map(|s| s.open_positions).sum();
        let max_global = pcfg.max_global_positions;

        let remaining_slots = if total_open_positions >= max_global {
            0
        } else {
            max_global - total_open_positions
        };

        let active_sleeves = sleeves.len() as u32;
        let extra_per_sleeve: u32 = if remaining_slots > 0 && active_sleeves > 0 {
            remaining_slots / active_sleeves // floor, conservatief
        } else {
            0
        };

        // ===== 5) Per-sleeve DD, concurrency & sizing =====
        let mut envelopes = Vec::with_capacity(sleeves.len());

        for sleeve in sleeves.iter_mut() {
            let scfg = self
                .config
                .sleeves
                .iter()
                .find(|c| c.sleeve_id == sleeve.sleeve_id)
                .expect("missing sleeve config");

            let equity = sleeve.equity_usd;

            // per-sleeve HWM update
            if equity > sleeve.peak_equity_usd {
                sleeve.peak_equity_usd = equity;
            }

            let dd_frac_sleeve = if sleeve.peak_equity_usd > 0.0 {
                (equity / sleeve.peak_equity_usd) - 1.0
            } else {
                0.0
            };

            let sleeve_halt_state = if dd_frac_sleeve <= scfg.kill_dd_frac {
                HaltState::Kill
            } else if dd_frac_sleeve <= scfg.halt_dd_frac {
                HaltState::Halt
            } else {
                HaltState::None
            };

            // ----- Dynamische concurrency cap -----
            let mut dyn_max_concurrent = scfg.max_concurrent_positions;

            if remaining_slots == 0 {
                // geen globale ruimte meer: lock per sleeve op huidige open positions
                dyn_max_concurrent = sleeve.open_positions;
            } else {
                // ieder krijgt een stukje van de resterende slots
                let target_cap = sleeve.open_positions + extra_per_sleeve;
                dyn_max_concurrent = dyn_max_concurrent.min(target_cap);
            }

            // ----- Position size logica (vol/leverage + headroom) -----
            let base_pos_usd = scfg.capital_alloc_usd * scfg.max_single_pos_risk_frac;

            let mut max_position_size_usd =
                base_pos_usd * volatility_regime_scalar * leverage_scalar;

            if margin_remaining_usd <= 0.0 || exposure_remaining_usd <= 0.0 {
                max_position_size_usd = 0.0;
            } else {
                max_position_size_usd = max_position_size_usd.min(exposure_remaining_usd);
            }

            if matches!(portfolio_halt_state, HaltState::Halt | HaltState::Kill)
                || matches!(sleeve_halt_state, HaltState::Halt | HaltState::Kill)
            {
                max_position_size_usd = 0.0;
            }

            let env = SleeveRiskEnvelope {
                sleeve_id: sleeve.sleeve_id,
                sleeve_halt: sleeve_halt_state,
                portfolio_halt: portfolio_halt_state,

                max_position_size_usd,
                max_concurrent_positions: dyn_max_concurrent,

                exposure_remaining_usd,
                margin_remaining_usd,

                volatility_regime_scalar,
                leverage_scalar,

                portfolio_risk_state,
            };

            envelopes.push(env);
        }

        envelopes
    }

    /// Optioneel: cashflow-reset helper (20% regel)
    pub fn apply_cashflow_reset(&mut self, equity_before: f64, equity_after: f64) {
        if equity_before <= 0.0 {
            self.internal_portfolio_peak_equity = equity_after;
            return;
        }

        let net_cf = equity_after - equity_before;
        let cf_frac = (net_cf.abs()) / equity_before;

        if cf_frac >= 0.20 {
            // reset HWM naar nieuwe equity
            self.internal_portfolio_peak_equity = equity_after;
            // TODO: per-sleeve peaks hier later netjes rescalen/resetten
        }
    }
}
