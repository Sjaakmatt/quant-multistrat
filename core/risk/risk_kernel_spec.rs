// risk_kernel_spec.rs

// ====== Enums & basic types ======

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
    pub equity_usd: f64,          // huidige value van de sleeve
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
    pub peak_equity_usd: f64,      // high-water mark
    pub total_notional_exposure: f64, // vol-genormaliseerd exposure
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

// ====== Kernel interface ======

pub struct GlobalRiskKernelConfig {
    pub portfolio: PortfolioRiskConfig,
    pub sleeves: Vec<SleeveRiskConfig>,
}

pub struct GlobalRiskKernel {
    pub config: GlobalRiskKernelConfig,
    // interne state kun je later uitbreiden (rolling vols, covars, logging, etc.)
}

impl GlobalRiskKernel {
    /// Hoofdfunctie: wordt aangeroepen op elke risk-heartbeat.
    pub fn evaluate(
        &mut self,
        now_ts: i64,
        portfolio: &PortfolioState,
        sleeves: &[SleeveState],
        margin: &MarginState,
        vol: &VolatilityRegime,
    ) -> Vec<SleeveRiskEnvelope> {
        // TODO: implementatie in volgende stappen
        unimplemented!()
    }
}
