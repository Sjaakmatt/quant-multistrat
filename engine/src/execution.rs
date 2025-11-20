use std::collections::HashMap;
use std::slice;
use std::path::{Path, PathBuf};
use std::fs::{OpenOptions, File};
use chrono::{Datelike};
use serde::Serialize;
use chrono::Utc;
use std::io::{self, Write};

use crate::risk::{
    GlobalRiskKernel,
    SleeveId,
    SleeveRiskEnvelope,
    SleeveState,
    PortfolioState,
    MarginState,
    VolatilityRegime,
};

use crate::strategies::macro_futures_sleeve::{
    EngineOrder,
    MacroFuturesSleeve,
    MacroFuturesHeartbeatOutput,
    FuturesSleeveContext,
    FuturesRiskBudget,
    FutureInstrument,
    InstrumentHistory,
    MacroScalars,
};

#[derive(Debug, Clone)]
pub struct MacroFuturesEngineHeartbeatResult {
    pub envelope: SleeveRiskEnvelope,
    pub heartbeat: MacroFuturesHeartbeatOutput,
    pub engine_orders: Vec<EngineOrder>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineHealth {
    Healthy,
    Degraded,     // behind on ticks or repeated failures
}

pub struct HeartbeatSupervisor {
    last_tick_ts: Option<i64>,     // UTC seconds
    max_gap_seconds: i64,          // e.g. 65 for once/minute heartbeats
    health: EngineHealth,
}


/// End-to-end heartbeat voor de Macro Futures sleeve:
/// GlobalRiskKernel → SleeveRiskEnvelope → MacroFuturesSleeve → EngineOrders → OrderSink.
pub fn run_macro_futures_engine_heartbeat(
    now_ts: i64,
    kernel: &mut GlobalRiskKernel,
    portfolio: &PortfolioState,
    sleeve_state: &mut SleeveState,
    margin: &MarginState,
    vol: &VolatilityRegime,
    sleeve: &MacroFuturesSleeve,
    histories: HashMap<FutureInstrument, InstrumentHistory>,
    macro_scalars: MacroScalars,
    current_positions: HashMap<FutureInstrument, i32>,
    eur_per_usd: f64,
    risk_budget: &FuturesRiskBudget,
    max_sleeve_risk_eur: f64,
    sink: &mut impl OrderSink,
) -> MacroFuturesEngineHeartbeatResult {
    // 1) Risk-kernel → envelope voor deze sleeve
    let sleeves_slice: &mut [SleeveState] = slice::from_mut(sleeve_state);

    let envelopes = kernel.evaluate(
        now_ts,
        portfolio,
        sleeves_slice,
        margin,
        vol,
    );

    let env = envelopes
        .into_iter()
        .find(|e| e.sleeve_id == SleeveId::MicroFuturesMacroTrend)
        .expect("Missing SleeveRiskEnvelope for MicroFuturesMacroTrend");


    let ctx = FuturesSleeveContext {
        as_of: macro_scalars.as_of,
        histories,
        macro_scalars,
        risk_envelope: env,
        current_positions,
        eur_per_usd,
        engine_health: EngineHealth::Healthy, // default
    };


    // 3) Sleeve-heartbeat (plan + intents)
    let hb = sleeve.run_heartbeat(&ctx, risk_budget, max_sleeve_risk_eur);

    // 4) Map naar EngineOrders en push naar sink
    let engine_orders =
        sleeve.map_heartbeat_to_engine_orders(SleeveId::MicroFuturesMacroTrend, &hb);

    for order in &engine_orders {
        sink.submit(order);
    }

    MacroFuturesEngineHeartbeatResult {
        envelope: env,
        heartbeat: hb,
        engine_orders,
    }
}


pub trait OrderSink {
    /// Submit één order naar de downstream executielaag.
    fn submit(&mut self, order: &EngineOrder);

    /// Optionele flush (default no-op).
    fn flush(&mut self) {}
}

#[derive(Debug, Default)]
pub struct InMemoryOrderSink {
    pub orders: Vec<EngineOrder>,
}

impl InMemoryOrderSink {
    pub fn new() -> Self {
        Self {
            orders: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct OrderLogEvent {
    /// Unix timestamp in UTC (seconden)
    pub ts_utc: i64,
    /// Sleeve-id als string (bijv. "MicroFuturesMacroTrend")
    pub sleeve_id: String,
    pub symbol: String,
    pub venue: String,
    /// "Buy" / "Sell"
    pub side: String,
    /// Aantal contracts (> 0)
    pub quantity: i32,
}

impl OrderLogEvent {
    pub fn from_engine_order(order: &EngineOrder, ts_utc: i64) -> Self {
        Self {
            ts_utc,
            sleeve_id: format!("{:?}", order.sleeve_id),
            symbol: order.symbol.to_string(),
            venue: order.venue.to_string(),
            side: format!("{:?}", order.side),
            quantity: order.quantity,
        }
    }
}

/// Convenience: direct JSON-string van één order.
pub fn encode_order_log_event_json(order: &EngineOrder, ts_utc: i64) -> String {
    let evt = OrderLogEvent::from_engine_order(order, ts_utc);
    serde_json::to_string(&evt).unwrap_or_else(|_| "{}".to_string())
}

#[derive(Debug, Clone, Serialize)]
pub struct HeartbeatLogEvent {
    pub ts_utc: i64,
    pub sleeve_id: String,
    pub portfolio_risk_state: String,
    pub engine_health: String,

    pub max_position_size_usd: f64,
    pub exposure_remaining_usd: f64,
    pub margin_remaining_usd: f64,

    pub total_risk_eur: f64,
    pub sanity: String,

    pub orders: Vec<OrderLogEvent>,
}


impl HeartbeatLogEvent {
    pub fn from_engine_result(
        ts_utc: i64,
        result: &MacroFuturesEngineHeartbeatResult,
        health: EngineHealth,
    ) -> Self {
        let sleeve_id = format!("{:?}", result.envelope.sleeve_id);
        let portfolio_risk_state = format!("{:?}", result.envelope.portfolio_risk_state);
        let engine_health = format!("{:?}", health);

        let max_position_size_usd = result.envelope.max_position_size_usd;
        let exposure_remaining_usd = result.envelope.exposure_remaining_usd;
        let margin_remaining_usd = result.envelope.margin_remaining_usd;

        let total_risk_eur = result.heartbeat.sleeve_plan.aggregate.total_risk_eur;
        let sanity = format!("{:?}", result.heartbeat.sleeve_plan.sanity);

        let orders: Vec<OrderLogEvent> = result
            .engine_orders
            .iter()
            .map(|o| OrderLogEvent::from_engine_order(o, ts_utc))
            .collect();

        Self {
            ts_utc,
            sleeve_id,
            portfolio_risk_state,
            engine_health,
            max_position_size_usd,
            exposure_remaining_usd,
            margin_remaining_usd,
            total_risk_eur,
            sanity,
            orders,
        }
    }
}


/// Convenience: JSON-string voor één heartbeat-event.
pub fn encode_heartbeat_log_event_json(
    ts_utc: i64,
    result: &MacroFuturesEngineHeartbeatResult,
    health: EngineHealth,
) -> String {
    let evt = HeartbeatLogEvent::from_engine_result(ts_utc, result, health);
    serde_json::to_string(&evt).unwrap_or_else(|_| "{}".to_string())
}


/// Variant van de heartbeat-orchestrator met directe heartbeat-logging.
///
/// - Roept `run_macro_futures_engine_heartbeat` aan met exact dezelfde args.
/// - Encodeert het resultaat als JSON.
/// - Stuurt één regel naar de aangeleverde `HeartbeatLogSink`.
pub fn run_macro_futures_engine_heartbeat_with_logging(
    now_ts: i64,
    supervisor: &mut HeartbeatSupervisor,
    kernel: &mut GlobalRiskKernel,
    portfolio: &PortfolioState,
    sleeve_state: &mut SleeveState,
    margin: &MarginState,
    vol: &VolatilityRegime,
    sleeve: &MacroFuturesSleeve,
    histories: HashMap<FutureInstrument, InstrumentHistory>,
    macro_scalars: MacroScalars,
    current_positions: HashMap<FutureInstrument, i32>,
    eur_per_usd: f64,
    risk_budget: &FuturesRiskBudget,
    max_sleeve_risk_eur: f64,
    sink: &mut impl OrderSink,
    heartbeat_log_sink: &mut impl HeartbeatLogSink,
) -> MacroFuturesEngineHeartbeatResult {
    // 0) Supervisor-update op basis van deze tick
    supervisor.register_tick(now_ts);

    if supervisor.health() == EngineHealth::Degraded {
        // Emergency event loggen vóór de normale heartbeat
        let sev = HeartbeatSupervisorEvent {
            ts_utc: now_ts,
            status: supervisor.health(),
            msg: "heartbeat_gap_detected",
        };
        let sev_json = encode_supervisor_event_json(&sev);
        heartbeat_log_sink.log(&sev_json);
        // hier expliciet flushen is optioneel; ik laat het aan de caller/batching
    }

    // 1) Run de normale engine-heartbeat
    let result = run_macro_futures_engine_heartbeat(
        now_ts,
        kernel,
        portfolio,
        sleeve_state,
        margin,
        vol,
        sleeve,
        histories,
        macro_scalars,
        current_positions,
        eur_per_usd,
        risk_budget,
        max_sleeve_risk_eur,
        sink,
    );

    // 2) Encodeer als JSON en log één regel (normale heartbeat)
    let json_line = encode_heartbeat_log_event_json(now_ts, &result, supervisor.health());
    heartbeat_log_sink.log(&json_line);

    result
}



/// Sink-interface voor heartbeat-logs (JSON-per-regel).
pub trait HeartbeatLogSink {
    /// Log één heartbeat-event als JSON-regel.
    fn log(&mut self, line: &str);

    /// Optionele flush (default no-op).
    fn flush(&mut self) {}
}

/// Logger die heartbeat-JSON als één regel naar stdout schrijft.
///
/// In productie gebruik je `StdoutHeartbeatLogger::new()`.
/// In tests kun je `with_writer(...)` gebruiken met een in-memory buffer.
#[derive(Debug)]
pub struct StdoutHeartbeatLogger<W: Write = io::Stdout> {
    writer: W,
}

impl StdoutHeartbeatLogger {
    /// Productie-constructie: schrijft naar process-stdout.
    pub fn new() -> Self {
        Self {
            writer: io::stdout(),
        }
    }
}

/// Batching sink: buffert N heartbeat JSON-lines en schrijft
/// ze pas door naar een onderliggende HeartbeatLogSink bij flush().
pub struct BatchingHeartbeatLogger {
    inner: Box<dyn HeartbeatLogSink>,
    buffer: Vec<String>,
    capacity: usize,
}

impl BatchingHeartbeatLogger {
    /// Maak een batching logger met vaste capaciteit.
    pub fn new(inner: Box<dyn HeartbeatLogSink>, capacity: usize) -> Self {
        assert!(capacity > 0, "BatchingHeartbeatLogger: capacity must be > 0");
        Self {
            inner,
            buffer: Vec::with_capacity(capacity),
            capacity,
        }
    }

    pub fn buffered_len(&self) -> usize {
        self.buffer.len()
    }

    /// Interne helper — forceer directe flush naar inner.
    fn flush_inner(&mut self) {
        for line in self.buffer.drain(..) {
            self.inner.log(&line);
        }
        self.inner.flush();
    }

    pub fn into_inner(self) -> Box<dyn HeartbeatLogSink> {
        self.inner
    }
}

impl HeartbeatLogSink for BatchingHeartbeatLogger {
    fn log(&mut self, line: &str) {
        self.buffer.push(line.to_string());
        if self.buffer.len() >= self.capacity {
            self.flush_inner();
        }
    }

    fn flush(&mut self) {
        if !self.buffer.is_empty() {
            self.flush_inner();
        }
    }
}


impl<W: Write> StdoutHeartbeatLogger<W> {
    /// Custom writer, handig voor tests of alternatieve sinks.
    pub fn with_writer(writer: W) -> Self {
        Self { writer }
    }

    /// Haal de onderliggende writer eruit (alleen echt nodig in tests).
    pub fn into_inner(self) -> W {
        self.writer
    }
}

impl<W: Write> HeartbeatLogSink for StdoutHeartbeatLogger<W> {
    fn log(&mut self, line: &str) {
        if let Err(e) = writeln!(self.writer, "{}", line) {
            // Logging mag nooit de engine doen crashen; slechts assertion in debug.
            debug_assert!(
                false,
                "StdoutHeartbeatLogger: failed to write heartbeat line: {:?}",
                e
            );
        }
    }

    fn flush(&mut self) {
        if let Err(e) = self.writer.flush() {
            debug_assert!(
                false,
                "StdoutHeartbeatLogger: failed to flush heartbeat writer: {:?}",
                e
            );
        }
    }
}


#[derive(Debug)]
pub struct FileOrderSink {
    path: PathBuf,
}

impl FileOrderSink {
    pub fn new<P: Into<PathBuf>>(path: P) -> Self {
        Self {
            path: path.into(),
        }
    }
}

impl OrderSink for FileOrderSink {
    fn submit(&mut self, order: &EngineOrder) {
        let ts = Utc::now().timestamp();
        let line = encode_order_log_event_json(order, ts);

        let file_result = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path);

        match file_result {
            Ok(mut file) => {
                if let Err(e) = writeln!(file, "{}", line) {
                    debug_assert!(
                        false,
                        "FileOrderSink: failed to write to log file: {:?}",
                        e
                    );
                }
            }
            Err(e) => {
                debug_assert!(
                    false,
                    "FileOrderSink: failed to open log file {:?}: {:?}",
                    self.path,
                    e
                );
            }
        }
    }
}


impl OrderSink for InMemoryOrderSink {
    fn submit(&mut self, order: &EngineOrder) {
        self.orders.push(order.clone());
    }
}

pub struct FileHeartbeatLogger {
    log_dir: PathBuf,
    current_date: Option<(i32, u32, u32)>,
    file: Option<File>,
}

impl FileHeartbeatLogger {
    pub fn new<P: AsRef<Path>>(log_dir: P) -> Self {
        Self {
            log_dir: log_dir.as_ref().to_path_buf(),
            current_date: None,
            file: None,
        }
    }

    fn get_file_for_date(&mut self, year: i32, month: u32, day: u32) -> &mut File {
        let date_tuple = (year, month, day);

        let needs_new_file = match self.current_date {
            None => true,
            Some(prev) => prev != date_tuple,
        };

        if needs_new_file {
            self.current_date = Some(date_tuple);

            let fname = format!("heartbeat-{:04}{:02}{:02}.jsonl", year, month, day);
            let fpath = self.log_dir.join(fname);

            let f = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&fpath)
                .expect("FileHeartbeatLogger: cannot open log file");

            self.file = Some(f);
        }

        self.file.as_mut().unwrap()
    }
}

impl HeartbeatLogSink for FileHeartbeatLogger {
    fn log(&mut self, line: &str) {
        let now = chrono::Utc::now();
        let y = now.year();
        let m = now.month();
        let d = now.day();

        let file = self.get_file_for_date(y, m, d);

        let _ = writeln!(file, "{}", line);
    }

    fn flush(&mut self) {
        if let Some(f) = &mut self.file {
            let _ = f.flush();
        }
    }
}

impl FileHeartbeatLogger {
    /// Test-helper: log using a forced timestamp instead of Utc::now().
    pub fn log_with_datetime(&mut self, dt: chrono::DateTime<chrono::Utc>, line: &str) {
        let y = dt.year();
        let m = dt.month();
        let d = dt.day();

        let file = self.get_file_for_date(y, m, d);

        if let Err(e) = writeln!(file, "{}", line) {
            debug_assert!(false, "FileHeartbeatLogger: write_with_datetime failed {:?}", e);
        }
    }
}

impl HeartbeatSupervisor {
    pub fn register_tick(&mut self, ts_utc: i64) {
        match self.last_tick_ts {
            None => {
                // eerste tick ooit
                self.last_tick_ts = Some(ts_utc);
                self.health = EngineHealth::Healthy;
            }
            Some(prev) => {
                let gap = ts_utc - prev;
                if gap > self.max_gap_seconds {
                    self.health = EngineHealth::Degraded;
                } else {
                    self.health = EngineHealth::Healthy;
                }
                self.last_tick_ts = Some(ts_utc);
            }
        }
    }

    pub fn new(max_gap_seconds: i64) -> Self {
        Self {
            last_tick_ts: None,
            max_gap_seconds,
            health: EngineHealth::Healthy,
        }
    }

    pub fn health(&self) -> EngineHealth {
        self.health
    }
}

#[derive(Debug)]
pub struct HeartbeatSupervisorEvent {
    pub ts_utc: i64,
    pub status: EngineHealth,
    pub msg: &'static str,
}

pub fn encode_supervisor_event_json(ev: &HeartbeatSupervisorEvent) -> String {
    format!(
        "{{\"ts_utc\":{},\"status\":\"{:?}\",\"msg\":\"{}\"}}",
        ev.ts_utc,
        ev.status,
        ev.msg
    )
}
