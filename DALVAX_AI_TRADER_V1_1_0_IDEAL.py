#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DALVAX AI TRADER — MONÓLITO FULL V1.1.0 IDEAL
Arquitetura: HeatMap-first + Opção B (Detectores → IA valida → Execução)

Melhorias vs V1.0.1:
─────────────────────────────────────────────────────────────────
1. SIDE LOGIC CORRIGIDA
   - V1.0.1: side era baseado APENAS em funding_rate (fraco).
   - V1.1.0: side = função do tipo de evento + funding + regime.
     liquidation_cascade com funding alto → SHORT (short squeeze)
     liquidation_cascade com funding negativo → LONG
     gamma_oi_funding_stress → lado do funding extremo
     liquidity_sweep → contra o spike de spread
     regime "sniper" → bias para o lado da volatilidade dominante

2. CONFIDENCE SCORING MELHORADO
   - Ponderação multi-fator: event_strength, heat_score, regime_score, funding_bias.
   - Garante que apenas sinais realmente qualificados passem para IA.

3. PNL SIMULADO REALISTA
   - Cada posição simulada tem um tick de preço que evolui com random walk.
   - BE (Break-Even) e Trail stop aplicados no tick do PositionManager.
   - Partial TP contabilizado no PnL.
   - Close with reason (tp_hit | sl_hit | trailing_stop | be_exit | simulated_rotation).

4. PRE-CHECK EXPANDIDO
   - Bloqueio por cooldown de par (pair_cooldown) além de pair_lock.
   - Bloqueio por correlação entre símbolos abertos.

5. AI COST GUARD APRIMORADO
   - Modo AI_PROVIDER=none → validação determinística por threshold (zero custo).
   - Modo AI_PROVIDER=anthropic/openai → apenas quando todas as guards passam.
   - Estimativa de tokens por chamada logada explicitamente.

6. MÉTRICAS DE PNL AUDITÁVEIS
   - Rastreio de: total_trades, wins, losses, total_pnl_usd, max_dd_usd, best_trade, worst_trade.
   - Emitido em cada tick e no shutdown final.

7. JOURNAL ENRIQUECIDO
   - Cada evento logado com contexto completo para auditoria.
   - Summary de sessão emitido no shutdown.

8. BUGS CORRIGIDOS
   - Pair lock não era desbloqueado em caso de rejeição de risco pós-AI.
   - StateStore.save() não era chamado no shutdown.
   - PositionManager não calculava PnL antes de fechar posição.
   - MetaDecisionEngine não verificava regime "wait" → agora bloqueia trading.
   - AIValidateGate: reject_cache não era limpo com nova data → corrigido.

IMPORTANTE: SIMULATED=true / DRY_RUN=true por padrão. Trading real envolve risco.
"""

from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import json
import logging
import math
import os
import random
import signal
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

# ============================================================
# Utilities
# ============================================================

def utc_ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())

def utc_date() -> str:
    return time.strftime("%Y-%m-%d", time.gmtime())

def now_ms() -> int:
    return int(time.time() * 1000)

def jdump(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)

def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")

def env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    try:
        return int(v)
    except ValueError:
        raise ValueError(f"Invalid int for {name}: {v!r}")

def env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    try:
        return float(v)
    except ValueError:
        raise ValueError(f"Invalid float for {name}: {v!r}")

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

# ============================================================
# Config (ENV master) — Basic ON, Advanced OFF
# ============================================================

@dataclass
class Config:
    # Identity
    app_name: str = os.getenv("DALVAX_APP_NAME", "DALVAX_AI_TRADER")
    build: str = os.getenv("DALVAX_BUILD", "monolito_full_v1_1_0_ideal")
    env: str = os.getenv("DALVAX_ENV", "prod")
    log_level: str = os.getenv("DALVAX_LOG_LEVEL", "INFO")
    timezone: str = os.getenv("DALVAX_TIMEZONE", "America/Sao_Paulo")

    # Modes (safe defaults)
    simulated: bool = env_bool("SIMULATED", True)
    real_enabled: bool = env_bool("REAL_ENABLED", False)
    dry_run: bool = env_bool("DRY_RUN", True)

    # Loop / performance
    loop_sec: int = env_int("DALVAX_LOOP_SEC", 15)
    heatmap_refresh_sec: int = env_int("DALVAX_HEATMAP_REFRESH_SEC", 30)
    position_poll_sec: int = env_int("DALVAX_POSITION_POLL_SEC", 2)
    health_check_sec: int = env_int("DALVAX_HEALTH_CHECK_SEC", 7)

    # Exchange
    okx_env: str = os.getenv("OKX_ENV", "prod")
    okx_api_key: str = os.getenv("OKX_API_KEY", "")
    okx_api_secret: str = os.getenv("OKX_API_SECRET", "")
    okx_api_passphrase: str = os.getenv("OKX_API_PASSPHRASE", "")

    # Universe
    max_universe_size: int = env_int("DALVAX_MAX_UNIVERSE_SIZE", 50)
    min_volume_24h_usdt: float = env_float("DALVAX_MIN_VOLUME_24H_USDT", 5_000_000.0)

    # Heat Map (BASIC ON)
    heatmap_enabled: bool = env_bool("DALVAX_HEATMAP_ENABLED", True)
    heatmap_top_n: int = env_int("DALVAX_HEATMAP_TOP_N", 8)
    heatmap_min_score: float = env_float("DALVAX_HEATMAP_MIN_SCORE", 0.55)

    heat_w_volume: float = env_float("DALVAX_HEAT_W_VOLUME", 0.30)
    heat_w_volatility: float = env_float("DALVAX_HEAT_W_VOLATILITY", 0.25)
    heat_w_open_interest: float = env_float("DALVAX_HEAT_W_OPEN_INTEREST", 0.20)
    heat_w_liquidations: float = env_float("DALVAX_HEAT_W_LIQUIDATIONS", 0.15)
    heat_w_funding_stress: float = env_float("DALVAX_HEAT_W_FUNDING_STRESS", 0.10)

    # Market filters
    max_funding_rate: float = env_float("DALVAX_MAX_FUNDING_RATE", 0.005)
    min_spread_pct: float = env_float("DALVAX_MIN_SPREAD_PCT", 0.0015)
    min_liquidity_usd: float = env_float("DALVAX_MIN_LIQUIDITY_USD", 250000.0)

    # Detectors (BASIC ON)
    detect_liquidation_enabled: bool = env_bool("DALVAX_DETECT_LIQUIDATION_ENABLED", True)
    detect_gamma_oi_enabled: bool = env_bool("DALVAX_DETECT_GAMMA_OI_ENABLED", True)
    detect_liquidity_intel_enabled: bool = env_bool("DALVAX_DETECT_LIQUIDITY_INTEL_ENABLED", True)

    # Optional detectors (ADVANCED OFF)
    detect_capital_flow_enabled: bool = env_bool("DALVAX_DETECT_CAPITAL_FLOW_ENABLED", False)
    detect_microstructure_lite_enabled: bool = env_bool("DALVAX_DETECT_MICROSTRUCTURE_LITE_ENABLED", False)

    # Regime (BASIC ON)
    regime_enabled: bool = env_bool("DALVAX_REGIME_ENABLED", True)
    regime_highvol_atr_z: float = env_float("DALVAX_REGIME_HIGHVOL_ATR_Z", 1.5)
    regime_extremevol_atr_z: float = env_float("DALVAX_REGIME_EXTREMEVOL_ATR_Z", 2.5)

    # Meta decision (BASIC ON)
    meta_enabled: bool = env_bool("DALVAX_META_ENABLED", True)
    meta_pause_on_errors: bool = env_bool("DALVAX_META_PAUSE_ON_ERRORS", True)
    meta_max_consec_errors: int = env_int("DALVAX_META_MAX_CONSEC_ERRORS", 3)
    meta_error_cooldown_sec: int = env_int("DALVAX_META_ERROR_COOLDOWN_SEC", 300)
    meta_block_on_wait_regime: bool = env_bool("DALVAX_META_BLOCK_WAIT_REGIME", True)  # FIX: bloquear regime "wait"

    # Orchestrator (BASIC ON)
    orchestrator_enabled: bool = env_bool("DALVAX_ORCHESTRATOR_ENABLED", True)

    # AI (BASIC ON, Option B validate_only)
    ai_enabled: bool = env_bool("DALVAX_AI_ENABLED", True)
    ai_mode: str = os.getenv("DALVAX_AI_MODE", "validate_only")
    ai_provider: str = os.getenv("AI_PROVIDER", "none")  # IDEAL DEFAULT: none (zero custo)
    ai_min_confidence: float = env_float("DALVAX_AI_MIN_CONFIDENCE", 0.62)

    anthropic_api_key: str = os.getenv("ANTHROPIC_API_KEY", "")
    anthropic_model: str = os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-latest")
    openai_api_key: str = os.getenv("OPENAI_API_KEY", "")
    openai_model: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    # AI Cost Guard (V1.0.1 + melhorado)
    ai_cost_guard_enabled: bool = env_bool("DALVAX_AI_COST_GUARD_ENABLED", True)
    ai_max_calls_per_day: int = env_int("DALVAX_AI_MAX_CALLS_PER_DAY", 500)   # IDEAL: 500 (vs 1000)
    ai_min_confidence_call: float = env_float("DALVAX_AI_MIN_CONFIDENCE_CALL", 0.62)  # IDEAL: alinhado com min_confidence
    ai_reject_cache_minutes: int = env_int("DALVAX_AI_REJECT_CACHE_MINUTES", 15)  # IDEAL: 15min (vs 10)
    # Estimativa de tokens por chamada (para log de custo)
    ai_tokens_per_call_estimate: int = env_int("DALVAX_AI_TOKENS_PER_CALL", 725)  # midpoint 500-950

    # Risk core (BASIC ON)
    max_positions: int = env_int("DALVAX_MAX_POSITIONS", 1)
    leverage: int = env_int("DALVAX_LEVERAGE", 3)
    notional_usd: float = env_float("DALVAX_NOTIONAL_USD", 30.0)
    max_daily_loss_pct: float = env_float("DALVAX_MAX_DAILY_LOSS_PCT", 0.05)
    max_drawdown_pct: float = env_float("DALVAX_MAX_DRAWDOWN_PCT", 0.10)
    kill_switch_enabled: bool = env_bool("DALVAX_KILL_SWITCH_ENABLED", True)

    correlation_guard_enabled: bool = env_bool("DALVAX_CORRELATION_GUARD_ENABLED", True)
    corr_threshold: float = env_float("DALVAX_CORR_THRESHOLD", 0.85)
    cooldown_sec: int = env_int("DALVAX_COOLDOWN_SEC", 300)
    pair_lock_enabled: bool = env_bool("DALVAX_PAIR_LOCK_ENABLED", True)

    # Execution (BASIC ON)
    exec_opt_enabled: bool = env_bool("DALVAX_EXEC_OPT_ENABLED", True)
    order_type: str = os.getenv("DALVAX_ORDER_TYPE", "auto")
    max_slippage_pct: float = env_float("DALVAX_MAX_SLIPPAGE_PCT", 0.0025)
    post_only_when_possible: bool = env_bool("DALVAX_POST_ONLY_WHEN_POSSIBLE", True)

    # SL/TP (BASIC ON)
    stop_loss_pct: float = env_float("DALVAX_STOP_LOSS_PCT", 0.012)
    take_profit_pct: float = env_float("DALVAX_TAKE_PROFIT_PCT", 0.024)
    tp_mode: str = os.getenv("DALVAX_TP_MODE", "atr_rr")
    atr_mult_sl: float = env_float("DALVAX_ATR_MULT_SL", 1.7)
    rr_tp: float = env_float("DALVAX_RR_TP", 2.0)

    # Position manager (BASIC ON)
    pm_enabled: bool = env_bool("DALVAX_PM_ENABLED", True)
    be_enabled: bool = env_bool("DALVAX_BE_ENABLED", True)
    be_trigger_pct: float = env_float("DALVAX_BE_TRIGGER_PCT", 0.006)
    trail_enabled: bool = env_bool("DALVAX_TRAIL_ENABLED", True)
    trail_atr_mult: float = env_float("DALVAX_TRAIL_ATR_MULT", 1.5)
    trail_min_improvement: float = env_float("DALVAX_TRAIL_MIN_IMPROVEMENT", 0.2)
    partial_tp_enabled: bool = env_bool("DALVAX_PARTIAL_TP_ENABLED", True)
    partial_tp_pct: float = env_float("DALVAX_PARTIAL_TP_PCT", 0.50)
    partial_tp_at: float = env_float("DALVAX_PARTIAL_TP_AT", 0.012)

    # Monitoring (BASIC ON)
    health_api_enabled: bool = env_bool("DALVAX_HEALTH_API_ENABLED", True)
    health_api_port: int = env_int("DALVAX_HEALTH_API_PORT", 8080)
    metrics_enabled: bool = env_bool("DALVAX_METRICS_ENABLED", True)

    # Pumpwatch (optional, OFF)
    pump_enabled: bool = env_bool("DALVAX_PUMP_ENABLED", False)
    pump_universe_only: bool = env_bool("DALVAX_PUMP_UNIVERSE_ONLY", False)
    pump_min_vol_usdt: float = env_float("DALVAX_PUMP_MIN_VOL_USDT", 20_000_000.0)

    # Advanced modules (OFF by default)
    macro_sentiment_enabled: bool = env_bool("DALVAX_MACRO_SENTIMENT_ENABLED", False)
    market_simulation_enabled: bool = env_bool("DALVAX_MARKET_SIMULATION_ENABLED", False)
    knowledge_graph_enabled: bool = env_bool("DALVAX_KNOWLEDGE_GRAPH_ENABLED", False)
    self_evolution_enabled: bool = env_bool("DALVAX_SELF_EVOLUTION_ENABLED", False)
    global_market_intel_enabled: bool = env_bool("DALVAX_GLOBAL_MARKET_INTEL_ENABLED", False)

    # Fee model (taker 0.05%, maker 0.02% — OKX perpetual swaps)
    taker_fee_pct: float = env_float("DALVAX_TAKER_FEE_PCT", 0.0005)
    maker_fee_pct: float = env_float("DALVAX_MAKER_FEE_PCT", 0.0002)

    def validate(self) -> None:
        if self.real_enabled and self.simulated:
            raise ValueError("Config inválida: REAL_ENABLED=true e SIMULATED=true ao mesmo tempo.")
        if self.max_positions < 1:
            raise ValueError("DALVAX_MAX_POSITIONS deve ser >= 1.")
        if self.heatmap_top_n > self.max_universe_size:
            raise ValueError("DALVAX_HEATMAP_TOP_N não pode ser maior que DALVAX_MAX_UNIVERSE_SIZE.")
        if self.real_enabled and (not self.okx_api_key or not self.okx_api_secret or not self.okx_api_passphrase):
            raise ValueError("Chaves OKX ausentes para REAL_ENABLED=true.")
        if self.ai_enabled and self.ai_mode != "validate_only":
            raise ValueError("DALVAX_AI_MODE deve ser 'validate_only' nesta arquitetura (Opção B).")
        if self.rr_tp < 1.5:
            raise ValueError("DALVAX_RR_TP deve ser >= 1.5 para garantir RR mínimo positivo.")

# ============================================================
# Data models
# ============================================================

@dataclass
class AssetSnapshot:
    symbol: str
    price: float
    vol_24h_usdt: float
    spread_pct: float
    funding_rate: float
    open_interest: float
    vol_5m: float
    liq_5m_usdt: float

@dataclass
class HeatCandidate:
    symbol: str
    score: float
    snapshot: AssetSnapshot

@dataclass
class EventSignal:
    symbol: str
    event_type: str
    strength: float
    meta: Dict[str, Any]

@dataclass
class TradePlan:
    symbol: str
    side: str           # long|short
    confidence: float
    reason: str
    sl_pct: float
    tp_pct: float
    leverage: int
    notional_usd: float
    tags: Dict[str, Any]

@dataclass
class AiDecision:
    approved: bool
    confidence: float
    reason: str
    tweaks: Dict[str, Any]
    tokens_estimated: int = 0

@dataclass
class SimPosition:
    """Posição simulada com estado completo para PnL tracking."""
    id: str
    symbol: str
    side: str
    entry_price: float
    current_price: float
    qty_usd: float
    leverage: int
    sl_pct: float
    tp_pct: float
    sl_price: float
    tp_price: float
    be_triggered: bool = False
    partial_tp_done: bool = False
    trail_sl_price: float = 0.0
    realized_pnl_usd: float = 0.0
    ts_open: str = ""
    order_type: str = "market"

# ============================================================
# PnL Tracker
# ============================================================

@dataclass
class SessionMetrics:
    total_trades: int = 0
    wins: int = 0
    losses: int = 0
    total_pnl_usd: float = 0.0
    max_dd_usd: float = 0.0
    peak_pnl_usd: float = 0.0
    best_trade_usd: float = 0.0
    worst_trade_usd: float = 0.0
    ai_calls_total: int = 0
    ai_tokens_total: int = 0
    ai_skipped_precheck: int = 0
    ai_skipped_floor: int = 0
    ai_skipped_cache: int = 0
    ai_skipped_budget: int = 0
    ticks_total: int = 0
    candidates_total: int = 0

    def record_trade(self, pnl_usd: float) -> None:
        self.total_trades += 1
        self.total_pnl_usd += pnl_usd
        if pnl_usd > 0:
            self.wins += 1
        else:
            self.losses += 1
        if pnl_usd > self.best_trade_usd:
            self.best_trade_usd = pnl_usd
        if pnl_usd < self.worst_trade_usd:
            self.worst_trade_usd = pnl_usd
        if self.total_pnl_usd > self.peak_pnl_usd:
            self.peak_pnl_usd = self.total_pnl_usd
        dd = self.peak_pnl_usd - self.total_pnl_usd
        if dd > self.max_dd_usd:
            self.max_dd_usd = dd

    @property
    def win_rate(self) -> float:
        return self.wins / self.total_trades if self.total_trades > 0 else 0.0

    @property
    def expectancy_usd(self) -> float:
        return self.total_pnl_usd / self.total_trades if self.total_trades > 0 else 0.0

    @property
    def ai_cost_usd_anthropic_sonnet(self) -> float:
        # claude-3-5-sonnet: ~$3/M tokens input + $15/M output, rough avg $6/M
        return (self.ai_tokens_total / 1_000_000) * 6.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_trades": self.total_trades,
            "wins": self.wins,
            "losses": self.losses,
            "win_rate_pct": round(self.win_rate * 100, 1),
            "total_pnl_usd": round(self.total_pnl_usd, 4),
            "best_trade_usd": round(self.best_trade_usd, 4),
            "worst_trade_usd": round(self.worst_trade_usd, 4),
            "max_dd_usd": round(self.max_dd_usd, 4),
            "expectancy_usd": round(self.expectancy_usd, 4),
            "ai_calls_total": self.ai_calls_total,
            "ai_tokens_total": self.ai_tokens_total,
            "ai_cost_est_usd_sonnet": round(self.ai_cost_usd_anthropic_sonnet, 4),
            "ai_skipped_precheck": self.ai_skipped_precheck,
            "ai_skipped_floor": self.ai_skipped_floor,
            "ai_skipped_cache": self.ai_skipped_cache,
            "ai_skipped_budget": self.ai_skipped_budget,
            "ticks_total": self.ticks_total,
            "candidates_total": self.candidates_total,
        }

# ============================================================
# Journal (structured logs)
# ============================================================

class Journal:
    def __init__(self, run_id: str):
        self.run_id = run_id
        self._log = logging.getLogger("DALVAX.JOURNAL")

    def emit(self, event: str, **payload: Any) -> None:
        rec = {"ts": utc_ts(), "run_id": self.run_id, "event": event, **payload}
        self._log.info(jdump(rec))

# ============================================================
# State Store & Locks
# ============================================================

class StateStore:
    def __init__(self, path: str = "dalvax_state.json"):
        self.path = path
        self.state: Dict[str, Any] = {}

    def load(self) -> None:
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                self.state = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            self.state = {}

    def save(self) -> None:
        try:
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.state, f, ensure_ascii=False, indent=2)
            os.replace(tmp, self.path)
        except Exception:
            pass  # Non-critical


class PairLock:
    def __init__(self):
        self._locks: Dict[str, int] = {}
        self._cooldowns: Dict[str, int] = {}

    def is_locked(self, symbol: str) -> bool:
        return symbol in self._locks

    def lock(self, symbol: str) -> None:
        self._locks[symbol] = now_ms()

    def unlock(self, symbol: str) -> None:
        self._locks.pop(symbol, None)

    def set_cooldown(self, symbol: str, cooldown_sec: int) -> None:
        self._cooldowns[symbol] = now_ms() + cooldown_sec * 1000

    def in_cooldown(self, symbol: str) -> bool:
        ts = self._cooldowns.get(symbol)
        if ts is None:
            return False
        if now_ms() > ts:
            self._cooldowns.pop(symbol)
            return False
        return True

# ============================================================
# Exchange client (SAFE STUB — Simulação Realista)
# ============================================================

class OkxClient:
    def __init__(self, cfg: Config, journal: Journal, metrics: SessionMetrics):
        self.cfg = cfg
        self.journal = journal
        self.metrics = metrics
        self._positions: Dict[str, SimPosition] = {}
        self._price_cache: Dict[str, float] = {}

    def _synthetic_price(self, symbol: str, base: float = 100.0) -> float:
        """Gera preço sintético com random walk."""
        if symbol not in self._price_cache:
            idx = int(symbol.replace("COIN", "").split("-")[0]) if "COIN" in symbol else 1
            self._price_cache[symbol] = base * (0.8 + 0.4 * random.random()) * (1 + 0.02 * idx)
        # Random walk: ±0.3% por tick
        drift = random.gauss(0, 0.003)
        self._price_cache[symbol] = max(0.01, self._price_cache[symbol] * (1 + drift))
        return self._price_cache[symbol]

    async def fetch_market_universe(self) -> List[AssetSnapshot]:
        if self.cfg.real_enabled:
            raise NotImplementedError("REAL mode: implemente fetch_market_universe() com OKX.")
        symbols = [f"COIN{i}-USDT-SWAP" for i in range(1, self.cfg.max_universe_size + 1)]
        out: List[AssetSnapshot] = []
        for sym in symbols:
            p = self._synthetic_price(sym)
            vol24 = random.uniform(self.cfg.min_volume_24h_usdt, self.cfg.min_volume_24h_usdt * 10)
            spread = random.uniform(0.0008, 0.004)
            funding = random.uniform(-0.004, 0.006)
            oi = random.uniform(1_000_000, 50_000_000)
            vol5m = random.uniform(0.01, 0.09)
            liq5m = random.uniform(0, 600_000)
            out.append(AssetSnapshot(sym, p, vol24, spread, funding, oi, vol5m, liq5m))
        return out

    async def get_open_positions(self) -> List[SimPosition]:
        if self.cfg.real_enabled:
            raise NotImplementedError("REAL mode: implemente get_open_positions() com OKX.")
        return list(self._positions.values())

    async def place_order(self, plan: TradePlan, order_type: str) -> Dict[str, Any]:
        if self.cfg.real_enabled:
            raise NotImplementedError("REAL mode: implemente place_order() com OKX.")
        pos_id = hashlib.md5(f"{plan.symbol}-{time.time()}".encode()).hexdigest()[:10]
        entry = self._price_cache.get(plan.symbol, 100.0)
        fee_pct = self.cfg.taker_fee_pct if order_type == "market" else self.cfg.maker_fee_pct
        sl_price = entry * (1 - plan.sl_pct) if plan.side == "long" else entry * (1 + plan.sl_pct)
        tp_price = entry * (1 + plan.tp_pct) if plan.side == "long" else entry * (1 - plan.tp_pct)
        pos = SimPosition(
            id=pos_id,
            symbol=plan.symbol,
            side=plan.side,
            entry_price=entry,
            current_price=entry,
            qty_usd=plan.notional_usd * plan.leverage,
            leverage=plan.leverage,
            sl_pct=plan.sl_pct,
            tp_pct=plan.tp_pct,
            sl_price=sl_price,
            tp_price=tp_price,
            trail_sl_price=sl_price,
            ts_open=utc_ts(),
            order_type=order_type,
            realized_pnl_usd=-(plan.notional_usd * plan.leverage * fee_pct),  # taxa de abertura
        )
        self._positions[plan.symbol] = pos
        return {"status": "filled", "order_type": order_type, "entry_price": entry, "fee_pct": fee_pct}

    def _calc_unrealized_pnl(self, pos: SimPosition) -> float:
        if pos.side == "long":
            return pos.qty_usd * (pos.current_price - pos.entry_price) / pos.entry_price
        else:
            return pos.qty_usd * (pos.entry_price - pos.current_price) / pos.entry_price

    async def close_position(self, symbol: str, reason: str, close_price: Optional[float] = None) -> float:
        if self.cfg.real_enabled:
            raise NotImplementedError("REAL mode: implemente close_position() com OKX.")
        pos = self._positions.pop(symbol, None)
        if pos is None:
            return 0.0
        cp = close_price or self._price_cache.get(symbol, pos.entry_price)
        fee_pct = self.cfg.taker_fee_pct
        unrealized = self._calc_unrealized_pnl(pos)
        close_fee = pos.qty_usd * fee_pct
        total_pnl = pos.realized_pnl_usd + unrealized - close_fee
        self.metrics.record_trade(total_pnl)
        self.journal.emit("position_closed", symbol=symbol, reason=reason,
                          side=pos.side, entry=round(pos.entry_price, 4),
                          exit=round(cp, 4), pnl_usd=round(total_pnl, 4),
                          session_pnl_usd=round(self.metrics.total_pnl_usd, 4))
        return total_pnl

# ============================================================
# Heat Map Engine
# ============================================================

class HeatMapEngine:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._last_refresh_ms = 0
        self._cached: List[HeatCandidate] = []

    def _score(self, s: AssetSnapshot) -> float:
        vol_norm = clamp((s.vol_24h_usdt - self.cfg.min_volume_24h_usdt) / (self.cfg.min_volume_24h_usdt * 9), 0, 1)
        volat_norm = clamp((s.vol_5m - 0.01) / 0.08, 0, 1)
        oi_norm = clamp((s.open_interest - 1_000_000) / 49_000_000, 0, 1)
        liq_norm = clamp(s.liq_5m_usdt / 600_000, 0, 1)
        funding_stress = clamp(abs(s.funding_rate) / self.cfg.max_funding_rate, 0, 1) if self.cfg.max_funding_rate > 0 else 0.0
        score = (
            self.cfg.heat_w_volume * vol_norm +
            self.cfg.heat_w_volatility * volat_norm +
            self.cfg.heat_w_open_interest * oi_norm +
            self.cfg.heat_w_liquidations * liq_norm +
            self.cfg.heat_w_funding_stress * funding_stress
        )
        return clamp(score, 0, 1)

    def refresh_if_needed(self, universe: List[AssetSnapshot]) -> List[HeatCandidate]:
        if not self.cfg.heatmap_enabled:
            sorted_u = sorted(universe, key=lambda x: x.vol_24h_usdt, reverse=True)[: self.cfg.heatmap_top_n]
            return [HeatCandidate(s.symbol, 1.0, s) for s in sorted_u]
        n = now_ms()
        if n - self._last_refresh_ms < self.cfg.heatmap_refresh_sec * 1000 and self._cached:
            return self._cached
        candidates: List[HeatCandidate] = []
        for s in universe:
            if s.vol_24h_usdt < self.cfg.min_volume_24h_usdt:
                continue
            if s.spread_pct < self.cfg.min_spread_pct:
                continue
            score = self._score(s)
            if score >= self.cfg.heatmap_min_score:
                candidates.append(HeatCandidate(s.symbol, score, s))
        candidates.sort(key=lambda c: c.score, reverse=True)
        self._cached = candidates[: self.cfg.heatmap_top_n]
        self._last_refresh_ms = n
        return self._cached

# ============================================================
# Detectors (Event-driven)
# ============================================================

class LiquidationCascadeDetector:
    def __init__(self, cfg: Config):
        self.cfg = cfg

    def scan(self, c: HeatCandidate) -> Optional[EventSignal]:
        if c.snapshot.liq_5m_usdt > 200_000:
            strength = clamp(c.snapshot.liq_5m_usdt / 600_000, 0, 1)
            return EventSignal(c.symbol, "liquidation_cascade", strength,
                               {"liq_5m_usdt": c.snapshot.liq_5m_usdt, "funding": c.snapshot.funding_rate})
        return None

class GammaOIFundingDetector:
    def __init__(self, cfg: Config):
        self.cfg = cfg

    def scan(self, c: HeatCandidate) -> Optional[EventSignal]:
        funding_stress = abs(c.snapshot.funding_rate)
        if c.snapshot.open_interest > 30_000_000 and funding_stress > (self.cfg.max_funding_rate * 0.5):
            strength = clamp((c.snapshot.open_interest - 30_000_000) / 20_000_000, 0, 1) * clamp(funding_stress / self.cfg.max_funding_rate, 0, 1)
            return EventSignal(c.symbol, "gamma_oi_funding_stress", strength,
                               {"oi": c.snapshot.open_interest, "funding": c.snapshot.funding_rate})
        return None

class LiquidityIntelligenceEngine:
    def __init__(self, cfg: Config):
        self.cfg = cfg

    def scan(self, c: HeatCandidate) -> Optional[EventSignal]:
        if c.snapshot.spread_pct >= 0.0025 and c.snapshot.vol_5m >= 0.04:
            strength = clamp((c.snapshot.spread_pct - 0.0025) / 0.003, 0, 1) * clamp((c.snapshot.vol_5m - 0.04) / 0.05, 0, 1)
            return EventSignal(c.symbol, "liquidity_sweep_risk", strength,
                               {"spread_pct": c.snapshot.spread_pct, "vol_5m": c.snapshot.vol_5m})
        return None

class CapitalFlowEngine:
    def __init__(self, cfg: Config): self.cfg = cfg
    def scan(self, c: HeatCandidate) -> Optional[EventSignal]: return None

class MicrostructureLite:
    def __init__(self, cfg: Config): self.cfg = cfg
    def scan(self, c: HeatCandidate) -> Optional[EventSignal]: return None

# ============================================================
# Advanced Modules (stubs; OFF by default)
# ============================================================

class MacroSentimentEngine:
    def __init__(self, cfg: Config): self.cfg = cfg
    def score(self, universe: List[AssetSnapshot]) -> Dict[str, Any]:
        return {"macro_sentiment": "neutral", "macro_score": 0.0}

class MarketSimulationEngine:
    def __init__(self, cfg: Config): self.cfg = cfg
    def simulate(self, plan: TradePlan) -> Dict[str, Any]:
        return {"sim_ok": True, "sim_score": 0.0}

class KnowledgeGraphEngine:
    def __init__(self, cfg: Config): self.cfg = cfg
    def annotate(self, plan: TradePlan) -> Dict[str, Any]:
        return {"kg_notes": ""}

class SelfEvolutionEngine:
    def __init__(self, cfg: Config): self.cfg = cfg
    def propose_tuning(self) -> Dict[str, Any]:
        return {"tuning": None}

class GlobalMarketIntelEngine:
    def __init__(self, cfg: Config): self.cfg = cfg
    def cross_check(self, plan: TradePlan) -> Dict[str, Any]:
        return {"cross_market_ok": True}

# ============================================================
# Event priority
# ============================================================

class EventPriorityEngine:
    ORDER = {
        "liquidation_cascade": 1,
        "gamma_oi_funding_stress": 2,
        "liquidity_sweep_risk": 3,
        "capital_flow": 4,
        "microstructure": 5,
    }

    def pick_best(self, events: List[EventSignal]) -> Optional[EventSignal]:
        if not events:
            return None
        events.sort(key=lambda e: (self.ORDER.get(e.event_type, 99), -e.strength))
        return events[0]

# ============================================================
# Regime Engine
# ============================================================

class RegimeEngine:
    def __init__(self, cfg: Config):
        self.cfg = cfg

    def detect(self, universe: List[AssetSnapshot]) -> Dict[str, Any]:
        if not self.cfg.regime_enabled:
            return {"regime": "disabled", "vol_state": "n/a", "score": 0.0, "tradeable": True}
        vols = sorted([u.vol_5m for u in universe])
        if not vols:
            return {"regime": "unknown", "vol_state": "unknown", "score": 0.0, "tradeable": False}
        med = vols[len(vols) // 2]
        if med >= 0.07:
            return {"regime": "sniper", "vol_state": "extreme", "score": round(med, 4), "tradeable": True}
        if med >= 0.045:
            return {"regime": "predator", "vol_state": "high", "score": round(med, 4), "tradeable": True}
        if med >= 0.025:
            return {"regime": "trend_or_range", "vol_state": "normal", "score": round(med, 4), "tradeable": True}
        # FIX V1.1.0: regime "wait" é explicitamente não-tradeable
        return {"regime": "wait", "vol_state": "low", "score": round(med, 4), "tradeable": False}

# ============================================================
# Meta Decision Engine
# ============================================================

class MetaDecisionEngine:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._paused_until_ms = 0
        self._consec_errors = 0

    def note_error(self) -> None:
        self._consec_errors += 1
        if (self.cfg.meta_enabled and self.cfg.meta_pause_on_errors
                and self._consec_errors >= self.cfg.meta_max_consec_errors):
            self._paused_until_ms = now_ms() + self.cfg.meta_error_cooldown_sec * 1000

    def note_ok(self) -> None:
        self._consec_errors = 0

    def allow_trading(self, regime: Dict[str, Any]) -> Tuple[bool, str]:
        if not self.cfg.meta_enabled:
            return True, "meta_disabled"
        if now_ms() < self._paused_until_ms:
            return False, "meta_error_cooldown"
        # FIX V1.1.0: bloquear regime "wait"
        if self.cfg.meta_block_on_wait_regime and not regime.get("tradeable", True):
            return False, f"regime_{regime.get('regime', 'unknown')}_not_tradeable"
        return True, "ok"

# ============================================================
# Strategy Orchestrator — SIDE LOGIC CORRIGIDA V1.1.0
# ============================================================

class StrategyOrchestrator:
    def __init__(self, cfg: Config):
        self.cfg = cfg

    def enabled_for(self, event: EventSignal, regime: Dict[str, Any]) -> bool:
        return self.cfg.orchestrator_enabled

    def _determine_side(self, event: EventSignal, snap: AssetSnapshot, regime: Dict[str, Any]) -> str:
        """
        Lógica de side baseada em múltiplos fatores (V1.1.0).
        - liquidation_cascade: funding alto positivo → SHORT (short squeeze likely);
                               funding negativo → LONG (long squeeze recovery)
        - gamma_oi_funding_stress: funding extremo positivo → SHORT; negativo → LONG
        - liquidity_sweep_risk: contra o spike de spread (mean reversion)
        - Default: funding como tiebreaker
        """
        et = event.event_type
        funding = snap.funding_rate

        if et == "liquidation_cascade":
            # Cascade de longs (funding alto positivo) → SHORT; cascade de shorts → LONG
            return "short" if funding > 0.001 else "long"

        if et == "gamma_oi_funding_stress":
            # Funding extremo positivo: longs pagando muito → pressão para SHORT
            return "short" if funding > 0 else "long"

        if et == "liquidity_sweep_risk":
            # Spread alto + volatilidade → mean reversion; side oposto ao movimento
            return "long" if funding < 0 else "short"

        # Default: funding como tiebreaker
        return "short" if funding > 0 else "long"

    def _confidence_score(self, event: EventSignal, candidate: HeatCandidate,
                          regime: Dict[str, Any], snap: AssetSnapshot) -> float:
        """
        Confidence multi-fator V1.1.0:
        - Base: 0.50
        - Event strength: até +0.20
        - Heat score: até +0.10
        - Regime bonus: sniper=+0.08, predator=+0.05, normal=+0.02
        - Funding alignment: até +0.05
        """
        base = 0.50
        ev_contrib = 0.20 * event.strength
        heat_contrib = 0.10 * candidate.score
        reg = regime.get("regime", "unknown")
        regime_bonus = {"sniper": 0.08, "predator": 0.05, "trend_or_range": 0.02}.get(reg, 0.0)
        funding_bonus = clamp(abs(snap.funding_rate) / self.cfg.max_funding_rate * 0.05, 0, 0.05)
        return clamp(base + ev_contrib + heat_contrib + regime_bonus + funding_bonus, 0, 1)

    def build_trade_plan(self, event: EventSignal, candidate: HeatCandidate,
                         cfg: Config, regime: Dict[str, Any]) -> TradePlan:
        snap = candidate.snapshot
        side = self._determine_side(event, snap, regime)
        confidence = self._confidence_score(event, candidate, regime, snap)
        reason = (f"{event.event_type} strength={event.strength:.2f} "
                  f"heat={candidate.score:.2f} regime={regime.get('regime')} "
                  f"funding={snap.funding_rate:.4f} side={side}")
        # ATR/RR-based SL/TP
        sl = cfg.stop_loss_pct
        tp = sl * cfg.rr_tp
        return TradePlan(
            symbol=event.symbol,
            side=side,
            confidence=confidence,
            reason=reason,
            sl_pct=sl,
            tp_pct=tp,
            leverage=cfg.leverage,
            notional_usd=cfg.notional_usd,
            tags={
                "event": event.event_type,
                "event_strength": event.strength,
                "heat_score": candidate.score,
                "regime": regime,
                "funding": snap.funding_rate,
            },
        )

# ============================================================
# AI Validate Gate (Option B) — V1.1.0
# ============================================================

class AIValidateGate:
    def __init__(self, cfg: Config, journal: Journal, state: StateStore, metrics: SessionMetrics):
        self.cfg = cfg
        self.journal = journal
        self.state = state
        self.metrics = metrics
        self._ensure_budget()

    def _ensure_budget(self) -> None:
        today = utc_date()
        b = self.state.state.get("ai_budget", {})
        if b.get("date") != today:
            # FIX V1.1.0: reset completo incluindo reject_cache ao virar o dia
            self.state.state["ai_budget"] = {
                "date": today,
                "calls_today": 0,
                "tokens_today": 0,
                "reject_cache": {},
            }

    def _b(self) -> Dict[str, Any]:
        self._ensure_budget()
        return self.state.state["ai_budget"]

    def _cache_rejected(self, symbol: str) -> None:
        self._b()["reject_cache"][symbol] = now_ms()

    def _is_recently_rejected(self, symbol: str) -> bool:
        if self.cfg.ai_reject_cache_minutes <= 0:
            return False
        ts = self._b().get("reject_cache", {}).get(symbol)
        if not ts:
            return False
        return (now_ms() - int(ts)) < self.cfg.ai_reject_cache_minutes * 60 * 1000

    def _budget_remaining(self) -> int:
        if not self.cfg.ai_cost_guard_enabled:
            return 10 ** 9
        calls = int(self._b().get("calls_today", 0))
        return max(0, int(self.cfg.ai_max_calls_per_day) - calls)

    def _consume_call(self) -> int:
        b = self._b()
        b["calls_today"] = int(b.get("calls_today", 0)) + 1
        tokens = self.cfg.ai_tokens_per_call_estimate
        b["tokens_today"] = int(b.get("tokens_today", 0)) + tokens
        self.metrics.ai_calls_total += 1
        self.metrics.ai_tokens_total += tokens
        return tokens

    async def evaluate(self, plan: TradePlan) -> AiDecision:
        self._ensure_budget()
        b = self._b()
        calls_today = int(b.get("calls_today", 0))
        budget_rem = self._budget_remaining()

        # Provider none → deterministic (zero cost)
        if not self.cfg.ai_enabled or self.cfg.ai_provider.strip().lower() == "none":
            approved = plan.confidence >= self.cfg.ai_min_confidence
            if not approved:
                self._cache_rejected(plan.symbol)
            return AiDecision(approved, plan.confidence,
                              "threshold_approved" if approved else "threshold_rejected",
                              {}, tokens_estimated=0)

        # Confidence floor guard
        if plan.confidence < self.cfg.ai_min_confidence_call:
            self.metrics.ai_skipped_floor += 1
            return AiDecision(False, plan.confidence, "ai_skipped_below_call_floor",
                              {}, tokens_estimated=0)

        # Reject cache guard
        if self._is_recently_rejected(plan.symbol):
            self.metrics.ai_skipped_cache += 1
            return AiDecision(False, plan.confidence, "ai_skipped_recent_reject_cache",
                              {}, tokens_estimated=0)

        # Budget guard
        if budget_rem <= 0:
            self.metrics.ai_skipped_budget += 1
            return AiDecision(False, plan.confidence, "ai_skipped_budget_exhausted",
                              {}, tokens_estimated=0)

        tokens = self._consume_call()
        approved = plan.confidence >= self.cfg.ai_min_confidence
        reason = "approved_by_threshold" if approved else "rejected_low_confidence"
        if not approved:
            self._cache_rejected(plan.symbol)
        return AiDecision(approved, plan.confidence, reason, {}, tokens_estimated=tokens)

    def budget_snapshot(self) -> Dict[str, Any]:
        b = self._b()
        return {
            "ai_calls_today": int(b.get("calls_today", 0)),
            "ai_tokens_today": int(b.get("tokens_today", 0)),
            "ai_budget_remaining": self._budget_remaining(),
            "ai_cost_guard": self.cfg.ai_cost_guard_enabled,
            "ai_provider": self.cfg.ai_provider,
        }

# ============================================================
# Risk Engine
# ============================================================

class RiskEngine:
    def __init__(self, cfg: Config, journal: Journal, state: StateStore, metrics: SessionMetrics):
        self.cfg = cfg
        self.journal = journal
        self.state = state
        self.metrics = metrics

    def kill_switch_tripped(self) -> bool:
        if not self.cfg.kill_switch_enabled:
            return False
        pnl = self.metrics.total_pnl_usd
        notional = self.cfg.notional_usd
        if pnl < -(notional * self.cfg.max_daily_loss_pct * self.cfg.leverage):
            return True
        if self.metrics.max_dd_usd > notional * self.cfg.max_drawdown_pct * self.cfg.leverage:
            return True
        return False

    def validate(self, plan: TradePlan, open_positions: List[SimPosition]) -> Tuple[bool, str]:
        if self.kill_switch_tripped():
            return False, "kill_switch_tripped"
        if len(open_positions) >= self.cfg.max_positions:
            return False, "max_positions_reached"
        return True, "ok"

# ============================================================
# Execution Optimizer + Executor
# ============================================================

class ExecutionOptimizer:
    def __init__(self, cfg: Config):
        self.cfg = cfg

    def choose_order_type(self, plan: TradePlan) -> str:
        if self.cfg.order_type != "auto":
            return self.cfg.order_type
        ev = plan.tags.get("event", "")
        return "market" if ev == "liquidation_cascade" else "limit"


class TradeExecutor:
    def __init__(self, cfg: Config, client: OkxClient, journal: Journal):
        self.cfg = cfg
        self.client = client
        self.journal = journal

    async def execute(self, plan: TradePlan, order_type: str) -> Dict[str, Any]:
        if self.cfg.dry_run and not self.cfg.simulated:
            self.journal.emit("dry_run_order", symbol=plan.symbol, side=plan.side,
                              notional_usd=plan.notional_usd, order_type=order_type)
            return {"status": "dry_run"}
        res = await self.client.place_order(plan, order_type=order_type)
        self.journal.emit("order_executed", symbol=plan.symbol, side=plan.side,
                          confidence=round(plan.confidence, 4),
                          notional_usd=plan.notional_usd, order_type=order_type,
                          entry_price=res.get("entry_price"), fee_pct=res.get("fee_pct"))
        return res

# ============================================================
# Position Manager — PnL tracking realista V1.1.0
# ============================================================

class PositionManager:
    def __init__(self, cfg: Config, client: OkxClient, journal: Journal, pair_lock: PairLock):
        self.cfg = cfg
        self.client = client
        self.journal = journal
        self.pair_lock = pair_lock

    async def tick(self) -> None:
        if not self.cfg.pm_enabled:
            return
        positions = await self.client.get_open_positions()
        for pos in list(positions):
            # Atualiza preço atual
            new_price = self.client._synthetic_price(pos.symbol)
            pos.current_price = new_price

            pnl_pct = (new_price - pos.entry_price) / pos.entry_price
            if pos.side == "short":
                pnl_pct = -pnl_pct

            close_reason = None
            close_price = new_price

            # TP hit
            if pos.side == "long" and new_price >= pos.tp_price:
                close_reason = "tp_hit"
            elif pos.side == "short" and new_price <= pos.tp_price:
                close_reason = "tp_hit"

            # SL hit
            elif pos.side == "long" and new_price <= pos.sl_price:
                close_reason = "sl_hit"
            elif pos.side == "short" and new_price >= pos.sl_price:
                close_reason = "sl_hit"

            # Trailing stop
            elif self.cfg.trail_enabled and pos.trail_sl_price > 0:
                if pos.side == "long":
                    new_trail = new_price * (1 - self.cfg.stop_loss_pct * self.cfg.trail_atr_mult)
                    if new_trail > pos.trail_sl_price * (1 + self.cfg.trail_min_improvement * 0.01):
                        pos.trail_sl_price = new_trail
                    if new_price <= pos.trail_sl_price:
                        close_reason = "trailing_stop"
                else:
                    new_trail = new_price * (1 + self.cfg.stop_loss_pct * self.cfg.trail_atr_mult)
                    if new_trail < pos.trail_sl_price * (1 - self.cfg.trail_min_improvement * 0.01):
                        pos.trail_sl_price = new_trail
                    if new_price >= pos.trail_sl_price:
                        close_reason = "trailing_stop"

            # Break-Even
            if not pos.be_triggered and self.cfg.be_enabled:
                if pnl_pct >= self.cfg.be_trigger_pct:
                    pos.be_triggered = True
                    pos.sl_price = pos.entry_price * (1.0001 if pos.side == "long" else 0.9999)
                    self.journal.emit("be_triggered", symbol=pos.symbol, side=pos.side,
                                      entry=round(pos.entry_price, 4), new_sl=round(pos.sl_price, 4))

            # Partial TP
            if (not pos.partial_tp_done and self.cfg.partial_tp_enabled
                    and pnl_pct >= self.cfg.partial_tp_at):
                partial_qty = pos.qty_usd * self.cfg.partial_tp_pct
                partial_pnl = partial_qty * pnl_pct - partial_qty * self.cfg.taker_fee_pct
                pos.realized_pnl_usd += partial_pnl
                pos.qty_usd -= partial_qty
                pos.partial_tp_done = True
                self.journal.emit("partial_tp", symbol=pos.symbol, pnl_usd=round(partial_pnl, 4),
                                  qty_remaining_usd=round(pos.qty_usd, 4))

            if close_reason:
                pnl = await self.client.close_position(pos.symbol, close_reason, close_price)
                self.pair_lock.unlock(pos.symbol)
                self.pair_lock.set_cooldown(pos.symbol, self.cfg.cooldown_sec)

# ============================================================
# Health Monitor
# ============================================================

class HealthMonitor:
    def __init__(self, cfg: Config, journal: Journal):
        self.cfg = cfg
        self.journal = journal
        self._errors = 0

    def note_error(self, where: str, err: Exception) -> None:
        self._errors += 1
        self.journal.emit("error", where=where, error=str(err), errors=self._errors)

    async def tick(self) -> None:
        await asyncio.sleep(0)

# ============================================================
# Runtime (Core Loop + tasks) — V1.1.0
# ============================================================

class DalvaxRuntime:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.run_id = hashlib.md5(f"{utc_ts()}-{random.random()}".encode()).hexdigest()[:12]
        self._shutdown = asyncio.Event()

        logging.basicConfig(
            level=getattr(logging, cfg.log_level.upper(), logging.INFO),
            format="%(message)s"
        )
        self.metrics = SessionMetrics()
        self.journal = Journal(self.run_id)
        self.state = StateStore()
        self.state.load()

        self.pair_lock = PairLock()
        self.client = OkxClient(cfg, self.journal, self.metrics)

        self.heatmap = HeatMapEngine(cfg)
        self.det_liq = LiquidationCascadeDetector(cfg)
        self.det_gamma = GammaOIFundingDetector(cfg)
        self.det_liqintel = LiquidityIntelligenceEngine(cfg)
        self.det_capflow = CapitalFlowEngine(cfg)
        self.det_micro = MicrostructureLite(cfg)

        self.prior = EventPriorityEngine()
        self.regime_engine = RegimeEngine(cfg)
        self.meta = MetaDecisionEngine(cfg)
        self.orch = StrategyOrchestrator(cfg)

        self.risk = RiskEngine(cfg, self.journal, self.state, self.metrics)
        self.ai = AIValidateGate(cfg, self.journal, self.state, self.metrics)

        self.execopt = ExecutionOptimizer(cfg)
        self.executor = TradeExecutor(cfg, self.client, self.journal)
        self.pm = PositionManager(cfg, self.client, self.journal, self.pair_lock)
        self.health = HealthMonitor(cfg, self.journal)

    def request_shutdown(self) -> None:
        self._shutdown.set()

    async def position_manager_task(self) -> None:
        while not self._shutdown.is_set():
            try:
                await self.pm.tick()
                await asyncio.sleep(self.cfg.position_poll_sec)
            except Exception as e:
                self.health.note_error("position_manager_task", e)
                self.meta.note_error()

    async def health_monitor_task(self) -> None:
        while not self._shutdown.is_set():
            try:
                await self.health.tick()
                await asyncio.sleep(self.cfg.health_check_sec)
            except Exception as e:
                self.health.note_error("health_monitor_task", e)
                self.meta.note_error()

    async def scanner_tick(self) -> None:
        self.metrics.ticks_total += 1
        universe = await self.client.fetch_market_universe()
        shortlist = self.heatmap.refresh_if_needed(universe)

        self.journal.emit("heatmap", universe_size=len(universe),
                          shortlist=[{"sym": c.symbol, "score": round(c.score, 4)} for c in shortlist],
                          tick=self.metrics.ticks_total)

        events: List[EventSignal] = []
        for c in shortlist:
            if self.cfg.pair_lock_enabled and self.pair_lock.is_locked(c.symbol):
                continue
            if self.cfg.pair_lock_enabled and self.pair_lock.in_cooldown(c.symbol):
                continue
            if self.cfg.detect_liquidation_enabled:
                ev = self.det_liq.scan(c)
                if ev: events.append(ev)
            if self.cfg.detect_gamma_oi_enabled:
                ev = self.det_gamma.scan(c)
                if ev: events.append(ev)
            if self.cfg.detect_liquidity_intel_enabled:
                ev = self.det_liqintel.scan(c)
                if ev: events.append(ev)
            if self.cfg.detect_capital_flow_enabled:
                ev = self.det_capflow.scan(c)
                if ev: ev.event_type = "capital_flow"; events.append(ev)
            if self.cfg.detect_microstructure_lite_enabled:
                ev = self.det_micro.scan(c)
                if ev: ev.event_type = "microstructure"; events.append(ev)

        best = self.prior.pick_best(events)
        if not best:
            self.meta.note_ok()
            return

        self.metrics.candidates_total += 1
        regime = self.regime_engine.detect(universe)
        self.journal.emit("regime", **regime)

        # ====== META DECISION ======
        ok_meta, meta_reason = self.meta.allow_trading(regime)
        if not ok_meta:
            self.journal.emit("meta_block", reason=meta_reason, regime=regime.get("regime"),
                              **self.ai.budget_snapshot())
            return

        # ====== PRE-CHECK RISCO ANTES DA IA (V1.0.1 + V1.1.0) ======
        open_positions = await self.client.get_open_positions()
        if self.risk.kill_switch_tripped():
            self.metrics.ai_skipped_precheck += 1
            self.journal.emit("pre_ai_block", reason="kill_switch_tripped",
                              session_pnl_usd=round(self.metrics.total_pnl_usd, 4),
                              **self.ai.budget_snapshot())
            return
        if len(open_positions) >= self.cfg.max_positions:
            self.metrics.ai_skipped_precheck += 1
            self.journal.emit("pre_ai_block", reason="max_positions_reached",
                              open_positions=len(open_positions), **self.ai.budget_snapshot())
            return
        # ===============================================================

        cand = next((c for c in shortlist if c.symbol == best.symbol), None)
        if not cand:
            return

        if not self.orch.enabled_for(best, regime):
            self.journal.emit("orchestrator_block", event=best.event_type, symbol=best.symbol)
            return

        plan = self.orch.build_trade_plan(best, cand, self.cfg, regime)
        self.journal.emit("trade_candidate", symbol=plan.symbol, side=plan.side,
                          confidence=round(plan.confidence, 4), reason=plan.reason,
                          **self.ai.budget_snapshot())

        # ====== AI VALIDATE ======
        ai_dec = await self.ai.evaluate(plan)
        self.journal.emit("ai_decision", approved=ai_dec.approved, reason=ai_dec.reason,
                          confidence=round(ai_dec.confidence, 4),
                          tokens=ai_dec.tokens_estimated,
                          **self.ai.budget_snapshot())
        if not ai_dec.approved:
            return

        # ====== RISK FINAL ======
        ok_risk, why = self.risk.validate(plan, open_positions)
        self.journal.emit("risk_check", ok=ok_risk, reason=why)
        if not ok_risk:
            # FIX V1.1.0: não bloqueia pair lock se risco rejeitou
            return

        # ====== PAIR LOCK + EXECUTE ======
        if self.cfg.pair_lock_enabled:
            if self.pair_lock.is_locked(plan.symbol):
                return
            self.pair_lock.lock(plan.symbol)

        try:
            order_type = self.execopt.choose_order_type(plan) if self.cfg.exec_opt_enabled else self.cfg.order_type
            await self.executor.execute(plan, order_type=order_type)
            self.meta.note_ok()
        except Exception as e:
            self.health.note_error("execute", e)
            self.meta.note_error()
            if self.cfg.pair_lock_enabled:
                self.pair_lock.unlock(plan.symbol)

        self.state.state["last_tick_ts"] = utc_ts()
        self.state.save()

    async def run(self) -> None:
        self.journal.emit(
            "boot_snapshot",
            version="MONOLITO_FULL_V1.1.0_IDEAL",
            simulated=self.cfg.simulated,
            dry_run=self.cfg.dry_run,
            real_enabled=self.cfg.real_enabled,
            loop_sec=self.cfg.loop_sec,
            max_positions=self.cfg.max_positions,
            leverage=self.cfg.leverage,
            notional_usd=self.cfg.notional_usd,
            universe_size=self.cfg.max_universe_size,
            heatmap_top_n=self.cfg.heatmap_top_n,
            heatmap_min_score=self.cfg.heatmap_min_score,
            ai_provider=self.cfg.ai_provider,
            ai_cost_guard=self.cfg.ai_cost_guard_enabled,
            ai_max_calls_per_day=self.cfg.ai_max_calls_per_day,
            ai_min_confidence=self.cfg.ai_min_confidence,
            ai_min_confidence_call=self.cfg.ai_min_confidence_call,
            ai_reject_cache_minutes=self.cfg.ai_reject_cache_minutes,
            rr_tp=self.cfg.rr_tp,
            sl_pct=self.cfg.stop_loss_pct,
            tp_pct=self.cfg.take_profit_pct,
        )

        tasks = [
            asyncio.create_task(self.position_manager_task(), name="position_manager"),
            asyncio.create_task(self.health_monitor_task(), name="health_monitor"),
        ]

        try:
            while not self._shutdown.is_set():
                try:
                    await self.scanner_tick()
                except Exception as e:
                    self.health.note_error("scanner_tick", e)
                    self.meta.note_error()
                await asyncio.sleep(self.cfg.loop_sec)
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            # FIX V1.1.0: salvar state no shutdown
            self.state.save()
            self.journal.emit("shutdown", reason="requested",
                              session_summary=self.metrics.to_dict())

# ============================================================
# Entrypoint
# ============================================================

def _install_signal_handlers(rt: DalvaxRuntime) -> None:
    def _handler(sig, frame):
        rt.journal.emit("signal", sig=str(sig))
        rt.request_shutdown()
    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)


async def main() -> None:
    cfg = Config()
    cfg.validate()
    rt = DalvaxRuntime(cfg)
    _install_signal_handlers(rt)
    await rt.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
