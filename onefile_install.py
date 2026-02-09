#!/usr/bin/env python3
"""One-file installer for the Telegram-controlled crypto trading bot."""
from __future__ import annotations

import json
import os
import platform
import subprocess
import sys
from pathlib import Path

BOT_TEMPLATE = r'''#!/usr/bin/env python3
"""Telegram-controlled crypto trading bot (educational use only).

DISCLAIMER: This software is for educational purposes only and is not financial advice.
Trading cryptocurrencies involves risk. You are solely responsible for any trading decisions
and losses. This bot includes rate limits and is not intended for high-frequency trading.
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import signal
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import websockets
from dotenv import load_dotenv
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ConversationHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

ENV_PATH = Path(__file__).parent / ".env"
CONFIG_PATH = Path(__file__).parent / "config.json"
WALLET_PATH = Path(__file__).parent / "wallet.json"
SIM_STATE_PATH = Path(__file__).parent / "sim_state.json"

PUMP_WS_URL = "wss://pumpportal.fun/api/data"
PUMP_TRADE_LOCAL = "https://pumpportal.fun/api/trade-local"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
LOGGER = logging.getLogger("bot")

STATE_MINT = 1
STATE_CONFIRM = 2
STATE_EDIT = 3
STATE_EDIT_GLOBAL = 4

@dataclass
class SimState:
    sol_balance: float = 0.0
    token_balances: Dict[str, float] = field(default_factory=dict)
    trade_log: List[Dict[str, Any]] = field(default_factory=list)
    realized_pnl: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "sol_balance": self.sol_balance,
            "token_balances": self.token_balances,
            "trade_log": self.trade_log,
            "realized_pnl": self.realized_pnl,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SimState":
        return cls(
            sol_balance=float(data.get("sol_balance", 0.0)),
            token_balances=dict(data.get("token_balances", {})),
            trade_log=list(data.get("trade_log", [])),
            realized_pnl=float(data.get("realized_pnl", 0.0)),
        )


def load_env() -> None:
    load_dotenv(ENV_PATH)


def load_config() -> Dict[str, Any]:
    with CONFIG_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def save_config(config: Dict[str, Any]) -> None:
    with CONFIG_PATH.open("w", encoding="utf-8") as handle:
        json.dump(config, handle, indent=2, sort_keys=True)


def load_wallet() -> Keypair:
    if not WALLET_PATH.exists():
        keypair = Keypair()
        secret = list(bytes(keypair))
        WALLET_PATH.write_text(json.dumps(secret))
        try:
            os.chmod(WALLET_PATH, 0o600)
        except PermissionError:
            LOGGER.warning("Unable to set restrictive permissions on wallet.json")
        return keypair
    data = json.loads(WALLET_PATH.read_text())
    secret = bytes(data)
    return Keypair.from_bytes(secret)


def load_sim_state() -> SimState:
    if not SIM_STATE_PATH.exists():
        state = SimState()
        save_sim_state(state)
        return state
    data = json.loads(SIM_STATE_PATH.read_text())
    return SimState.from_dict(data)


def save_sim_state(state: SimState) -> None:
    SIM_STATE_PATH.write_text(json.dumps(state.to_dict(), indent=2, sort_keys=True))


def now_ts() -> float:
    return time.time()


def pump_fun_url(mint: str) -> str:
    return f"https://pump.fun/{mint}"


def parse_allowed_users(value: str) -> List[int]:
    return [int(item.strip()) for item in value.split(",") if item.strip()]


def check_allowed(update: Update, allowed_ids: List[int]) -> bool:
    if update.effective_user is None:
        return False
    return update.effective_user.id in allowed_ids


def format_bool(value: bool) -> str:
    return "ON" if value else "OFF"


def to_float(value: str) -> Optional[float]:
    try:
        return float(value)
    except ValueError:
        return None


def to_int(value: str) -> Optional[int]:
    try:
        return int(value)
    except ValueError:
        return None


class StrategyEngine:
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def active_strategy_name(self) -> str:
        return self.config["strategy"]["active"]

    def active_strategy_params(self) -> Dict[str, Any]:
        name = self.active_strategy_name()
        return self.config["strategy"]["strategies"][name]

    def global_exits(self) -> Dict[str, Any]:
        return self.config["strategy"]["global_exits"]

    def should_enter(self, prices: List[float], trade_times: List[float]) -> Tuple[bool, str]:
        if not prices:
            return False, "no data"
        name = self.active_strategy_name()
        params = self.active_strategy_params()
        if name == "timebox_v1":
            base_name = params.get("base_strategy", "momentum_v1")
            base_params = self.config["strategy"]["strategies"].get(base_name, {})
            return self._should_enter_for(prices, trade_times, base_name, base_params)
        return self._should_enter_for(prices, trade_times, name, params)

    def _should_enter_for(
        self,
        prices: List[float],
        trade_times: List[float],
        name: str,
        params: Dict[str, Any],
    ) -> Tuple[bool, str]:
        min_trades = int(params.get("min_trades_in_window", 0))
        window_seconds = int(params.get("window_seconds", 60))
        cutoff = now_ts() - window_seconds
        trade_count = sum(1 for t in trade_times if t >= cutoff)
        if trade_count < min_trades:
            return False, f"trade count {trade_count} < {min_trades}"

        if name == "momentum_v1":
            lookback = int(params.get("lookback", 5))
            min_momentum = float(params.get("min_momentum_pct", 1.0))
            if len(prices) <= lookback:
                return False, "insufficient lookback"
            base = prices[-lookback - 1]
            if base <= 0:
                return False, "invalid base"
            momentum = (prices[-1] - base) / base * 100
            return momentum >= min_momentum, f"momentum {momentum:.2f}%"

        if name == "breakout_v1":
            lookback = int(params.get("lookback", 10))
            breakout = float(params.get("breakout_pct", 1.0))
            if len(prices) <= lookback:
                return False, "insufficient lookback"
            recent_high = max(prices[-lookback - 1 : -1])
            threshold = recent_high * (1 + breakout / 100)
            return prices[-1] >= threshold, f"breakout {prices[-1]:.6f} >= {threshold:.6f}"

        if name == "mean_reversion_v1":
            lookback = int(params.get("lookback", 20))
            zscore_entry = float(params.get("zscore_entry", -1.5))
            if len(prices) <= lookback:
                return False, "insufficient lookback"
            window = prices[-lookback:]
            mean = sum(window) / len(window)
            variance = sum((p - mean) ** 2 for p in window) / len(window)
            std = variance ** 0.5
            if std == 0:
                return False, "zero std"
            zscore = (prices[-1] - mean) / std
            return zscore <= zscore_entry, f"z-score {zscore:.2f}"

        return False, "unknown strategy"


@dataclass
class Position:
    mint: str
    entry_price: float
    size_sol: float
    tokens: float
    entry_time: float
    strategy: str
    peak_price: float


class Trader:
    def __init__(self, config: Dict[str, Any], wallet: Keypair, sim_state: SimState):
        self.config = config
        self.wallet = wallet
        self.sim_state = sim_state
        self.positions: Dict[str, Position] = {}
        self.last_trade_time: Dict[str, float] = {}
        self.trade_timestamps: List[float] = []
        self.daily_pnl: float = 0.0
        self.daily_reset = datetime.now(timezone.utc).date()
        self.locked_until: Optional[float] = None

    def reset_daily_if_needed(self) -> None:
        today = datetime.now(timezone.utc).date()
        if today != self.daily_reset:
            self.daily_pnl = 0.0
            self.daily_reset = today

    def in_lockout(self) -> bool:
        if self.locked_until and now_ts() < self.locked_until:
            return True
        return False

    def record_trade_timestamp(self) -> None:
        self.trade_timestamps.append(now_ts())
        cutoff = now_ts() - 3600
        self.trade_timestamps = [t for t in self.trade_timestamps if t >= cutoff]

    def max_trades_reached(self) -> bool:
        max_per_hour = int(self.config["risk"]["max_trades_per_hour"])
        return len(self.trade_timestamps) >= max_per_hour

    def cooldown_active(self, mint: str) -> bool:
        cooldown = float(self.config["risk"]["min_seconds_between_trades"])
        last_time = self.last_trade_time.get(mint)
        return last_time is not None and now_ts() - last_time < cooldown

    def register_trade(self, mint: str) -> None:
        self.last_trade_time[mint] = now_ts()
        self.record_trade_timestamp()

    def can_buy(self, mint: str) -> Tuple[bool, str]:
        if self.config["runtime"]["kill_switch"]:
            return False, "kill switch"
        if self.in_lockout():
            return False, "daily loss lockout"
        if self.cooldown_active(mint):
            return False, "cooldown"
        if self.max_trades_reached():
            return False, "max trades per hour"
        return True, "ok"

    def update_daily_pnl(self, delta: float) -> None:
        self.daily_pnl += delta
        max_loss = float(self.config["risk"]["max_daily_loss_sol"])
        if self.daily_pnl <= -abs(max_loss):
            self.locked_until = now_ts() + 24 * 3600


class PumpPortalClient:
    def __init__(self, rpc_url: str, wallet: Keypair, config: Dict[str, Any]):
        self.rpc_url = rpc_url
        self.wallet = wallet
        self.config = config

    def build_trade_payload(self, mint: str, side: str, size_sol: float) -> Dict[str, Any]:
        slippage = float(self.config["trade"]["slippage_pct"])
        priority_fee = float(self.config["trade"]["priority_fee"])
        return {
            "publicKey": str(self.wallet.pubkey()),
            "action": side,
            "mint": mint,
            "amount": size_sol,
            "slippage": slippage,
            "priorityFee": priority_fee,
        }

    def trade_local(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        for attempt in range(5):
            try:
                response = requests.post(PUMP_TRADE_LOCAL, json=payload, timeout=10)
                response.raise_for_status()
                return response.json()
            except requests.RequestException as exc:
                LOGGER.warning("trade-local error: %s", exc)
                time.sleep(2 ** attempt)
        raise RuntimeError("trade-local failed after retries")

    def sign_and_send(self, tx_base64: str) -> str:
        raw = base64.b64decode(tx_base64)
        tx = VersionedTransaction.from_bytes(raw)
        signed = tx.sign([self.wallet])
        signed_b64 = base64.b64encode(bytes(signed)).decode("utf-8")
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendTransaction",
            "params": [signed_b64, {"encoding": "base64"}],
        }
        response = requests.post(self.rpc_url, json=payload, timeout=10)
        response.raise_for_status()
        data = response.json()
        if "error" in data:
            raise RuntimeError(data["error"])
        return data.get("result", "")


class TradeBot:
    def __init__(self, config: Dict[str, Any], wallet: Keypair, sim_state: SimState):
        self.config = config
        self.wallet = wallet
        self.sim_state = sim_state
        self.trader = Trader(config, wallet, sim_state)
        self.strategy = StrategyEngine(config)
        self.allowed_users = parse_allowed_users(os.environ.get("ALLOWED_USERS", ""))
        self.trade_alerts = config["runtime"]["trade_alerts"]
        self.active_mint: Optional[str] = config["runtime"].get("active_mint")
        self.websocket_task: Optional[asyncio.Task] = None
        self.price_history: Dict[str, List[float]] = {}
        self.trade_times: Dict[str, List[float]] = {}
        self.app: Optional[Application] = None
        self.pump_client = PumpPortalClient(os.environ["SOLANA_RPC_URL"], wallet, config)

    def mode(self) -> str:
        return "TEST" if self.config["runtime"]["test_mode"] else "LIVE"

    def save_runtime(self) -> None:
        self.config["runtime"]["active_mint"] = self.active_mint
        self.config["runtime"]["trade_alerts"] = self.trade_alerts
        save_config(self.config)

    async def start(self, app: Application) -> None:
        self.app = app
        self.websocket_task = asyncio.create_task(self.websocket_loop())

    async def stop(self) -> None:
        if self.websocket_task:
            self.websocket_task.cancel()
            try:
                await self.websocket_task
            except asyncio.CancelledError:
                pass

    async def websocket_loop(self) -> None:
        backoff = 1
        while True:
            try:
                async with websockets.connect(PUMP_WS_URL, ping_interval=20, ping_timeout=20) as ws:
                    LOGGER.info("Connected to PumpPortal websocket")
                    backoff = 1
                    subscribed: set[str] = set()
                    while True:
                        if self.active_mint and self.active_mint not in subscribed:
                            payload = {
                                "method": "subscribeTokenTrade",
                                "keys": [self.active_mint],
                            }
                            await ws.send(json.dumps(payload))
                            subscribed.add(self.active_mint)
                        try:
                            message = await asyncio.wait_for(ws.recv(), timeout=5)
                        except asyncio.TimeoutError:
                            continue
                        await self.handle_ws_message(message)
            except asyncio.CancelledError:
                return
            except Exception as exc:
                LOGGER.warning("Websocket error: %s", exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def handle_ws_message(self, message: str) -> None:
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return
        mint = data.get("mint")
        price = data.get("price")
        if not mint or price is None:
            return
        try:
            price = float(price)
        except (ValueError, TypeError):
            return
        self.price_history.setdefault(mint, []).append(price)
        self.trade_times.setdefault(mint, []).append(now_ts())
        await self.maybe_trade(mint)

    async def maybe_trade(self, mint: str) -> None:
        if mint != self.active_mint:
            return
        self.trader.reset_daily_if_needed()
        prices = self.price_history.get(mint, [])
        times = self.trade_times.get(mint, [])
        if mint not in self.trader.positions:
            allowed, reason = self.trader.can_buy(mint)
            if not allowed:
                return
            should_enter, rationale = self.strategy.should_enter(prices, times)
            if should_enter:
                await self.execute_buy(mint, prices[-1], rationale)
        else:
            await self.check_exit(mint, prices[-1])

    async def execute_buy(self, mint: str, price: float, rationale: str) -> None:
        size_sol = min(
            float(self.config["risk"]["max_position_sol"]),
            float(self.config["trade"]["default_position_sol"]),
        )
        if self.config["runtime"]["test_mode"]:
            await self.sim_buy(mint, price, size_sol, rationale)
        else:
            await self.live_buy(mint, price, size_sol, rationale)

    async def execute_sell(self, mint: str, price: float, reason: str) -> None:
        if self.config["runtime"]["test_mode"]:
            await self.sim_sell(mint, price, reason)
        else:
            await self.live_sell(mint, price, reason)

    async def sim_buy(self, mint: str, price: float, size_sol: float, rationale: str) -> None:
        if self.sim_state.sol_balance < size_sol:
            return
        tokens = size_sol / price if price > 0 else 0.0
        self.sim_state.sol_balance -= size_sol
        self.sim_state.token_balances[mint] = self.sim_state.token_balances.get(mint, 0.0) + tokens
        position = Position(
            mint=mint,
            entry_price=price,
            size_sol=size_sol,
            tokens=tokens,
            entry_time=now_ts(),
            strategy=self.strategy.active_strategy_name(),
            peak_price=price,
        )
        self.trader.positions[mint] = position
        self.trader.register_trade(mint)
        self.sim_state.trade_log.append(
            {
                "side": "BUY",
                "mint": mint,
                "price": price,
                "size_sol": size_sol,
                "timestamp": now_ts(),
                "mode": "TEST",
            }
        )
        save_sim_state(self.sim_state)
        await self.notify_trade(
            "BUY",
            mint,
            price,
            size_sol,
            rationale,
            signature="SIM",
            strategy=position.strategy,
        )

    async def sim_sell(self, mint: str, price: float, reason: str) -> None:
        position = self.trader.positions.pop(mint, None)
        if not position:
            return
        proceeds = position.tokens * price
        pnl = proceeds - position.size_sol
        self.sim_state.sol_balance += proceeds
        self.sim_state.token_balances[mint] = max(
            0.0, self.sim_state.token_balances.get(mint, 0.0) - position.tokens
        )
        self.sim_state.realized_pnl += pnl
        self.trader.update_daily_pnl(pnl)
        self.trader.register_trade(mint)
        self.sim_state.trade_log.append(
            {
                "side": "SELL",
                "mint": mint,
                "price": price,
                "size_sol": position.size_sol,
                "timestamp": now_ts(),
                "mode": "TEST",
                "pnl": pnl,
            }
        )
        save_sim_state(self.sim_state)
        pnl_pct = pnl / position.size_sol * 100 if position.size_sol else 0.0
        await self.notify_trade(
            "SELL",
            mint,
            price,
            position.size_sol,
            reason,
            signature="SIM",
            entry_price=position.entry_price,
            pnl=pnl,
            pnl_pct=pnl_pct,
        )

    async def live_buy(self, mint: str, price: float, size_sol: float, rationale: str) -> None:
        payload = self.pump_client.build_trade_payload(mint, "buy", size_sol)
        try:
            response = self.pump_client.trade_local(payload)
            signature = self.pump_client.sign_and_send(response["transaction"])
        except Exception as exc:
            LOGGER.error("Live buy failed: %s", exc)
            return
        position = Position(
            mint=mint,
            entry_price=price,
            size_sol=size_sol,
            tokens=size_sol / price if price > 0 else 0.0,
            entry_time=now_ts(),
            strategy=self.strategy.active_strategy_name(),
            peak_price=price,
        )
        self.trader.positions[mint] = position
        self.trader.register_trade(mint)
        await self.notify_trade(
            "BUY",
            mint,
            price,
            size_sol,
            rationale,
            signature=signature,
            strategy=position.strategy,
        )

    async def live_sell(self, mint: str, price: float, reason: str) -> None:
        position = self.trader.positions.pop(mint, None)
        if not position:
            return
        payload = self.pump_client.build_trade_payload(mint, "sell", position.size_sol)
        try:
            response = self.pump_client.trade_local(payload)
            signature = self.pump_client.sign_and_send(response["transaction"])
        except Exception as exc:
            LOGGER.error("Live sell failed: %s", exc)
            return
        pnl = (price - position.entry_price) * position.tokens
        self.trader.update_daily_pnl(pnl)
        self.trader.register_trade(mint)
        pnl_pct = pnl / position.size_sol * 100 if position.size_sol else 0.0
        await self.notify_trade(
            "SELL",
            mint,
            price,
            position.size_sol,
            reason,
            signature=signature,
            entry_price=position.entry_price,
            pnl=pnl,
            pnl_pct=pnl_pct,
        )

    async def check_exit(self, mint: str, price: float) -> None:
        position = self.trader.positions.get(mint)
        if not position:
            return
        if price > position.peak_price:
            position.peak_price = price
        exits = self.strategy.global_exits()
        take_profit = float(exits.get("take_profit_pct", 0))
        stop_loss = float(exits.get("stop_loss_pct", 0))
        trailing_stop = exits.get("trailing_stop_pct")
        pnl_pct = (price - position.entry_price) / position.entry_price * 100

        if take_profit and pnl_pct >= take_profit:
            await self.execute_sell(mint, price, "TP")
            return
        if stop_loss and pnl_pct <= -abs(stop_loss):
            await self.execute_sell(mint, price, "SL")
            return
        if trailing_stop:
            trail_price = position.peak_price * (1 - float(trailing_stop) / 100)
            if price <= trail_price:
                await self.execute_sell(mint, price, "TRAIL")
                return
        if self.strategy.active_strategy_name() == "timebox_v1":
            params = self.strategy.active_strategy_params()
            max_hold = int(params.get("max_hold_seconds", 300))
            if now_ts() - position.entry_time >= max_hold:
                await self.execute_sell(mint, price, "TIMEBOX")

    async def notify_trade(
        self,
        side: str,
        mint: str,
        price: float,
        size_sol: float,
        reason: str,
        signature: str,
        strategy: Optional[str] = None,
        entry_price: Optional[float] = None,
        pnl: Optional[float] = None,
        pnl_pct: Optional[float] = None,
    ) -> None:
        if not self.trade_alerts or not self.app:
            return
        mode = self.mode()
        message = [f"*{side}* ({mode})", f"Mint: `{mint}`"]
        if strategy:
            message.append(f"Strategy: `{strategy}`")
        message.append(f"Reason: {reason}")
        message.append(f"Price: {price:.8f}")
        message.append(f"Size: {size_sol:.4f} SOL")
        if entry_price is not None and pnl is not None and pnl_pct is not None:
            message.append(f"Entry: {entry_price:.8f}")
            message.append(f"P&L: {pnl:.4f} SOL ({pnl_pct:.2f}%)")
            message.append(f"Daily P&L est: {self.trader.daily_pnl:.4f} SOL")
        message.append(f"Pump.fun: {pump_fun_url(mint)}")
        message.append(f"Signature: `{signature}`")
        text = "\n".join(message)
        for user_id in self.allowed_users:
            try:
                await self.app.bot.send_message(
                    chat_id=user_id,
                    text=text,
                    parse_mode=ParseMode.MARKDOWN,
                    disable_web_page_preview=True,
                )
            except Exception as exc:
                LOGGER.warning("Failed to send trade alert: %s", exc)


async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    bot: TradeBot = context.bot_data["bot"]
    if not check_allowed(update, bot.allowed_users):
        return
    keyboard = [
        [InlineKeyboardButton("Start Trading", callback_data="start_trading")],
        [InlineKeyboardButton("Stop Trading", callback_data="stop_trading")],
        [InlineKeyboardButton("Status / P&L", callback_data="status")],
        [InlineKeyboardButton("Deposit Address", callback_data="deposit")],
        [InlineKeyboardButton(f"Toggle TEST/LIVE ({bot.mode()})", callback_data="toggle_mode")],
        [InlineKeyboardButton(f"Kill Switch ({format_bool(bot.config['runtime']['kill_switch'])})", callback_data="toggle_kill")],
        [InlineKeyboardButton(f"Trade Alerts ({format_bool(bot.trade_alerts)})", callback_data="toggle_alerts")],
        [InlineKeyboardButton("Strategy Menu", callback_data="strategy")],
    ]
    if bot.config["runtime"]["test_mode"]:
        keyboard.append([InlineKeyboardButton("Sim Deposit SOL", callback_data="sim_deposit")])
        keyboard.append([InlineKeyboardButton("Sim Reset", callback_data="sim_reset")])
    reply = InlineKeyboardMarkup(keyboard)
    if update.message:
        await update.message.reply_text("Menu:", reply_markup=reply)
    elif update.callback_query:
        await update.callback_query.message.reply_text("Menu:", reply_markup=reply)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await menu(update, context)


async def handle_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    bot: TradeBot = context.bot_data["bot"]
    query = update.callback_query
    await query.answer()
    if not check_allowed(update, bot.allowed_users):
        return ConversationHandler.END

    if query.data == "start_trading":
        await query.message.reply_text("Enter coin alias or mint:")
        return STATE_MINT

    if query.data == "stop_trading":
        bot.active_mint = None
        bot.trader.positions.clear()
        bot.save_runtime()
        await query.message.reply_text("Trading stopped.")
        return ConversationHandler.END

    if query.data == "status":
        position = bot.trader.positions.get(bot.active_mint or "")
        status_lines = [
            f"Mode: {bot.mode()}",
            f"Active mint: {bot.active_mint or 'None'}",
            f"Kill switch: {format_bool(bot.config['runtime']['kill_switch'])}",
            f"Daily P&L est: {bot.trader.daily_pnl:.4f} SOL",
        ]
        if bot.config["runtime"]["test_mode"]:
            status_lines.append(f"Sim balance: {bot.sim_state.sol_balance:.4f} SOL")
            status_lines.append(f"Sim realized P&L: {bot.sim_state.realized_pnl:.4f} SOL")
        if position:
            status_lines.append(
                f"Open position: {position.size_sol:.4f} SOL @ {position.entry_price:.8f}"
            )
        await query.message.reply_text("\n".join(status_lines))
        return ConversationHandler.END

    if query.data == "deposit":
        await query.message.reply_text(
            f"Deposit address:\n`{bot.wallet.pubkey()}`",
            parse_mode=ParseMode.MARKDOWN,
        )
        return ConversationHandler.END

    if query.data == "toggle_mode":
        bot.config["runtime"]["test_mode"] = not bot.config["runtime"]["test_mode"]
        save_config(bot.config)
        await query.message.reply_text(f"Mode is now {bot.mode()}.")
        return ConversationHandler.END

    if query.data == "toggle_kill":
        bot.config["runtime"]["kill_switch"] = not bot.config["runtime"]["kill_switch"]
        save_config(bot.config)
        await query.message.reply_text(
            f"Kill switch: {format_bool(bot.config['runtime']['kill_switch'])}"
        )
        return ConversationHandler.END

    if query.data == "toggle_alerts":
        bot.trade_alerts = not bot.trade_alerts
        bot.save_runtime()
        await query.message.reply_text(f"Trade alerts: {format_bool(bot.trade_alerts)}")
        return ConversationHandler.END

    if query.data == "strategy":
        strategies = bot.config["strategy"]["strategies"]
        active = bot.strategy.active_strategy_name()
        text = [f"Active strategy: {active}", "Pick a strategy or edit params:"]
        keyboard = []
        for name in strategies:
            keyboard.append([InlineKeyboardButton(name, callback_data=f"strategy_set:{name}")])
        keyboard.append([InlineKeyboardButton("Edit Active Params", callback_data="strategy_edit")])
        keyboard.append([InlineKeyboardButton("Edit Global Exits", callback_data="global_edit")])
        await query.message.reply_text(
            "\n".join(text),
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return ConversationHandler.END

    if query.data == "sim_deposit":
        bot.sim_state.sol_balance += float(bot.config["trade"]["sim_deposit_amount"])
        save_sim_state(bot.sim_state)
        await query.message.reply_text("Sim deposit completed.")
        return ConversationHandler.END

    if query.data == "sim_reset":
        bot.sim_state.sol_balance = 0.0
        bot.sim_state.token_balances = {}
        bot.sim_state.trade_log = []
        bot.sim_state.realized_pnl = 0.0
        save_sim_state(bot.sim_state)
        await query.message.reply_text("Sim state reset.")
        return ConversationHandler.END

    if query.data and query.data.startswith("strategy_set:"):
        name = query.data.split(":", 1)[1]
        bot.config["strategy"]["active"] = name
        save_config(bot.config)
        await query.message.reply_text(f"Active strategy set to {name}")
        return ConversationHandler.END

    if query.data == "strategy_edit":
        params = bot.strategy.active_strategy_params()
        await query.message.reply_text(
            "Edit active strategy params with `key=value` (e.g. lookback=10).\n"
            f"Current: {params}",
            parse_mode=ParseMode.MARKDOWN,
        )
        return STATE_EDIT

    if query.data == "global_edit":
        exits = bot.strategy.global_exits()
        await query.message.reply_text(
            "Edit global exits with `key=value` (e.g. take_profit_pct=12).\n"
            f"Current: {exits}",
            parse_mode=ParseMode.MARKDOWN,
        )
        return STATE_EDIT_GLOBAL

    return ConversationHandler.END


async def handle_mint(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    bot: TradeBot = context.bot_data["bot"]
    if not check_allowed(update, bot.allowed_users):
        return ConversationHandler.END
    mint_input = update.message.text.strip().lower()
    aliases = bot.config["aliases"]
    mint = aliases.get(mint_input, mint_input)
    bot.active_mint = mint
    bot.save_runtime()
    await update.message.reply_text(
        f"Mint: `{mint}`\nPump.fun: {pump_fun_url(mint)}\nConfirm YES/NO",
        parse_mode=ParseMode.MARKDOWN,
    )
    return STATE_CONFIRM


async def handle_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    bot: TradeBot = context.bot_data["bot"]
    if not check_allowed(update, bot.allowed_users):
        return ConversationHandler.END
    text = update.message.text.strip().lower()
    if text in {"yes", "y"}:
        await update.message.reply_text("Trading enabled for mint.")
    else:
        bot.active_mint = None
        bot.save_runtime()
        await update.message.reply_text("Cancelled.")
    return ConversationHandler.END


async def handle_edit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    bot: TradeBot = context.bot_data["bot"]
    if not check_allowed(update, bot.allowed_users):
        return ConversationHandler.END
    entry = update.message.text.strip()
    if "=" not in entry:
        await update.message.reply_text("Invalid format. Use key=value.")
        return STATE_EDIT
    key, value = [item.strip() for item in entry.split("=", 1)]
    params = bot.strategy.active_strategy_params()
    if key not in params:
        await update.message.reply_text("Unknown key.")
        return STATE_EDIT
    updated = cast_value(params[key], value)
    if updated is None:
        await update.message.reply_text("Invalid value type.")
        return STATE_EDIT
    params[key] = updated
    save_config(bot.config)
    await update.message.reply_text(f"Updated {key} to {updated}")
    return ConversationHandler.END


async def handle_edit_global(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    bot: TradeBot = context.bot_data["bot"]
    if not check_allowed(update, bot.allowed_users):
        return ConversationHandler.END
    entry = update.message.text.strip()
    if "=" not in entry:
        await update.message.reply_text("Invalid format. Use key=value.")
        return STATE_EDIT_GLOBAL
    key, value = [item.strip() for item in entry.split("=", 1)]
    exits = bot.strategy.global_exits()
    if key not in exits:
        await update.message.reply_text("Unknown key.")
        return STATE_EDIT_GLOBAL
    updated = cast_value(exits[key], value)
    if updated is None:
        await update.message.reply_text("Invalid value type.")
        return STATE_EDIT_GLOBAL
    exits[key] = updated
    save_config(bot.config)
    await update.message.reply_text(f"Updated {key} to {updated}")
    return ConversationHandler.END


def cast_value(original: Any, value: str) -> Optional[Any]:
    if original is None:
        if value.lower() in {"none", "null"}:
            return None
        return to_float(value)
    if isinstance(original, bool):
        if value.lower() in {"true", "yes", "1"}:
            return True
        if value.lower() in {"false", "no", "0"}:
            return False
        return None
    if isinstance(original, int):
        return to_int(value)
    if isinstance(original, float):
        return to_float(value)
    return value


async def self_check() -> None:
    load_env()
    missing = [
        key
        for key in ["TELEGRAM_BOT_TOKEN", "ALLOWED_USERS", "SOLANA_RPC_URL"]
        if not os.environ.get(key)
    ]
    if missing:
        raise RuntimeError(f"Missing env values: {missing}")
    config = load_config()
    wallet = load_wallet()
    sim_state = load_sim_state()
    sim_state.sol_balance += 1.0
    save_sim_state(sim_state)
    bot = TradeBot(config, wallet, sim_state)
    await bot.sim_buy("SIM_MINT", 1.0, 0.1, "self-check")
    await bot.sim_sell("SIM_MINT", 1.1, "self-check")
    LOGGER.info("Self-check passed.")


def build_application(bot: TradeBot) -> Application:
    application = Application.builder().token(os.environ["TELEGRAM_BOT_TOKEN"]).build()

    conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(handle_menu)],
        states={
            STATE_MINT: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_mint)],
            STATE_CONFIRM: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_confirm)],
            STATE_EDIT: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_edit)],
            STATE_EDIT_GLOBAL: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_edit_global)],
        },
        fallbacks=[CommandHandler("menu", menu)],
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("menu", menu))
    application.add_handler(conv)
    application.add_handler(CallbackQueryHandler(handle_menu))
    application.bot_data["bot"] = bot
    return application


async def on_startup(app: Application) -> None:
    bot: TradeBot = app.bot_data["bot"]
    await bot.start(app)


async def on_shutdown(app: Application) -> None:
    bot: TradeBot = app.bot_data["bot"]
    await bot.stop()


def main() -> None:
    load_env()
    config = load_config()
    wallet = load_wallet()
    sim_state = load_sim_state()
    bot = TradeBot(config, wallet, sim_state)

    if "--self-check" in os.sys.argv:
        asyncio.run(self_check())
        return

    application = build_application(bot)
    application.post_init = on_startup
    application.post_shutdown = on_shutdown

    loop = asyncio.get_event_loop()
    stop_event = asyncio.Event()

    def handle_signal(*_: object) -> None:
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_signal)
        except NotImplementedError:
            signal.signal(sig, lambda *_: stop_event.set())

    async def runner() -> None:
        await application.initialize()
        await application.start()
        await application.updater.start_polling()
        await stop_event.wait()
        await application.updater.stop()
        await application.stop()
        await application.shutdown()

    loop.run_until_complete(runner())


if __name__ == "__main__":
    main()
'''


def prompt(value: str, default: str | None = None) -> str:
    if default is None:
        return input(f"{value}: ").strip()
    return input(f"{value} [{default}]: ").strip() or default


def make_project_structure(base_path: Path) -> None:
    base_path.mkdir(parents=True, exist_ok=True)


def create_venv(base_path: Path) -> Path:
    venv_path = base_path / "venv"
    subprocess.check_call([sys.executable, "-m", "venv", str(venv_path)])
    return venv_path


def venv_python(venv_path: Path) -> Path:
    if platform.system() == "Windows":
        return venv_path / "Scripts" / "python.exe"
    return venv_path / "bin" / "python"


def venv_pip(venv_path: Path) -> Path:
    if platform.system() == "Windows":
        return venv_path / "Scripts" / "pip.exe"
    return venv_path / "bin" / "pip"


def install_deps(venv_path: Path) -> None:
    pip_path = venv_pip(venv_path)
    subprocess.check_call([str(pip_path), "install", "--upgrade", "pip"])
    subprocess.check_call(
        [
            str(pip_path),
            "install",
            "python-telegram-bot>=21.0",
            "requests",
            "python-dotenv",
            "websockets",
            "solders>=0.20.0",
        ]
    )


def write_file(path: Path, content: str, mode: int | None = None) -> None:
    path.write_text(content, encoding="utf-8")
    if mode is not None:
        try:
            os.chmod(path, mode)
        except PermissionError:
            print(f"Warning: could not set permissions on {path}")


def generate_wallet(path: Path) -> None:
    from solders.keypair import Keypair

    keypair = Keypair()
    secret = list(bytes(keypair))
    write_file(path, json.dumps(secret, indent=2), mode=0o600)


def default_config(test_mode: bool) -> Dict[str, Any]:
    return {
        "aliases": {
            "btc": "So11111111111111111111111111111111111111112",
            "sol": "So11111111111111111111111111111111111111112",
        },
        "risk": {
            "min_seconds_between_trades": 30,
            "max_trades_per_hour": 10,
            "max_position_sol": 0.2,
            "max_daily_loss_sol": 1.0,
        },
        "trade": {
            "slippage_pct": 5.0,
            "priority_fee": 0.00005,
            "default_position_sol": 0.1,
            "sim_deposit_amount": 1.0,
        },
        "strategy": {
            "active": "momentum_v1",
            "global_exits": {
                "take_profit_pct": 10.0,
                "stop_loss_pct": 5.0,
                "trailing_stop_pct": None,
            },
            "strategies": {
                "momentum_v1": {
                    "lookback": 5,
                    "min_momentum_pct": 2.0,
                    "min_trades_in_window": 3,
                    "window_seconds": 60,
                },
                "breakout_v1": {
                    "lookback": 10,
                    "breakout_pct": 1.5,
                    "min_trades_in_window": 3,
                    "window_seconds": 60,
                },
                "mean_reversion_v1": {
                    "lookback": 20,
                    "zscore_entry": -1.5,
                    "min_trades_in_window": 3,
                    "window_seconds": 60,
                },
                "timebox_v1": {
                    "base_strategy": "momentum_v1",
                    "max_hold_seconds": 300,
                    "min_trades_in_window": 3,
                    "window_seconds": 60,
                },
            },
        },
        "runtime": {
            "test_mode": test_mode,
            "kill_switch": False,
            "trade_alerts": True,
            "active_mint": None,
        },
    }


def write_env(base_path: Path, values: Dict[str, str]) -> None:
    env_lines = [f"{key}={values[key]}" for key in values]
    write_file(base_path / ".env", "\n".join(env_lines) + "\n")


def write_sim_state(base_path: Path) -> None:
    state = {
        "sol_balance": 0.0,
        "token_balances": {},
        "trade_log": [],
        "realized_pnl": 0.0,
    }
    write_file(base_path / "sim_state.json", json.dumps(state, indent=2))


def build_bot_py(base_path: Path) -> None:
    write_file(base_path / "bot.py", BOT_TEMPLATE)


def installer() -> None:
    print("Telegram Trading Bot Installer")
    project_name = prompt("Project folder name", "telegram-trader")
    telegram_token = prompt("Telegram bot token")
    allowed_users = prompt("Allowed Telegram user IDs (comma-separated)")
    rpc_url = prompt("Solana RPC URL", "https://api.mainnet-beta.solana.com")
    test_mode_input = prompt("Default TEST mode? (true/false)", "true")
    slippage = prompt("Slippage %", "5")
    priority_fee = prompt("Priority fee", "0.00005")

    project_path = Path.cwd() / project_name
    if project_path.exists() and any(project_path.iterdir()):
        print("Project folder exists and is not empty. Aborting.")
        sys.exit(1)

    make_project_structure(project_path)
    venv_path = create_venv(project_path)
    install_deps(venv_path)

    env_values = {
        "TELEGRAM_BOT_TOKEN": telegram_token,
        "ALLOWED_USERS": allowed_users,
        "SOLANA_RPC_URL": rpc_url,
    }
    write_env(project_path, env_values)

    config = default_config(test_mode_input.lower() in {"true", "yes", "1"})
    config["trade"]["slippage_pct"] = float(slippage)
    config["trade"]["priority_fee"] = float(priority_fee)
    write_file(project_path / "config.json", json.dumps(config, indent=2))

    wallet_path = project_path / "wallet.json"
    if not wallet_path.exists():
        generate_wallet(wallet_path)

    write_sim_state(project_path)
    build_bot_py(project_path)

    run_bot = prompt("Run bot now? (y/n)", "n")
    if run_bot.lower() in {"y", "yes"}:
        python_path = venv_python(venv_path)
        subprocess.check_call([str(python_path), "bot.py"], cwd=str(project_path))

    print("Install complete.")


if __name__ == "__main__":
    installer()
