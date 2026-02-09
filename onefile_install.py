#!/usr/bin/env python3
"""Single-file installer for a local-only Solana pump.fun trading bot.

This script creates a project directory with a virtual environment, installs
required dependencies, writes configuration files, and generates a fully
functional Telegram bot implementation.
"""

from __future__ import annotations

import json
import os
import platform
import subprocess
import sys
import textwrap
from datetime import datetime, timezone
from pathlib import Path

BOT_SOURCE = r'''#!/usr/bin/env python3
"""Telegram-controlled Solana trading bot (local-only).

DISCLAIMER: This software is for educational purposes only and is NOT
financial advice. Trading crypto involves significant risk. You are solely
responsible for any losses incurred. This bot includes rate-limits and is not
intended for high-frequency trading.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import dataclasses
import json
import logging
import os
import random
import sys
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Tuple

import requests
import websockets
from dotenv import load_dotenv
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction
from solana.rpc.async_api import AsyncClient
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

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"
SIM_STATE_PATH = BASE_DIR / "sim_state.json"
WALLET_PATH = BASE_DIR / "wallet.json"
ENV_PATH = BASE_DIR / ".env"

PUMPPORTAL_WS = "wss://pumpportal.fun/api/data"
PUMPPORTAL_TRADE_LOCAL = "https://pumpportal.fun/api/trade-local"

MENU, START_MINT, START_CONFIRM, EDIT_PARAM, EDIT_VALUE = range(5)

logger = logging.getLogger("bot")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class Position:
    mint: str
    entry_price: float
    size_sol: float
    entry_time: float
    high_water: float
    strategy: str


@dataclass
class SimState:
    sol_balance: float
    token_balances: Dict[str, float]
    trade_log: List[Dict[str, Any]]
    realized_pnl_sol: float
    daily_pnl_sol: float
    last_reset_date: str

    @classmethod
    def load(cls) -> "SimState":
        if not SIM_STATE_PATH.exists():
            return cls(sol_balance=0.0, token_balances={}, trade_log=[], realized_pnl_sol=0.0, daily_pnl_sol=0.0, last_reset_date=utc_now().date().isoformat())
        data = json.loads(SIM_STATE_PATH.read_text())
        return cls(**data)

    def save(self) -> None:
        SIM_STATE_PATH.write_text(json.dumps(dataclasses.asdict(self), indent=2))

    def reset_daily(self) -> None:
        today = utc_now().date().isoformat()
        if self.last_reset_date != today:
            self.daily_pnl_sol = 0.0
            self.last_reset_date = today


def load_config() -> Dict[str, Any]:
    if not CONFIG_PATH.exists():
        raise RuntimeError("config.json missing")
    return json.loads(CONFIG_PATH.read_text())


def save_config(config: Dict[str, Any]) -> None:
    CONFIG_PATH.write_text(json.dumps(config, indent=2))


def update_env_key(key: str, value: str) -> None:
    lines = []
    if ENV_PATH.exists():
        lines = ENV_PATH.read_text().splitlines()
    updated = False
    for idx, line in enumerate(lines):
        if line.startswith(f"{key}="):
            lines[idx] = f"{key}={value}"
            updated = True
            break
    if not updated:
        lines.append(f"{key}={value}")
    ENV_PATH.write_text("\n".join(lines) + "\n")


def load_wallet() -> Keypair:
    if Keypair is None:
        raise RuntimeError("solders not available")
    data = json.loads(WALLET_PATH.read_text())
    secret = bytes(data["secret_key"])
    return Keypair.from_bytes(secret)


def get_pumpfun_url(mint: str) -> str:
    return f"https://pump.fun/{mint}"


def parse_allowed_users(raw: str) -> List[int]:
    return [int(val.strip()) for val in raw.split(",") if val.strip()]


def is_allowed(update: Update, allowed: List[int]) -> bool:
    if update.effective_user is None:
        return False
    return update.effective_user.id in allowed


def safe_float(value: str) -> Optional[float]:
    try:
        return float(value)
    except ValueError:
        return None


def safe_int(value: str) -> Optional[int]:
    try:
        return int(value)
    except ValueError:
        return None


def format_pct(value: float) -> str:
    return f"{value * 100:.2f}%"


class PumpPortalClient:
    def __init__(self, mint: str) -> None:
        self.mint = mint
        self.last_price: Optional[float] = None
        self.trades: Deque[Tuple[float, float]] = deque(maxlen=500)
        self._stop = asyncio.Event()

    async def run(self) -> None:
        backoff = 1
        while not self._stop.is_set():
            try:
                async with websockets.connect(PUMPPORTAL_WS, ping_interval=20) as ws:
                    await ws.send(json.dumps({"method": "subscribeTokenTrade", "keys": [self.mint]}))
                    backoff = 1
                    while not self._stop.is_set():
                        msg = await ws.recv()
                        data = json.loads(msg)
                        price = float(data.get("price", 0))
                        ts = float(data.get("timestamp", time.time()))
                        if price > 0:
                            self.last_price = price
                            self.trades.append((ts, price))
            except Exception as exc:
                logger.warning("WS error for %s: %s", self.mint, exc)
                await asyncio.sleep(min(backoff, 30))
                backoff *= 2

    def stop(self) -> None:
        self._stop.set()

    def recent_prices(self, window_seconds: int) -> List[float]:
        cutoff = time.time() - window_seconds
        return [price for ts, price in self.trades if ts >= cutoff]


class Trader:
    def __init__(self, config: Dict[str, Any], sim_state: SimState, bot_app: Application) -> None:
        self.config = config
        self.sim_state = sim_state
        self.bot_app = bot_app
        self.positions: Dict[str, Position] = {}
        self.kill_switch = bool(config["risk"].get("kill_switch", False))
        self.trade_alerts = bool(config["risk"].get("trade_alerts", True))
        self.test_mode = bool(config["runtime"].get("test_mode", True))
        self.tracked: Dict[str, PumpPortalClient] = {}
        self.trade_times: Deque[float] = deque(maxlen=500)
        self.lockout_until: Optional[datetime] = None

    async def ensure_feed(self, mint: str) -> None:
        if mint in self.tracked:
            return
        client = PumpPortalClient(mint)
        self.tracked[mint] = client
        asyncio.create_task(client.run())

    async def stop_all(self) -> None:
        for client in self.tracked.values():
            client.stop()
        self.tracked = {}

    def _check_daily_lockout(self) -> bool:
        if self.lockout_until and utc_now() < self.lockout_until:
            return True
        return False

    def _register_trade(self) -> None:
        self.trade_times.append(time.time())

    def _trades_last_hour(self) -> int:
        cutoff = time.time() - 3600
        return len([t for t in self.trade_times if t >= cutoff])

    def _cooldown_ok(self) -> bool:
        cooldown = self.config["risk"]["min_seconds_between_trades"]
        if not self.trade_times:
            return True
        return time.time() - self.trade_times[-1] >= cooldown

    def _risk_ok(self, size_sol: float) -> Tuple[bool, str]:
        risk = self.config["risk"]
        self.sim_state.reset_daily()
        if self.kill_switch:
            return False, "Kill switch enabled"
        if self._check_daily_lockout():
            return False, "Daily loss lockout"
        if not self._cooldown_ok():
            return False, "Cooldown active"
        if self._trades_last_hour() >= risk["max_trades_per_hour"]:
            return False, "Trade/hour limit"
        if size_sol > risk["max_position_sol"]:
            return False, "Max position size"
        if self.sim_state.daily_pnl_sol <= -risk["max_daily_loss_sol"]:
            self.lockout_until = utc_now() + timedelta(hours=24)
            return False, "Max daily loss reached"
        return True, "OK"

    def _update_pnl(self, pnl_sol: float) -> None:
        self.sim_state.realized_pnl_sol += pnl_sol
        self.sim_state.daily_pnl_sol += pnl_sol
        self.sim_state.save()

    async def _notify(self, text: str) -> None:
        if not self.trade_alerts:
            return
        allowed = parse_allowed_users(os.getenv("ALLOWED_USER_IDS", ""))
        for user_id in allowed:
            try:
                await self.bot_app.bot.send_message(chat_id=user_id, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            except Exception as exc:
                logger.warning("Notify failed: %s", exc)

    async def buy(self, mint: str, price: float, reason: str, strategy_name: str) -> None:
        size_sol = self.config["risk"]["max_position_sol"]
        ok, msg = self._risk_ok(size_sol)
        if not ok:
            logger.info("Buy blocked: %s", msg)
            return
        if mint in self.positions:
            return
        signature = "SIM"
        if self.test_mode:
            if self.sim_state.sol_balance < size_sol:
                logger.info("Insufficient sim balance")
                return
            self.sim_state.sol_balance -= size_sol
            self.sim_state.token_balances[mint] = self.sim_state.token_balances.get(mint, 0.0) + (size_sol / price)
            self.sim_state.trade_log.append({"side": "BUY", "mint": mint, "price": price, "size_sol": size_sol, "time": time.time()})
            self.sim_state.save()
        else:
            signature = await self._send_live_trade(mint, size_sol, "buy")
        self.positions[mint] = Position(mint=mint, entry_price=price, size_sol=size_sol, entry_time=time.time(), high_water=price, strategy=strategy_name)
        self._register_trade()
        text = (
            f"<b>BUY</b>\n"
            f"Mode: {'TEST' if self.test_mode else 'LIVE'}\n"
            f"Mint: {mint}\n"
            f"Strategy: {strategy_name}\n"
            f"Reason: {reason}\n"
            f"Price: {price:.8f}\n"
            f"Size (SOL): {size_sol:.4f}\n"
            f"URL: {get_pumpfun_url(mint)}\n"
            f"Signature: {signature}"
        )
        await self._notify(text)

    async def sell(self, mint: str, price: float, reason: str) -> None:
        position = self.positions.get(mint)
        if not position:
            return
        entry_price = position.entry_price
        size_sol = position.size_sol
        pnl_sol = size_sol * ((price - entry_price) / entry_price)
        pnl_pct = (price - entry_price) / entry_price
        signature = "SIM"
        if self.test_mode:
            self.sim_state.sol_balance += size_sol + pnl_sol
            self.sim_state.token_balances[mint] = max(0.0, self.sim_state.token_balances.get(mint, 0.0) - (size_sol / entry_price))
            self.sim_state.trade_log.append({"side": "SELL", "mint": mint, "price": price, "size_sol": size_sol, "time": time.time(), "pnl_sol": pnl_sol})
            self._update_pnl(pnl_sol)
        else:
            signature = await self._send_live_trade(mint, size_sol, "sell")
        self.positions.pop(mint, None)
        text = (
            f"<b>SELL</b>\n"
            f"Mode: {'TEST' if self.test_mode else 'LIVE'}\n"
            f"Mint: {mint}\n"
            f"Exit Reason: {reason}\n"
            f"Entry: {entry_price:.8f}\n"
            f"Exit: {price:.8f}\n"
            f"P&L: {pnl_sol:.4f} SOL ({format_pct(pnl_pct)})\n"
            f"Daily P&L: {self.sim_state.daily_pnl_sol:.4f} SOL\n"
            f"Signature: {signature}"
        )
        await self._notify(text)

    async def _send_live_trade(self, mint: str, size_sol: float, side: str) -> str:
        payload = {
            "mint": mint,
            "inAmount": size_sol,
            "slippage": self.config["runtime"]["slippage_pct"],
            "priorityFee": self.config["runtime"]["priority_fee"],
            "direction": side,
        }
        try:
            response = requests.post(PUMPPORTAL_TRADE_LOCAL, json=payload, timeout=20)
            response.raise_for_status()
            tx_b64 = response.json().get("transaction")
            if not tx_b64:
                raise RuntimeError("No transaction in response")
            if VersionedTransaction is None or AsyncClient is None:
                raise RuntimeError("Solana libraries missing")
            raw = base64.b64decode(tx_b64)
            tx = VersionedTransaction.from_bytes(raw)
            wallet = load_wallet()
            signed = VersionedTransaction.populate(tx.message, [wallet])
            signed_b64 = base64.b64encode(bytes(signed)).decode()
            async with AsyncClient(os.getenv("SOLANA_RPC_URL")) as client:
                result = await client.send_transaction(signed)
            return getattr(result, "value", None) or str(result)
        except Exception as exc:
            logger.error("Live trade failed: %s", exc)
            return "ERROR"

    def update_runtime(self, *, test_mode: Optional[bool] = None, trade_alerts: Optional[bool] = None, kill_switch: Optional[bool] = None) -> None:
        if test_mode is not None:
            self.test_mode = test_mode
            self.config["runtime"]["test_mode"] = test_mode
        if trade_alerts is not None:
            self.trade_alerts = trade_alerts
            self.config["risk"]["trade_alerts"] = trade_alerts
        if kill_switch is not None:
            self.kill_switch = kill_switch
            self.config["risk"]["kill_switch"] = kill_switch
        save_config(self.config)


class StrategyEngine:
    def __init__(self, config: Dict[str, Any], trader: Trader) -> None:
        self.config = config
        self.trader = trader

    def _momentum(self, prices: List[float]) -> float:
        if len(prices) < 2:
            return 0.0
        return (prices[-1] - prices[0]) / prices[0]

    def should_enter(self, mint: str, trades: List[Tuple[float, float]]) -> Tuple[bool, str, str]:
        active = self.config["strategies"]["active"]
        params = self.config["strategies"][active]
        strategy_name = active
        if active == "timebox_v1":
            base = params.get("base_strategy", "momentum_v1")
            params = self.config["strategies"][base]
            strategy_name = "timebox_v1"
        window = params.get("lookback_seconds", 30)
        cutoff = time.time() - window
        prices = [price for ts, price in trades if ts >= cutoff]
        min_trades = params.get("min_trades_in_window", 3)
        if len(prices) < min_trades:
            return False, "Not enough trades", strategy_name
        if active == "momentum_v1":
            mom = self._momentum(prices)
            if mom >= params["min_momentum_pct"]:
                return True, f"Momentum {mom:.2%}", strategy_name
        if active == "breakout_v1":
            if len(prices) > 1:
                high = max(prices[:-1])
                if prices[-1] >= high * (1 + params["breakout_pct"]):
                    return True, "Breakout", strategy_name
        if active == "mean_reversion_v1":
            if prices:
                mean = sum(prices) / len(prices)
                std = (sum((p - mean) ** 2 for p in prices) / len(prices)) ** 0.5
                z = (prices[-1] - mean) / std if std else 0
                if z <= params["zscore_entry"]:
                    return True, f"Z-score {z:.2f}", strategy_name
        return False, "No signal", strategy_name

    def should_exit(self, position: Position, price: float) -> Tuple[bool, str]:
        exits = self.config["risk"]["exits"]
        pnl_pct = (price - position.entry_price) / position.entry_price
        if pnl_pct >= exits["take_profit_pct"]:
            return True, "TP"
        if pnl_pct <= -exits["stop_loss_pct"]:
            return True, "SL"
        trailing = exits.get("trailing_stop_pct")
        if trailing is not None:
            if price > position.high_water:
                position.high_water = price
            if price <= position.high_water * (1 - trailing):
                return True, "Trailing"
        if position.strategy == "timebox_v1":
            max_hold = self.config["strategies"]["timebox_v1"]["max_hold_seconds"]
            if time.time() - position.entry_time >= max_hold:
                return True, "Timebox"
        return False, "Hold"


async def trading_loop(config: Dict[str, Any], trader: Trader) -> None:
    engine = StrategyEngine(config, trader)
    while True:
        try:
            for mint, client in trader.tracked.items():
                price = client.last_price
                if price is None:
                    continue
                position = trader.positions.get(mint)
                if position:
                    exit_now, reason = engine.should_exit(position, price)
                    if exit_now:
                        await trader.sell(mint, price, reason)
                else:
                    enter, reason, strategy_name = engine.should_enter(mint, list(client.trades))
                    if enter:
                        await trader.buy(mint, price, reason, strategy_name)
        except Exception as exc:
            logger.exception("Trading loop error: %s", exc)
        await asyncio.sleep(2)


async def build_menu(config: Dict[str, Any]) -> InlineKeyboardMarkup:
    test_mode = config["runtime"]["test_mode"]
    buttons = [
        [InlineKeyboardButton("Start Trading (guided)", callback_data="start_trading")],
        [InlineKeyboardButton("Stop Trading (all)", callback_data="stop_trading")],
        [InlineKeyboardButton("Status / P&L", callback_data="status")],
        [InlineKeyboardButton("Deposit Address", callback_data="deposit")],
        [InlineKeyboardButton(f"Toggle TEST/LIVE (now {'TEST' if test_mode else 'LIVE'})", callback_data="toggle_mode")],
        [InlineKeyboardButton("Kill Switch toggle", callback_data="toggle_kill")],
        [InlineKeyboardButton("Toggle Trade Alerts", callback_data="toggle_alerts")],
        [InlineKeyboardButton("Strategy menu", callback_data="strategy_menu")],
    ]
    if test_mode:
        buttons.append([InlineKeyboardButton("Sim Deposit SOL", callback_data="sim_deposit")])
        buttons.append([InlineKeyboardButton("Sim Reset", callback_data="sim_reset")])
    return InlineKeyboardMarkup(buttons)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    config = context.application.bot_data["config"]
    allowed = context.application.bot_data["allowed_users"]
    if not is_allowed(update, allowed):
        return ConversationHandler.END
    menu = await build_menu(config)
    await update.message.reply_text("Trading Bot Menu", reply_markup=menu)
    return MENU


async def handle_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    config = context.application.bot_data["config"]
    trader: Trader = context.application.bot_data["trader"]
    if query.data == "start_trading":
        await query.message.reply_text("Enter coin alias or mint:")
        return START_MINT
    if query.data == "stop_trading":
        await trader.stop_all()
        await query.message.reply_text("Stopped all trading feeds.")
        return MENU
    if query.data == "status":
        sim_state = trader.sim_state
        status = (
            f"Mode: {'TEST' if trader.test_mode else 'LIVE'}\n"
            f"Positions: {len(trader.positions)}\n"
            f"Sim Balance: {sim_state.sol_balance:.4f} SOL\n"
            f"Realized P&L: {sim_state.realized_pnl_sol:.4f} SOL\n"
            f"Daily P&L: {sim_state.daily_pnl_sol:.4f} SOL"
        )
        await query.message.reply_text(status)
        return MENU
    if query.data == "deposit":
        wallet = load_wallet()
        await query.message.reply_text(f"Deposit Address:\n{str(wallet.pubkey())}")
        return MENU
    if query.data == "toggle_mode":
        trader.update_runtime(test_mode=not trader.test_mode)
        update_env_key("TEST_MODE", "1" if trader.test_mode else "0")
        await query.message.reply_text(f"Mode now {'TEST' if trader.test_mode else 'LIVE'}")
        return MENU
    if query.data == "toggle_kill":
        trader.update_runtime(kill_switch=not trader.kill_switch)
        await query.message.reply_text(f"Kill switch {'ON' if trader.kill_switch else 'OFF'}")
        return MENU
    if query.data == "toggle_alerts":
        trader.update_runtime(trade_alerts=not trader.trade_alerts)
        update_env_key("TRADE_ALERTS", "1" if trader.trade_alerts else "0")
        await query.message.reply_text(f"Trade alerts {'ON' if trader.trade_alerts else 'OFF'}")
        return MENU
    if query.data == "strategy_menu":
        strategies = config["strategies"]
        buttons = []
        for name in ["momentum_v1", "breakout_v1", "mean_reversion_v1", "timebox_v1"]:
            buttons.append([InlineKeyboardButton(f"Select {name}", callback_data=f"select_{name}")])
        buttons.append([InlineKeyboardButton("Edit parameters", callback_data="edit_params")])
        await query.message.reply_text("Strategy menu", reply_markup=InlineKeyboardMarkup(buttons))
        return MENU
    if query.data.startswith("select_"):
        strategy = query.data.replace("select_", "")
        config["strategies"]["active"] = strategy
        save_config(config)
        await query.message.reply_text(f"Active strategy: {strategy}")
        return MENU
    if query.data == "edit_params":
        await query.message.reply_text("Send parameter name to edit (e.g. take_profit_pct or min_momentum_pct):")
        return EDIT_PARAM
    if query.data == "sim_deposit":
        await query.message.reply_text("Send deposit amount in SOL:")
        return EDIT_VALUE
    if query.data == "sim_reset":
        trader.sim_state.sol_balance = 0.0
        trader.sim_state.token_balances = {}
        trader.sim_state.trade_log = []
        trader.sim_state.realized_pnl_sol = 0.0
        trader.sim_state.daily_pnl_sol = 0.0
        trader.sim_state.save()
        await query.message.reply_text("Simulation state reset.")
        return MENU
    return MENU


async def handle_start_mint(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    config = context.application.bot_data["config"]
    alias_or_mint = update.message.text.strip()
    mint = config["aliases"].get(alias_or_mint.lower(), alias_or_mint)
    context.user_data["start_mint"] = mint
    trader: Trader = context.application.bot_data["trader"]
    await trader.ensure_feed(mint)
    client = trader.tracked[mint]
    price = client.last_price
    if price is None:
        await update.message.reply_text("Waiting for latest trade price...")
        for _ in range(10):
            await asyncio.sleep(2)
            if client.last_price:
                price = client.last_price
                break
    price_text = f"{price:.8f}" if price else "N/A"
    await update.message.reply_text(
        f"Mint: {mint}\n"
        f"Last price: {price_text}\n"
        f"URL: {get_pumpfun_url(mint)}\n"
        "Confirm start? YES/NO"
    )
    return START_CONFIRM


async def handle_start_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    response = update.message.text.strip().lower()
    mint = context.user_data.get("start_mint")
    trader: Trader = context.application.bot_data["trader"]
    if response in {"yes", "y"}:
        await trader.ensure_feed(mint)
        await update.message.reply_text(f"Trading enabled for {mint}.")
    else:
        await update.message.reply_text("Trading cancelled.")
    return MENU


async def handle_edit_param(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    param = update.message.text.strip()
    context.user_data["edit_param"] = param
    await update.message.reply_text(f"Send new value for {param}:")
    return EDIT_VALUE


async def handle_edit_value(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    trader: Trader = context.application.bot_data["trader"]
    config = trader.config
    value = update.message.text.strip()
    param = context.user_data.get("edit_param")
    if param:
        parsed: Any
        if value.lower() in {"true", "false"}:
            parsed = value.lower() == "true"
        else:
            parsed = safe_int(value) if value.isdigit() else safe_float(value)
        if parsed is None:
            await update.message.reply_text("Invalid value. Try again.")
            return EDIT_VALUE
        updated = False
        if param in config["risk"]["exits"]:
            config["risk"]["exits"][param] = parsed
            updated = True
        for strategy_name, params in config["strategies"].items():
            if isinstance(params, dict) and param in params:
                params[param] = parsed
                updated = True
        if updated:
            save_config(config)
            await update.message.reply_text("Updated configuration.")
        else:
            await update.message.reply_text("Parameter not found.")
        return MENU
    # Sim deposit
    amount = safe_float(value)
    if amount is None:
        await update.message.reply_text("Invalid amount.")
        return MENU
    trader.sim_state.sol_balance += amount
    trader.sim_state.save()
    await update.message.reply_text(f"Deposited {amount:.4f} SOL to simulation.")
    return MENU


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Unhandled error: %s", context.error)


def build_defaults() -> Dict[str, Any]:
    return {
        "aliases": {"btc": "BTC_ALIAS"},
        "risk": {
            "min_seconds_between_trades": 10,
            "max_trades_per_hour": 30,
            "max_position_sol": 0.5,
            "max_daily_loss_sol": 2.0,
            "kill_switch": False,
            "trade_alerts": True,
            "exits": {
                "take_profit_pct": 0.2,
                "stop_loss_pct": 0.1,
                "trailing_stop_pct": 0.05,
            },
        },
        "strategies": {
            "active": "momentum_v1",
            "momentum_v1": {
                "lookback_seconds": 30,
                "min_momentum_pct": 0.05,
                "min_trades_in_window": 3,
            },
            "breakout_v1": {
                "lookback_seconds": 60,
                "breakout_pct": 0.03,
                "min_trades_in_window": 3,
            },
            "mean_reversion_v1": {
                "lookback_seconds": 60,
                "zscore_entry": -1.5,
                "min_trades_in_window": 3,
            },
            "timebox_v1": {
                "base_strategy": "momentum_v1",
                "max_hold_seconds": 120,
            },
        },
        "runtime": {
            "test_mode": True,
            "slippage_pct": 0.05,
            "priority_fee": 0.0001,
        },
    }


def self_check() -> int:
    issues = []
    if not ENV_PATH.exists():
        issues.append("Missing .env")
    if not WALLET_PATH.exists():
        issues.append("Missing wallet.json")
    try:
        load_wallet()
    except Exception as exc:
        issues.append(f"Wallet load failed: {exc}")
    try:
        load_config()
    except Exception as exc:
        issues.append(f"Config load failed: {exc}")
    sim = SimState.load()
    sim.sol_balance += 1.0
    sim.save()
    if issues:
        print("Self-check issues:\n" + "\n".join(issues))
        return 1
    print("Self-check passed")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-check", action="store_true")
    args = parser.parse_args()
    if args.self_check:
        raise SystemExit(self_check())
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    load_dotenv(ENV_PATH)
    config = load_config()
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN")
    allowed = parse_allowed_users(os.getenv("ALLOWED_USER_IDS", ""))
    sim_state = SimState.load()
    application = Application.builder().token(token).build()
    trader = Trader(config, sim_state, application)
    application.bot_data["config"] = config
    application.bot_data["trader"] = trader
    application.bot_data["allowed_users"] = allowed
    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            MENU: [CallbackQueryHandler(handle_menu)],
            START_MINT: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_start_mint)],
            START_CONFIRM: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_start_confirm)],
            EDIT_PARAM: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_edit_param)],
            EDIT_VALUE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_edit_value)],
        },
        fallbacks=[CommandHandler("start", start)],
    )
    application.add_handler(conv)
    application.add_error_handler(error_handler)
    application.job_queue.run_once(lambda _: asyncio.create_task(trading_loop(config, trader)), when=1)
    application.run_polling()


if __name__ == "__main__":
    main()
'''


def prompt(text: str, default: str | None = None) -> str:
    suffix = f" [{default}]" if default else ""
    value = input(f"{text}{suffix}: ").strip()
    return value or (default or "")


def prompt_bool(text: str, default: bool) -> bool:
    suffix = "Y/n" if default else "y/N"
    while True:
        value = input(f"{text} ({suffix}): ").strip().lower()
        if not value:
            return default
        if value in {"y", "yes"}:
            return True
        if value in {"n", "no"}:
            return False


def run_command(cmd: list[str], cwd: Path | None = None) -> None:
    subprocess.check_call(cmd, cwd=str(cwd) if cwd else None)


def create_wallet(venv_python: Path, wallet_path: Path) -> None:
    script = textwrap.dedent(
        """
        import json
        from solders.keypair import Keypair
        kp = Keypair()
        data = {"secret_key": list(bytes(kp))}
        with open(r"{wallet}", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        """
    ).format(wallet=str(wallet_path))
    run_command([str(venv_python), "-c", script])
    try:
        os.chmod(wallet_path, 0o600)
    except PermissionError:
        pass


def write_env(env_path: Path, data: dict[str, str]) -> None:
    lines = [f"{key}={value}" for key, value in data.items()]
    env_path.write_text("\n".join(lines) + "\n")


def main() -> None:
    print("Solana Telegram Trading Bot Installer")
    token = prompt("Telegram bot token")
    allowed_users = prompt("Allowed Telegram user IDs (comma-separated)")
    rpc_url = prompt("Solana RPC URL", "https://api.mainnet-beta.solana.com")
    project_name = prompt("Project folder name", "solana-telegram-bot")
    test_mode = prompt_bool("Default TEST mode", True)
    slippage = prompt("Slippage % (e.g. 0.05 for 5%)", "0.05")
    priority_fee = prompt("Priority fee", "0.0001")
    auto_run = prompt_bool("Run bot after install", False)

    project_dir = Path.cwd() / project_name
    project_dir.mkdir(parents=True, exist_ok=True)
    venv_dir = project_dir / "venv"
    print("Creating virtual environment...")
    run_command([sys.executable, "-m", "venv", str(venv_dir)])

    venv_python = venv_dir / ("Scripts" if platform.system() == "Windows" else "bin") / "python"
    pip_cmd = [str(venv_python), "-m", "pip", "install", "--upgrade", "pip"]
    print("Upgrading pip...")
    run_command(pip_cmd)

    deps = [
        "python-telegram-bot>=21.0",
        "python-dotenv>=1.0",
        "requests>=2.31",
        "websockets>=12.0",
        "solana>=0.33.0",
        "solders>=0.20.0",
    ]
    print("Installing dependencies...")
    run_command([str(venv_python), "-m", "pip", "install", *deps])

    env_data = {
        "TELEGRAM_BOT_TOKEN": token,
        "ALLOWED_USER_IDS": allowed_users,
        "SOLANA_RPC_URL": rpc_url,
        "TEST_MODE": "1" if test_mode else "0",
        "TRADE_ALERTS": "1",
    }
    write_env(project_dir / ".env", env_data)

    defaults = {
        "aliases": {"btc": "BTC_ALIAS"},
        "risk": {
            "min_seconds_between_trades": 10,
            "max_trades_per_hour": 30,
            "max_position_sol": 0.5,
            "max_daily_loss_sol": 2.0,
            "kill_switch": False,
            "trade_alerts": True,
            "exits": {
                "take_profit_pct": 0.2,
                "stop_loss_pct": 0.1,
                "trailing_stop_pct": 0.05,
            },
        },
        "strategies": {
            "active": "momentum_v1",
            "momentum_v1": {
                "lookback_seconds": 30,
                "min_momentum_pct": 0.05,
                "min_trades_in_window": 3,
            },
            "breakout_v1": {
                "lookback_seconds": 60,
                "breakout_pct": 0.03,
                "min_trades_in_window": 3,
            },
            "mean_reversion_v1": {
                "lookback_seconds": 60,
                "zscore_entry": -1.5,
                "min_trades_in_window": 3,
            },
            "timebox_v1": {
                "base_strategy": "momentum_v1",
                "max_hold_seconds": 120,
            },
        },
        "runtime": {
            "test_mode": test_mode,
            "slippage_pct": float(slippage),
            "priority_fee": float(priority_fee),
        },
    }

    (project_dir / "config.json").write_text(json.dumps(defaults, indent=2))
    (project_dir / "sim_state.json").write_text(
        json.dumps(
            {
                "sol_balance": 0.0,
                "token_balances": {},
                "trade_log": [],
                "realized_pnl_sol": 0.0,
                "daily_pnl_sol": 0.0,
                "last_reset_date": datetime.now(timezone.utc).date().isoformat(),
            },
            indent=2,
        )
    )

    wallet_path = project_dir / "wallet.json"
    if not wallet_path.exists():
        print("Generating wallet...")
        create_wallet(venv_python, wallet_path)

    bot_path = project_dir / "bot.py"
    bot_path.write_text(BOT_SOURCE)

    print("Installation complete.")
    print(f"Project created at: {project_dir}")
    print("Run instructions:")
    print(f"  cd {project_dir}")
    print(f"  {venv_python} bot.py")

    if auto_run:
        run_command([str(venv_python), "bot.py"], cwd=project_dir)


if __name__ == "__main__":
    from datetime import datetime, timezone

    main()
