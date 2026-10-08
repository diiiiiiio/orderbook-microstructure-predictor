"""价格/数量的无损表示。

原始层保留字符串；盘口内部用整数定点数：price_ticks = price / tickSize，
qty_steps = qty / stepSize，两者必须整除，否则抛 PrecisionError（不静默取整）。
tickSize/stepSize 从 exchangeInfo 的 filters 读，不用 pricePrecision/quantityPrecision。
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation


class PrecisionError(ValueError):
    pass


def parse_decimal(s: str | Decimal) -> Decimal:
    if isinstance(s, Decimal):
        return s
    if not isinstance(s, str):
        raise PrecisionError(f"期望字符串数值，得到 {type(s).__name__}: {s!r}")
    try:
        d = Decimal(s)
    except InvalidOperation as exc:
        raise PrecisionError(f"无法解析数值 {s!r}") from exc
    if not d.is_finite():
        raise PrecisionError(f"非有限数值 {s!r}")
    return d


@dataclass(frozen=True)
class SymbolSpec:
    symbol: str
    tick_size: Decimal
    step_size: Decimal
    contract_type: str = "PERPETUAL"
    status: str = "TRADING"
    quote_asset: str = "USDT"
    margin_asset: str = "USDT"
    price_precision: int | None = None
    quantity_precision: int | None = None
    spec_version: str = ""      # exchangeInfo serverTime 或哈希

    def __post_init__(self) -> None:
        if self.tick_size <= 0 or self.step_size <= 0:
            raise PrecisionError(f"{self.symbol}: tickSize/stepSize 必须为正")

    def price_to_ticks(self, s: str | Decimal) -> int:
        d = parse_decimal(s)
        q = d / self.tick_size
        if q != q.to_integral_value():
            raise PrecisionError(f"{self.symbol}: 价格 {s} 不是 tickSize {self.tick_size} 的整数倍")
        if q < 0:
            raise PrecisionError(f"{self.symbol}: 价格为负 {s}")
        return int(q)

    def qty_to_steps(self, s: str | Decimal) -> int:
        d = parse_decimal(s)
        q = d / self.step_size
        if q != q.to_integral_value():
            raise PrecisionError(f"{self.symbol}: 数量 {s} 不是 stepSize {self.step_size} 的整数倍")
        if q < 0:
            raise PrecisionError(f"{self.symbol}: 数量为负 {s}")
        return int(q)

    def ticks_to_price(self, t: int) -> Decimal:
        return Decimal(t) * self.tick_size

    def steps_to_qty(self, n: int) -> Decimal:
        return Decimal(n) * self.step_size

    @staticmethod
    def from_exchange_info_symbol(s: dict, spec_version: str = "") -> "SymbolSpec":
        filters = {f["filterType"]: f for f in s.get("filters", [])}
        if "PRICE_FILTER" not in filters or "LOT_SIZE" not in filters:
            raise PrecisionError(f"{s.get('symbol')}: exchangeInfo 缺少 PRICE_FILTER/LOT_SIZE")
        return SymbolSpec(
            symbol=s["symbol"],
            tick_size=parse_decimal(filters["PRICE_FILTER"]["tickSize"]),
            step_size=parse_decimal(filters["LOT_SIZE"]["stepSize"]),
            contract_type=s.get("contractType", ""),
            status=s.get("status", ""),
            quote_asset=s.get("quoteAsset", ""),
            margin_asset=s.get("marginAsset", ""),
            price_precision=s.get("pricePrecision"),
            quantity_precision=s.get("quantityPrecision"),
            spec_version=spec_version,
        )


def validate_usdt_perpetual(spec: SymbolSpec) -> None:
    """确认是 USDT 本位永续、可交易；否则拒绝采集（避免误采交割/币本位/现货）。"""
    problems = []
    if spec.contract_type != "PERPETUAL":
        problems.append(f"contractType={spec.contract_type!r} 不是 PERPETUAL")
    if spec.quote_asset != "USDT":
        problems.append(f"quoteAsset={spec.quote_asset!r} 不是 USDT")
    if spec.margin_asset != "USDT":
        problems.append(f"marginAsset={spec.margin_asset!r} 不是 USDT")
    if spec.status != "TRADING":
        problems.append(f"status={spec.status!r} 不是 TRADING")
    if problems:
        raise PrecisionError(f"{spec.symbol} 不是可交易的 USDT 永续: " + "; ".join(problems))
