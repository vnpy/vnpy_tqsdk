import sys
from datetime import datetime
from pathlib import Path
from types import ModuleType

import pandas as pd
import pytest
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, HistoryRequest


# tqsdk 不参与测试。TqApi 只记录查询参数并返回预设 K 线。
class _TqAuth:
    def __init__(self, username: str, password: str) -> None:
        self.username: str = username
        self.password: str = password


class _State:
    frame: pd.DataFrame = pd.DataFrame()
    api: object = None


class _TqApi:
    def __init__(self, auth: _TqAuth | None = None) -> None:
        self.auth: _TqAuth | None = auth
        self.closed: bool = False
        self.calls: list[dict[str, object]] = []
        _State.api = self

    def get_kline_data_series(
        self,
        symbol: str,
        duration_seconds: int,
        start_dt: datetime,
        end_dt: datetime,
    ) -> pd.DataFrame:
        self.calls.append({
            "symbol": symbol,
            "duration_seconds": duration_seconds,
            "start_dt": start_dt,
            "end_dt": end_dt,
        })
        return _State.frame

    def close(self) -> None:
        self.closed = True


_tqsdk: ModuleType = ModuleType("tqsdk")
_tqsdk.TqApi = _TqApi  # type: ignore[attr-defined]
_tqsdk.TqAuth = _TqAuth  # type: ignore[attr-defined]
sys.modules["tqsdk"] = _tqsdk

from vnpy_tqsdk.tqsdk_datafeed import CHINA_TZ, TqsdkDatafeed  # noqa: E402


_BAR_DT: datetime = datetime(2024, 1, 15, 10, 0, tzinfo=CHINA_TZ)
_FRAME: pd.DataFrame = pd.DataFrame([{
    "datetime": int(_BAR_DT.timestamp()) * 1_000_000_000,
    "open": 100.5,
    "high": 110.0,
    "low": 90.25,
    "close": 105.0,
    "volume": 12.0,
    "open_oi": 33.0,
}])


def _request(symbol: str, exchange: Exchange, interval: Interval) -> HistoryRequest:
    return HistoryRequest(
        symbol=symbol,
        exchange=exchange,
        start=datetime(2024, 1, 15, 9, 0),
        end=datetime(2024, 1, 15, 15, 0),
        interval=interval,
    )


@pytest.mark.parametrize(
    ("symbol", "exchange", "interval", "duration"),
    [
        ("rb2410", Exchange.SHFE, Interval.MINUTE, 60),
        ("IF2410", Exchange.CFFEX, Interval.HOUR, 60 * 60),
        ("sc2501", Exchange.INE, Interval.DAILY, 60 * 60 * 24),
        ("600000", Exchange.SSE, Interval.MINUTE, 60),
        ("TA501", Exchange.CZCE, Interval.MINUTE, 60),
    ],
)
def test_query_bar_history(symbol: str, exchange: Exchange, interval: Interval, duration: int) -> None:
    assert Path(sys.modules["vnpy_tqsdk.tqsdk_datafeed"].__file__ or "").resolve().is_relative_to(
        Path(__file__).resolve().parents[1]
    )
    _State.frame = _FRAME
    _State.api = None
    logs: list[str] = []
    bars: list[BarData] = TqsdkDatafeed().query_bar_history(
        _request(symbol, exchange, interval),
        output=logs.append,
    )

    assert logs == []
    assert isinstance(_State.api, _TqApi)
    assert _State.api.calls[0]["symbol"] == f"{exchange.value}.{symbol}"
    assert _State.api.calls[0]["duration_seconds"] == duration
    assert len(bars) == 1
    bar: BarData = bars[0]
    assert bar.symbol == symbol
    assert bar.exchange == exchange
    assert bar.interval == interval
    assert bar.datetime == _BAR_DT
    assert bar.open_price == 100.5
    assert bar.high_price == 110.0
    assert bar.low_price == 90.25
    assert bar.close_price == 105.0
    assert bar.volume == 12.0
