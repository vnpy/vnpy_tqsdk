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
    pro: bool = True


class _Auth:
    def _has_feature(self, feature: str) -> bool:
        return feature == "tq_dl" and _State.pro


class _TqApi:
    def __init__(self, auth: _TqAuth | None = None) -> None:
        self.auth: _TqAuth | None = auth
        self.closed: bool = False
        self.calls: list[dict[str, object]] = []
        self._auth: _Auth = _Auth()
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

    def get_kline_serial(self, symbol: str, duration_seconds: int, data_length: int) -> pd.DataFrame:
        self.calls.append({
            "symbol": symbol,
            "duration_seconds": duration_seconds,
            "data_length": data_length,
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
    _State.pro = True
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


def _frame(times: list[float]) -> pd.DataFrame:
    return pd.DataFrame([
        {"datetime": t, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5, "volume": 10.0, "open_oi": 100.0}
        for t in times
    ])


def _nano(dt: datetime) -> float:
    return dt.timestamp() * 1_000_000_000


def test_query_bar_history_free_version() -> None:
    """免费版无 tq_dl 权限：取最近 10000 根，去掉填充行与正在形成的 K 线后按起止时间筛选"""
    _State.pro = False
    _State.frame = _frame([float("nan"), _nano(_BAR_DT), _nano(_BAR_DT.replace(minute=1)),
                           _nano(_BAR_DT.replace(minute=2)), _nano(_BAR_DT.replace(minute=3))])
    logs: list[str] = []
    req: HistoryRequest = HistoryRequest(
        symbol="rb2410",
        exchange=Exchange.SHFE,
        start=datetime(2024, 1, 15, 10, 1),
        end=datetime(2024, 1, 15),
        interval=Interval.MINUTE,
    )

    bars: list[BarData] = TqsdkDatafeed().query_bar_history(req, output=logs.append)

    assert isinstance(_State.api, _TqApi)
    assert _State.api.calls == [{"symbol": "SHFE.rb2410", "duration_seconds": 60, "data_length": 10000}]
    assert _State.api.closed
    assert [bar.datetime for bar in bars] == [_BAR_DT.replace(minute=1), _BAR_DT.replace(minute=2)]
    assert logs == []


def test_query_bar_history_free_version_warns_short_history() -> None:
    """免费版请求的开始时间早于可获取的最早 K 线时输出提示"""
    _State.pro = False
    _State.frame = _frame([_nano(_BAR_DT), _nano(_BAR_DT.replace(minute=1))])
    logs: list[str] = []
    req: HistoryRequest = HistoryRequest(
        symbol="rb2410",
        exchange=Exchange.SHFE,
        start=datetime(2023, 1, 1),
        end=datetime(2024, 1, 15),
        interval=Interval.MINUTE,
    )

    bars: list[BarData] = TqsdkDatafeed().query_bar_history(req, output=logs.append)

    assert [bar.datetime for bar in bars] == [_BAR_DT]
    assert logs == ["Tqsdk免费版只能获取最近10000根K线，本次数据从2024-01-15 10:00:00开始"]
