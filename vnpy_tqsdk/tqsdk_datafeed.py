"""天勤Tqsdk历史数据服务实现。"""

from datetime import timedelta, datetime
from typing import Any, cast
from collections.abc import Callable
import traceback

from pandas import DataFrame
from tqsdk import TqApi, TqAuth

from vnpy.trader.datafeed import BaseDatafeed
from vnpy.trader.setting import SETTINGS
from vnpy.trader.constant import Interval
from vnpy.trader.object import BarData, HistoryRequest
from vnpy.trader.utility import ZoneInfo


INTERVAL_VT2TQ: dict[Interval, int] = {
    Interval.MINUTE: 60,
    Interval.HOUR: 60 * 60,
    Interval.DAILY: 60 * 60 * 24
}

CHINA_TZ: ZoneInfo = ZoneInfo("Asia/Shanghai")


def _as_float(value: object) -> float:
    """
    把 pandas 单元格视为 float。

    itertuples 的字段在类型上是巨大联合，运行时天勤 K 线字段是数值。
    """
    return cast(float, value)


class TqsdkDatafeed(BaseDatafeed):
    """天勤TQsdk数据服务接口"""

    def __init__(self) -> None:
        """读取数据服务账号。"""
        self.username: str = SETTINGS["datafeed.username"]
        self.password: str = SETTINGS["datafeed.password"]

    def query_bar_history(self, req: HistoryRequest, output: Callable = print) -> list[BarData]:
        """查询k线数据"""
        # 初始化API
        try:
            api: TqApi = TqApi(auth=TqAuth(self.username, self.password))
        except Exception:
            output(traceback.format_exc())
            return []

        # 查询数据
        vt_interval: Interval = cast(Interval, req.interval)
        interval: int | None = INTERVAL_VT2TQ.get(vt_interval, None)
        if not interval:
            output(f"Tqsdk查询K线数据失败：不支持的时间周期{vt_interval.value}")
            return []

        tq_symbol: str = f"{req.exchange.value}.{req.symbol}"

        df: DataFrame = api.get_kline_data_series(
            symbol=tq_symbol,
            duration_seconds=interval,
            start_dt=req.start,
            end_dt=(cast(datetime, req.end) + timedelta(1))
        )

        # 关闭API
        api.close()

        # 解析数据
        bars: list[BarData] = []

        if df is not None:
            # itertuples 静态类型是 tuple，列字段无法命名
            tp: Any
            for tp in df.itertuples():
                bar: BarData = BarData(
                    symbol=req.symbol,
                    exchange=req.exchange,
                    interval=req.interval,
                    datetime=datetime.fromtimestamp(_as_float(tp.datetime) / 1_000_000_000, tz=CHINA_TZ),
                    open_price=_as_float(tp.open),
                    high_price=_as_float(tp.high),
                    low_price=_as_float(tp.low),
                    close_price=_as_float(tp.close),
                    volume=_as_float(tp.volume),
                    open_interest=_as_float(tp.open_oi),
                    gateway_name="TQ",
                )
                bars.append(bar)

        return bars
