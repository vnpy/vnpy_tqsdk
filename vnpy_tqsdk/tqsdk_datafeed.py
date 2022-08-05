from datetime import timedelta
from typing import Dict, List, Optional
import traceback

from pandas import DataFrame, Timestamp
from tqsdk import TqApi, TqAuth

from vnpy.trader.datafeed import BaseDatafeed
from vnpy.trader.setting import SETTINGS
from vnpy.trader.constant import Interval
from vnpy.trader.object import BarData, HistoryRequest
from vnpy.trader.utility import ZoneInfo


INTERVAL_VT2TQ: Dict[Interval, int] = {
    Interval.MINUTE: 60,
    Interval.HOUR: 60 * 60,
    Interval.DAILY: 60 * 60 * 24,
    Interval.TICK: 0
}

CHINA_TZ = ZoneInfo("Asia/Shanghai")


class TqsdkDatafeed(BaseDatafeed):
    """天勤TQsdk数据服务接口"""

    def __init__(self):
        """"""
        self.username: str = SETTINGS["datafeed.username"]
        self.password: str = SETTINGS["datafeed.password"]

    def query_bar_history(self, req: HistoryRequest) -> Optional[List[BarData]]:
        """查询k线数据"""
        # 初始化API
        try:
            api: TqApi = TqApi(auth=TqAuth(self.username, self.password))
        except Exception:
            traceback.print_exc()
            return None

        # 查询数据
        tq_symbol: str = f"{req.exchange.value}.{req.symbol}"

        df: DataFrame = api.get_kline_data_series(
            symbol=tq_symbol,
            duration_seconds=INTERVAL_VT2TQ[req.interval],
            start_dt=req.start,
            end_dt=(req.end + timedelta(1))
        )

        # 关闭API
        api.close()

        # 解析数据
        bars: List[BarData] = []

        if df is not None:
            for tp in df.itertuples():
                # 天勤时间为与1970年北京时间相差的秒数，需要加上8小时差
                dt: Timestamp = Timestamp(tp.datetime).to_pydatetime() + timedelta(hours=8)

                bar: BarData = BarData(
                    symbol=req.symbol,
                    exchange=req.exchange,
                    interval=req.interval,
                    datetime=dt.replace(tzinfo=CHINA_TZ),
                    open_price=tp.open,
                    high_price=tp.high,
                    low_price=tp.low,
                    close_price=tp.close,
                    volume=tp.volume,
                    open_interest=tp.open_oi,
                    gateway_name="TQ",
                )
                bars.append(bar)

        return bars
