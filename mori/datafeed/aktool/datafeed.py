# 继承 BaseDatafeed
from typing import Callable
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.datafeed import BaseDatafeed
from vnpy.trader.object import BarData, HistoryRequest, TickData
from vnpy.trader.locale import _
from vnpy.trader.setting import SETTINGS
from vnpy.trader.utility import round_to

from datetime import datetime
from typing import cast
from collections.abc import Callable

from numpy import ndarray
from pandas import DataFrame, Timestamp
import akshare as ak

INTERVAL_VT2RQ: dict[Interval, str] = {
    # Interval.MINUTE: "1m",
    # Interval.HOUR: "60m",
    Interval.DAILY: "daily",
    Interval.WEEKLY: "weekly",
    # Interval.MONTHLY: "monthly",
}


class AktoolDatafeed(BaseDatafeed):
    """
    AKShare 数据服务适配器。
    """

    def __init__(self):
        """
        可拓展需要的自定义参数
        """
        self.username: str = SETTINGS["datafeed.username"]
        self.password: str = SETTINGS["datafeed.password"]

        self.inited: bool = True

    def query_bar_history(
        self, req: HistoryRequest, output: Callable = print
    ) -> list[BarData]:
        """
        查询K线数据
        """
        if not self.inited:
            n: bool = self.init(output)
            if not n:
                return []

        symbol: str = req.symbol
        exchange: Exchange = req.exchange
        interval: Interval | None = req.interval
        start: datetime = req.start
        end: datetime = req.end if req.end is not None else datetime.now()

        # 股票期权
        if exchange not in [Exchange.SSE, Exchange.SZSE]:
            output(f"暂不支持该合约，代码：{req.vt_symbol}")
            return []

        parsed_interval: str | None = (
            None if interval is None else INTERVAL_VT2RQ.get(interval, None)
        )
        if not parsed_interval:
            output(f"查询K线数据失败：不支持的时间周期{req.interval}")
            return []

        # 东财历史行情接口：https://akshare.akfamily.xyz/data/stock/stock.html#id23
        stock_zh_a_hist_df = ak.stock_zh_a_hist(
            symbol,
            period=parsed_interval,
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
            # 前复权
            adjust="qfq",
        )

        data: list[BarData] = []

        if stock_zh_a_hist_df is not None:
            # 填充NaN为0
            stock_zh_a_hist_df.fillna(0, inplace=True)

            for row in stock_zh_a_hist_df.itertuples():
                # 提取日期字段
                dt: datetime = datetime.combine(row.日期, datetime.min.time())  # type: ignore

                if datetime.timestamp(dt) >= datetime.timestamp(end):
                    break

                bar: BarData = BarData(
                    symbol=symbol,
                    exchange=exchange,
                    interval=interval,
                    datetime=dt,
                    open_price=round_to(row.开盘, 0.000001),  # type: ignore
                    high_price=round_to(row.最高, 0.000001),  # type: ignore
                    low_price=round_to(row.最低, 0.000001),  # type: ignore
                    close_price=round_to(row.收盘, 0.000001),  # type: ignore
                    volume=row.成交量,  # type: ignore
                    turnover=row.成交额,  # type: ignore
                    open_interest=getattr(row, "open_interest", 0),
                    gateway_name="RQ",
                )

                data.append(bar)

        return data

    def query_tick_history(
        self, req: HistoryRequest, output: Callable = print
    ) -> list[TickData]:
        """
        Query history tick data.
        """
        output(_("查询Tick数据失败：没有正确配置数据服务"))
        return []
