from lightweight_charts import Chart
import time
import threading
from provider import Candlesticks


class Plotcandles:
    def __init__(self):
        self.candlesticks = Candlesticks()

    def start(self):
        time.sleep(3)

        df = self.candlesticks.fetch()

        rt_df1 = df

        rt_chart = Chart()

        # styles
        rt_chart.grid(vert_enabled=True, horz_enabled=True)
        rt_chart.volume_config(scale_margin_top=0.98)

        rt_chart.set(rt_df1)

        rt_chart.show(block=False)

        def update():
            while True:
                df2 = self.candlesticks.fetch()
                rt_df2 = df2.tail(1)

                # Cast dataframe to panda series
                for i, series in rt_df2.iterrows():
                    rt_chart.update(series)
                    time.sleep(0.1)

        t = threading.Thread(target=update)
        t.start()
