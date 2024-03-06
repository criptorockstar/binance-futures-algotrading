from provider import Candlesticks
from charts import Plotcandles


class Main:
    def __init__(self):
        self.candlesticks = Candlesticks()
        self.plotcandles = Plotcandles()

    def start(self):
        self.candlesticks.stream()
        self.plotcandles.start()


if __name__ == "__main__":
    main = Main()
    main.start()
