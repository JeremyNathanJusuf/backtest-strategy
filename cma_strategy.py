import numpy as np
import pandas as pd
from backtesting import Backtest, Strategy
from backtesting.lib import crossover

# Load the data
full_stock_data = pd.read_csv('./stock_data/five_minute_data/SPY_2019-2022')[:1000]
full_stock_data['date'] = pd.to_datetime(full_stock_data['date'])
full_stock_data.set_index('date', drop=True, inplace=True)

class CMA(Strategy):
    
    def init(self):
        self.data0 = full_stock_data
    
    def next(self):
        is_narrow = crossover(self.data0['Narrow'], 0.5)

        below_ma_200 = crossover(self.data0['MA 200'], self.data0['Hourly Close'])
        below_ma_50 = crossover(self.data0['MA 50'], self.data0['Hourly Close'])

        above_ma_200 = crossover(self.data0['Hourly Close'], self.data0['MA 200'])
        above_ma_50 = crossover(self.data0['Hourly Close'], self.data0['MA 50'])
        
        below_R4 = crossover(self.data0['R4'], self.data0['Close'])
        above_S4 = crossover(self.data0['Close'], self.data0['S4'])

        if (is_narrow and below_ma_200 and below_ma_50 and below_R4):
            self.sell()

        elif (is_narrow and above_ma_200 and above_ma_50 and above_S4):
            self.buy()


bt = Backtest(full_stock_data, CMA, cash=10000)
stats = bt.run()
print(stats)
bt.plot()