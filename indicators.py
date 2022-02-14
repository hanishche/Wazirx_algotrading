#!/usr/bin/env python
# coding: utf-8

# In[16]:


import pandas as pd
import numpy as np
def rsi_tradingview(ohlc: pd.DataFrame, period: int = 14, round_rsi: bool = True):
    """ Implements the RSI indicator as defined by TradingView on March 15, 2021.
        The TradingView code is as follows:
        //@version=4
        study(title="Relative Strength Index", shorttitle="RSI", format=format.price, precision=2, resolution="")
        len = input(14, minval=1, title="Length")
        src = input(close, "Source", type = input.source)
        up = rma(max(change(src), 0), len)
        down = rma(-min(change(src), 0), len)
        rsi = down == 0 ? 100 : up == 0 ? 0 : 100 - (100 / (1 + up / down))
        plot(rsi, "RSI", color=#8E1599)
        band1 = hline(70, "Upper Band", color=#C0C0C0)
        band0 = hline(30, "Lower Band", color=#C0C0C0)
        fill(band1, band0, color=#9915FF, transp=90, title="Background")
    :param ohlc:
    :param period:
    :param round_rsi:
    :return: an array with the RSI indicator values
    """
    delta = ohlc["close"].diff()

    up = delta.copy()
    up[up < 0] = 0
    up = pd.Series.ewm(up, alpha=1/period).mean()

    down = delta.copy()
    down[down > 0] = 0
    down *= -1
    down = pd.Series.ewm(down, alpha=1/period).mean()

    rsi = np.where(up == 0, 0, np.where(down == 0, 100, 100 - (100 / (1 + up / down))))

    return np.round(rsi, 2) if round_rsi else rsi


def stoch_rsi_tradingview(ohlc:pd.DataFrame, period:14, smoothK:3, smoothD:3, rsi):
    """ Calculating Stochastic RSI (gives the same values as TradingView as of March 20, 2021.
        smoothK = input(3, "K", minval=1)
        smoothD = input(3, "D", minval=1)
        lengthRSI = input(14, "RSI Length", minval=1)
        lengthStoch = input(14, "Stochastic Length", minval=1)
        src = input(close, title="RSI Source")
        rsi1 = rsi(src, lengthRSI)
        k = sma(stoch(rsi1, rsi1, rsi1, lengthStoch), smoothK)
        d = sma(k, smoothD)
    :param ohlc:
    :param period:
    :param smoothK:
    :param smoothD:
    :return:
    """
    # Calculate RSI
    rsi = rsi

    # Calculate StochRSI
    rsi = pd.Series(rsi)
    stochrsi  = (rsi - rsi.rolling(period).min()) / (rsi.rolling(period).max() - rsi.rolling(period).min())
    stochrsi_K = stochrsi.rolling(smoothK).mean()
    stochrsi_D = stochrsi_K.rolling(smoothD).mean()

    return round(stochrsi_K * 100, 2), round(stochrsi_D * 100, 2)


def MACD(data,period1:12,period2:26,period3:9):
    # Get the 26-day EMA of the closing price
    k = data['close'].ewm(span=period1, adjust=False, min_periods=period1).mean()
    # Get the 12-day EMA of the closing price
    d = data['close'].ewm(span=period2, adjust=False, min_periods=period2).mean()
    # Subtract the 26-day EMA from the 12-Day EMA to get the MACD
    macd = k - d
    # Get the 9-Day EMA of the MACD for the Trigger line
    macd_s = macd.ewm(span=period3, adjust=False, min_periods=period3).mean()
    # Calculate the difference between the MACD - Trigger for the Convergence/Divergence value
    macd_h = macd - macd_s
    # Add all of our new values for the MACD to the dataframe
#     data['macd'] = data.index.map(macd)
#     data['macd_s'] = data.index.map(macd_s)
#     data['macd_diff'] = data.index.map(macd_h)
#     macd=[]
#     for i in data['macd_diff'].astype(float):
#         if i<0 :
#             macd.append('bearish')
#         elif i>0:
#             macd.append('bullish')
#         else:
#             macd.append('Neutral')

#     data['macd_signal']=macd
    return macd,macd_s,macd_h


# In[ ]:




