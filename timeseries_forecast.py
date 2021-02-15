import matplotlib.pyplot as plt
import tensorflow as tf
import keras
import numpy as np
import pandas as pd
import datetime
from sklearn.preprocessing import MinMaxScaler

from keras.models import Sequential
from keras.layers import Input, Dense, GRU, Embedding
from keras.optimizers import RMSprop
from keras.callbacks import EarlyStopping, ModelCheckpoint

df = pd.read_csv('metro_zhvi.csv')
dates = df.columns[5:]
x_vals = [datetime.datetime.strptime(d,"%m/%d/%Y").date() for d in dates]

for row in df.iloc[:1].itertuples():
    y_vals = row[6:]
    print(plt.plot(x_vals, y_vals))