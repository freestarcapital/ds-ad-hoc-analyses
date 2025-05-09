import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
import configparser
from google.cloud import bigquery_storage
import os, sys
import datetime
import pickle
import scipy
from matplotlib.backends.backend_pdf import PdfPages
from sklearn.linear_model import LinearRegression

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

def get_cdf(x, col_name):
    return pd.DataFrame(np.arange(len(x)) / len(x), index=pd.Index(x[f'fp_{col_name}'].sort_values()), columns=[col_name])

def do_scatter(x, name):
    fig, ax = plt.subplots(figsize=(12, 9))
    ax.scatter(x['fp_old'], x['fp_new'])
    reg = LinearRegression(fit_intercept=False).fit(x[['fp_old']], x['fp_new'])
    x_max = x['fp_old'].max()
    ax.plot([0, x_max], [0, x_max] * reg.coef_ + reg.intercept_, c='r')
    ax.set_xlabel('old floor price')
    ax.set_ylabel('new floor price')
    r2 = reg.score(x[['fp_old']], x['fp_new'])
    fig.suptitle(
        f'{name}, floor price new ~ {reg.intercept_:0.2f} + {reg.coef_[0]:0.3f} x floor price old, R^2: {r2 * 100:0.1f}%')
    fig.savefig(f'scatter_{name}.png')


def main():

    x = pd.read_csv('reuters_compare_data.csv')

    fig, ax = plt.subplots(figsize=(16, 12))
    get_cdf(x, 'old').plot(ax=ax)
    get_cdf(x, 'new').plot(ax=ax)
    ax.set_xlim([0.2, 0.5])
    fig.savefig('cdf.png')

    do_scatter(x, 'all')
    do_scatter(x[x['country_code'] == 'US'], 'US')

    h = 0

if __name__ == "__main__":

    main()

