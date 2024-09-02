import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
import configparser
from google.cloud import bigquery_storage
import os, sys
import numpy as np
import datetime
import pickle
import plotly.express as px
import kaleido
from scipy.stats import linregress

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

config_path = '../config.ini'
config = configparser.ConfigParser()
config.read(config_path)

def main():

    x_in = pd.read_csv('v3.csv', header=None)

    x = np.ones((9, 9, 9))
    for i1 in range(9):
        for i2 in range(9):
            if np.isnan(x_in.iloc[i1, i2]):
                continue
            for n in range(9):
                if x_in.iloc[i1, i2] - 1 == n:
                    x[i1, i2, n] = 2

    x_list = []
    for _ in range(1000):

        for i1 in range(9):
            for i2 in range(9):
                for n in range(9):
                    if x[i1, i2, n] < 2:
                        continue

                    for c in range(9):
                        if x[i1, c, n] == 1:
                            x[i1, c, n] = 0
                        if x[c, i2, n] == 1:
                            x[c, i2, n] = 0
                        if x[i1, i2, c] == 1:
                            x[i1, i2, c] = 0

                    h =0

                    for c1 in range(3):
                        for c2 in range(3):
                            if x[int(np.floor(i1/3)*3+c1), int(np.floor(i2/3)*3+c2), n] == 1:
                                x[int(np.floor(i1 / 3) * 3 + c1), int(np.floor(i2 / 3) * 3 + c2), n] = 0

        for i in range(9):
            for n in range(9):
                if (x[i, :, n] >= 1).sum() == 1:
                    x[i, (x[i, :, n] >= 1), n] = 2
                if (x[:, i, n] >= 1).sum() == 1:
                    x[(x[:, i, n] >= 1), i, n] = 2

        for i1 in range(3):
            for i2 in range(3):
                for n in range(9):
                    if (x[i1 * 3: i1 * 3 + 3, i2 * 3: i2 * 3 + 3, n] >= 1).sum() == 1:
                        for c1 in range(3):
                            for c2 in range(3):
                                if x[i1 * 3 + c1, i2 * 3 + c2, n] >= 1:
                                    x[i1 * 3 + c1, i2 * 3 + c2, n] = 2

        for i1 in range(9):
            for i2 in range(9):
                if (x[i1, i2, :] >= 1).sum() == 1:
                    x[i1, i2, x[i1, i2, :] >= 1] = 2


        done = True
        wrong = False

        for i in range(9):
            for n in range(9):
                if (x[i, :, n] == 2).sum() < 1:
                    done = False
                if (x[i, :, n] == 2).sum() > 1:
                    wrong = True
                    print(f'horizt, i: {i}, n+1: {n + 1}')
                if (x[:, i, n] == 2).sum() < 1:
                    done = False
                if (x[:, i, n] == 2).sum() > 1:
                    wrong = True
                    print(f'vert, i: {i}, n+1: {n+1}')

        for i1 in range(3):
            for i2 in range(3):
                for n in range(9):
                    if (x[i1 * 3: i1 * 3 + 3, i2 * 3: i2 * 3 + 3, n] == 2).sum() < 1:
                        done = False
                    if (x[i1 * 3: i1 * 3 + 3, i2 * 3: i2 * 3 + 3, n] == 2).sum() > 1:
                        wrong = True
                        print(f'sq, i1: {i1}, i2: {i2}, n+1: {n + 1}')

        for i1 in range(9):
            for i2 in range(9):
                if (x[i1, i2, :] == 2).sum() < 1:
                    done = False
                if (x[i1, i2, :] == 2).sum() > 1:
                    wrong = True
                    print(f'num, i1: {i1}, i2: {i2}')

        x_list.append(x.sum())
        if ((len(x_list) > 2) and (x_list[-1] == x_list[-2])) or done or wrong:
            break

    print(f'done: {done}, wrong: {wrong}')

    x_out = np.zeros((9, 9))
    for i1 in range(9):
        for i2 in range(9):
            for n in range(9):
                if x[i1, i2, n] == 2:
                    x_out[i1, i2] = n+1

    print(x_out)
    h = 0

if __name__ == "__main__":
    main()