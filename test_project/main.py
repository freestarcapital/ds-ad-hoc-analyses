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

def check_new_and_add(X_all, X_new, seq):
    if len([1 for X_ in X_all if (X_['panel'] == X_new).all()]) == 0:
        X_all.append({'panel': X_new, 'seq': seq + [len(X_all)]})

        if (X_all[-1]['panel'] == 2).sum() == 0:
            print('puzzle solved')
            print(X_all[-1]['panel'])
            winning_seq = X_all[-1]['seq']
            print(f'winning seq: {winning_seq}')
            for seq_i, seq_n in enumerate(winning_seq):
                print(f'step: {seq_i}, panel no: {seq_n}, seq: {X_all[seq_n]['seq']}')
                print(X_all[seq_n]['panel'])
            return []

    return X_all

def X_add(X, seq, X_all, i0, i1, d_i0, d_i1, v_old, v_new):
    X_new = X.copy()
    X_new[i0 + d_i0, i1 + d_i1] = v_new
    X_new[i0, i1] = v_old
    return check_new_and_add(X_all, X_new, seq)

def X_add_2(X, seq, X_all, i0, i1, d_i0, d_i1, v_old, v_new, v_new_2):
    X_new = X.copy()
    X_new[i0 + d_i0, i1 + d_i1] = v_new
    X_new[i0 + 2 * d_i0, i1 + 2 * d_i1] = v_new_2
    X_new[i0, i1] = v_old
    return check_new_and_add(X_all, X_new, seq)

def X_print(X_all):
    for i, X_ in enumerate(X_all):
        print(f'panel: {i}, seq: {X_["seq"]}')
        print(X_['panel'])


def main_solve_puzzle(X):

    # 0 - wall
    # 1 - space
    # 2 - jewel
    # 3 - box
    # 4 - man
    # 5 - box and jewel
    # 6 - man and jewel

    # i0 down/up
    # i1 right/left

    (N0, N1) = np.shape(X)
    assert (X==2).sum() == (X==3).sum()
    assert (X==4).sum() == 1
    print('starting panel')
    print(X)
    X_all = [{'panel': X, 'seq': [0]}]
    p = 0

    for i in range(1000):

        print(f'iteration: {i}, number of panels: {len(X_all)}, about to work on panel: {p}')
    #    X_print(X_all)

        X = X_all[p]['panel']
        seq = X_all[p]['seq']

        assert (X == 2).sum() == (X == 3).sum()
        assert (X == 4).sum() == 1

        i0, i1 = np.where(X == 4)
        i0 = i0[0]
        i1 = i1[0]

        # 0 - wall
        # 1 - space
        # 2 - jewel
        # 3 - box
        # 4 - man
        # 5 - box and jewel
        # 6 - man and jewel

        if i0 > 0:              # not on top row
            if X[i0 - 1, i1] == 1:       # empty space above
                X_all = X_add(X, seq, X_all, i0, i1, -1, 0, 1, 4)

        if i0 > 1:              # not on top row
            if X[i0 - 1, i1] == 3:
                if X[i0 - 2, i1] == 1:
                    X_all = X_add_2(X, seq, X_all, i0, i1, -1, 0, 1, 4, 3)
                elif X[i0 - 2, i1] == 2:
                    X_all = X_add_2(X, seq, X_all, i0, i1, -1, 0, 1, 4, 5)

        if i1 > 0:              # not on top row
            if X[i0, i1 - 1] == 1:       # empty space above
                X_all = X_add(X, seq, X_all, i0, i1, 0, -1, 1, 4)

        if i1 > 1:              # not on top row
            if X[i0, i1 - 1] == 3:
                if X[i0, i1 - 2] == 1:
                    X_all = X_add_2(X, seq, X_all, i0, i1, 0, -1, 1, 4, 3)
                elif X[i0, i1 - 2] == 2:
                    X_all = X_add_2(X, seq, X_all, i0, i1, 0, -1, 1, 4, 5)

        if i0 < (N0 - 1):              # not on top row
            if X[i0 + 1, i1] == 1:       # empty space above
                X_all = X_add(X, seq, X_all, i0, i1, 1, 0, 1, 4)

        if i0 < (N0 - 2):              # not on top row
            if X[i0 + 1, i1] == 3:
                if X[i0 + 2, i1] == 1:
                    X_all = X_add_2(X, seq, X_all, i0, i1, 1, 0, 1, 4, 3)
                elif X[i0 + 2, i1] == 2:
                    X_all = X_add_2(X, seq, X_all, i0, i1, 1, 0, 1, 4, 5)

        if i1 < (N1 - 1):              # not on top row
            if X[i0, i1 + 1] == 1:       # empty space above
                X_all = X_add(X, seq, X_all, i0, i1, 0, 1, 1, 4)

        if i1 < (N1 -2 ):              # not on top row
            if X[i0, i1 + 1] == 3:
                if X[i0, i1 + 2] == 1:
                    X_all = X_add_2(X, seq, X_all, i0, i1, 0, 1, 1, 4, 3)
                elif X[i0, i1 + 2] == 2:
                    X_all = X_add_2(X, seq, X_all, i0, i1, 0, 1, 1, 4, 5)

        print(f'iteration: {i}, number of panels: {len(X_all)}, completed work on panel: {p}')
        X_print(X_all)
        # 0, 1, 4, 7, 12, 20,
        p = p + 1
        if p > len(X_all) - 1:
            print('done')
            break

def main_test():

    # 0 - wall
    # 1 - space
    # 2 - jewel
    # 3 - box
    # 4 - man
    # 5 - box and jewel
    # 6 - man and jewel

    X = np.array([[0, 0, 0, 0, 2, 0, 0, 0],
                  [0, 0, 0, 0, 1, 0, 0, 0],
                  [0, 0, 0, 0, 1, 0, 0, 0],
                  [0, 0, 0, 0, 3, 0, 0, 0],
                  [0, 0, 0, 0, 1, 0, 0, 0],
                  [2, 3, 1, 1, 4, 3, 1, 2],
                  [0, 0, 0, 0, 1, 0, 0, 0],
                  [0, 0, 0, 0, 3, 0, 0, 0],
                  [0, 0, 0, 0, 2, 0, 0, 0]])

    main_solve_puzzle(X)

def main_level_1():

    # 0 - wall
    # 1 - space
    # 2 - jewel
    # 3 - box
    # 4 - man
    # 5 - box and jewel
    # 6 - man and jewel

    X = np.array([[0, 0, 0, 2, 0, 0],
                  [0, 0, 0, 1, 0, 0],
                  [2, 1, 3, 3, 0, 0],
                  [0, 0, 1, 4, 3, 2],
                  [0, 0, 3, 0, 0, 0],
                  [0, 0, 2, 0, 1, 0]])

    main_solve_puzzle(X)


def main_level_2():

    # 0 - wall
    # 1 - space
    # 2 - jewel
    # 3 - box
    # 4 - man
    # 5 - box and jewel
    # 6 - man and jewel

    X = np.array([[0, 4, 0, 0, 2, 2],
                  [1, 3, 3, 1, 3, 2],
                  [1, 1, 1, 3, 1, 1],
                  [0, 0, 0, 1, 1, 2]])

    main_solve_puzzle(X)



if __name__ == "__main__":
#    main_test()
#    main_level_1()
    main_level_2()
