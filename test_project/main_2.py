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

        if ((X_new == 2).sum() + (X_new == 6).sum()) == 0:
            print('puzzle solved')
            print(X_new)
            winning_seq = X_all[-1]['seq']
            print(f'winning seq: {winning_seq}')
            for seq_i, seq_n in enumerate(winning_seq):
                print(f'step: {seq_i}, panel no: {seq_n}, seq: {X_all[seq_n]['seq']}')
                print(X_all[seq_n]['panel'])
            return []

    return X_all

def X_move_into_space_or_jewel(X, seq, X_all, i0, i1, d_i0, d_i1):
    X_new = X.copy()

    # space moving from is man or man and jewel

    # 0 - wall
    # 1 - space
    # 2 - jewel
    # 3 - box
    # 4 - man
    # 5 - box and jewel
    # 6 - man and jewel
    
    if X[i0 + d_i0, i1 + d_i1] == 1:    # moving into an empty space
        X_new[i0 + d_i0, i1 + d_i1] = 4     # so that space becomes man
    elif X[i0 + d_i0, i1 + d_i1] == 2:    # moving into a jewel space
        X_new[i0 + d_i0, i1 + d_i1] = 6   # so that space becomes man and jewwl
    else:
        assert False     # shouldn't move into anything else

    if X[i0, i1] == 4:  # moving from a man space
        X_new[i0, i1] = 1  # so that space becomes empty
    elif X[i0, i1] == 6:  # moving from a man and jewel space
        X_new[i0, i1] = 2  # so that space becomes a jewwl
    else:
        assert False  # shouldn't move from anything else
    
    return check_new_and_add(X_all, X_new, seq)

def X_move_into_box_or_box_and_jewel(X, seq, X_all, i0, i1, d_i0, d_i1):
    X_new = X.copy()

    # one space ahead is a box or box and jewel
    # the panel exists 2 spaces ahead, but we don't know what's there
    # first check there is something we can move into 2 spaces ahead i.e. space or jewel, otherwise return without doing anything

    # 0 - wall
    # 1 - space
    # 2 - jewel
    # 3 - box
    # 4 - man
    # 5 - box and jewel
    # 6 - man and jewel

    v_ahead_2 = X[i0 + 2 * d_i0, i1 + 2 * d_i1]
    if v_ahead_2 in [0, 3, 5]:
        return X_all
    elif v_ahead_2 == 1:
        v_new_2 = 3
    elif v_ahead_2 == 2:
        v_new_2 = 5
    else:
        assert False
    X_new[i0 + 2 * d_i0, i1 + 2 * d_i1] = v_new_2

    # one space ahead is a box or box and jewel
    if X[i0 + d_i0, i1 + d_i1] == 3:  # moving into a box space
        X_new[i0 + d_i0, i1 + d_i1] = 4  # so that space becomes man
    elif X[i0 + d_i0, i1 + d_i1] == 5:  # moving into a box and jewel space
        X_new[i0 + d_i0, i1 + d_i1] = 6  # so that space becomes man and jewwl
    else:
        assert False  # shouldn't move into anything else

    # space moving from is man or man and jewel
    if X[i0, i1] == 4:  # moving from a man space
        X_new[i0, i1] = 1  # so that space becomes empty
    elif X[i0, i1] == 6:  # moving from a man and jewel space
        X_new[i0, i1] = 2  # so that space becomes a jewwl
    else:
        assert False  # shouldn't move from anything else

    return check_new_and_add(X_all, X_new, seq)

def X_print(X_all):
    for i, X_ in enumerate(X_all):
        print(f'panel: {i}, seq: {X_["seq"]}')
        print(X_['panel'])


def main_solve_puzzle(X, verbose=False):

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
    assert ((X==2).sum() + (X==6).sum()) == (X==3).sum()
    assert ((X==4).sum() + (X==6).sum()) == 1
    print('starting panel')
    print(X)
    X_all = [{'panel': X, 'seq': [0]}]
    p = 0

    for i in range(100000):

        if (i/1000) == np.round(i/1000):
            print(i)

        if verbose:
            print(f'iteration: {i}, number of panels: {len(X_all)}, about to work on panel: {p}')
        #    X_print(X_all)

        X = X_all[p]['panel']
        seq = X_all[p]['seq']

        assert ((X == 2).sum() + (X == 6).sum()) == (X == 3).sum()
        assert ((X == 4).sum() + (X == 6).sum()) == 1

        i0, i1 = np.where(X == 4)
        if len(i0) == 0:
            i0, i1 = np.where(X == 6)
        i0 = i0[0]
        i1 = i1[0]

        # 0 - wall
        # 1 - space
        # 2 - jewel
        # 3 - box
        # 4 - man
        # 5 - box and jewel
        # 6 - man and jewel

        if i0 > 0:              
            if X[i0 - 1, i1] in [1, 2]:       
                X_all = X_move_into_space_or_jewel(X, seq, X_all, i0, i1, -1, 0)

        if i0 > 1:              
            if X[i0 - 1, i1] in [3, 5]:
                X_all = X_move_into_box_or_box_and_jewel(X, seq, X_all, i0, i1, -1, 0)

        if i1 > 0:              
            if X[i0, i1 - 1] in [1, 2]:       
                X_all = X_move_into_space_or_jewel(X, seq, X_all, i0, i1, 0, -1)

        if i1 > 1:              
            if X[i0, i1 - 1] in [3, 5]:
                X_all = X_move_into_box_or_box_and_jewel(X, seq, X_all, i0, i1, 0, -1)
                
        if i0 < (N0 - 1):              
            if X[i0 + 1, i1] in [1, 2]:       
                X_all = X_move_into_space_or_jewel(X, seq, X_all, i0, i1, 1, 0)

        if i0 < (N0 - 2):              
            if X[i0 + 1, i1] in [3, 5]:
                X_all = X_move_into_box_or_box_and_jewel(X, seq, X_all, i0, i1, 1, 0)
                
        if i1 < (N1 - 1):              
            if X[i0, i1 + 1] in [1, 2]:       
                X_all = X_move_into_space_or_jewel(X, seq, X_all, i0, i1, 0, 1)

        if i1 < (N1 - 2):              
            if X[i0, i1 + 1] == 3:
                X_all = X_move_into_box_or_box_and_jewel(X, seq, X_all, i0, i1, 0, 1)

        if verbose:
            print(f'iteration: {i}, number of panels: {len(X_all)}, completed work on panel: {p}')
            #X_print(X_all)

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
                  [0, 0, 2, 0, 0, 0]])

    main_solve_puzzle(X)


def main_level_2():

    # 0 - wall
    # 1 - space
    # 2 - jewel
    # 3 - box
    # 4 - man
    # 5 - box and jewel
    # 6 - man and jewel

    # X = np.array(
    # [[0, 1, 0, 0, 2, 2],
    #  [1, 1, 1, 3, 3, 2],
    #  [1, 3, 1, 1, 1, 3],
    #  [0, 0, 0, 1, 1, 6]]
    # )

    X = np.array([[0, 4, 0, 0, 2, 2],
                  [1, 3, 3, 1, 3, 2],
                  [1, 1, 1, 3, 1, 1],
                  [0, 0, 0, 1, 1, 2]])

    main_solve_puzzle(X)


def main_level_3():

    # 0 - wall
    # 1 - space
    # 2 - jewel
    # 3 - box
    # 4 - man
    # 5 - box and jewel
    # 6 - man and jewel

    X = np.array([[0, 0, 0, 2, 2, 2],
                  [1, 4, 0, 0, 3, 1],
                  [1, 3, 1, 1, 1, 1],
                  [0, 1, 0, 0, 1, 1],
                  [0, 1, 1, 1, 0, 3],
                  [0, 0, 0, 1, 1, 1]])

    main_solve_puzzle(X)


def main_level_4():

    # 0 - wall
    # 1 - space
    # 2 - jewel
    # 3 - box
    # 4 - man
    # 5 - box and jewel
    # 6 - man and jewel

    X = np.array([[0, 0, 4, 1, 0],
                  [0, 0, 1, 3, 0],
                  [1, 3, 3, 2, 0],
                  [1, 3, 2, 2, 1],
                  [1, 3, 2, 2, 1],
                  [1, 1, 1, 0, 0]])

    X1 = np.array([[0, 0, 1, 1, 0],
                   [0, 0, 1, 1, 0],
                   [1, 3, 3, 2, 0],
                   [1, 3, 2, 6, 1],
                   [1, 3, 2, 5, 1],
                   [1, 1, 1, 0, 0]])

    X2 = np.array([[0, 0, 1, 1, 0],
                   [0, 0, 3, 1, 0],
                   [1, 3, 4, 2, 0],
                   [1, 3, 2, 2, 1],
                   [1, 3, 2, 5, 1],
                   [1, 1, 1, 0, 0]])

    X2 = np.array([[0, 0, 1, 1, 0],
                   [0, 0, 3, 1, 0],
                   [1, 3, 1, 2, 0],
                   [1, 1, 6, 5, 1],
                   [1, 3, 2, 5, 1],
                   [1, 1, 1, 0, 0]])

    X3 = np.array([[0, 0, 1, 1, 0],
                   [0, 0, 1, 1, 0],
                   [1, 3, 1, 2, 0],
                   [1, 1, 6, 5, 1],
                   [1, 3, 5, 5, 1],
                   [1, 1, 1, 0, 0]])

    X4 = np.array([[0, 0, 1, 1, 0],
                   [0, 0, 1, 1, 0],
                   [1, 1, 4, 5, 0],
                   [1, 1, 2, 5, 1],
                   [1, 3, 5, 5, 1],
                   [1, 1, 1, 0, 0]])


    main_solve_puzzle(X1)


if __name__ == "__main__":
#    main_test()
  #  main_level_1()
    #main_level_2()
#    main_level_3()
    main_level_4()
