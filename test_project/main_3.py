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


def X_print(X_all):
    for i, X_ in enumerate(X_all):
        print(f'panel: {i}, seq: {X_["seq"]}')
        print(X_['panel'])


def find_where_man_can_touch(panel_walls, boxes, man):

    (N0, N1) = np.shape(panel_walls)
    panel_man = np.zeros([N0, N1], np.int8)
    panel_man[man[0], man[1]] = 1
    for _ in range(max(N0, N1)):
        for [n0_i, n1_i] in np.argwhere(panel_man):
            if panel_man[n0_i, n1_i] > 0:
                if (n0_i < N0 - 1) and (panel_walls[n0_i + 1, n1_i] == 0) and (
                        len([1 for b in boxes if (b == np.array([n0_i + 1, n1_i])).all()]) == 0):
                    panel_man[n0_i + 1, n1_i] = 1
                if (n0_i > 0) and (panel_walls[n0_i - 1, n1_i] == 0) and (
                        len([1 for b in boxes if (b == np.array([n0_i - 1, n1_i])).all()]) == 0):
                    panel_man[n0_i - 1, n1_i] = 1
                if (n1_i < N1 - 1) and (panel_walls[n0_i, n1_i + 1] == 0) and (
                        len([1 for b in boxes if (b == np.array([n0_i, n1_i + 1])).all()]) == 0):
                    panel_man[n0_i, n1_i + 1] = 1
                if (n1_i > 0) and (panel_walls[n0_i, n1_i - 1] == 0) and (
                        len([1 for b in boxes if (b == np.array([n0_i, n1_i - 1])).all()]) == 0):
                    panel_man[n0_i, n1_i - 1] = 1
    return panel_man

def main_solve_puzzle(panel_in, verbose=False):

    # 0 - wall
    # 1 - space
    # 2 - jewel
    # 3 - box
    # 4 - man

    # i0 down/up
    # i1 right/left

#    (N0, N1) = np.shape(panel_in)
    assert (panel_in==2).sum() == (panel_in==3).sum()
    assert (panel_in==4).sum() == 1
    print('starting panel')
    print(panel_in)
    #X_all = [{'panel': panel_in, 'seq': [0]}]
    
    panel_walls = 1 * (panel_in == 0)
    boxes = np.argwhere(panel_in == 3)
    jewels = np.argwhere(panel_in == 2)
    man = np.argwhere(panel_in == 4)[0]
    
    p = 0
    for i in range(10000):

        if (i/1000) == np.round(i/1000):
            print(i)

        panel_man = find_where_man_can_touch(panel_walls, boxes, man)

        print(panel_man)
        f=0


        #
        # and see if he's touching each box

        possible_box_moves_list = []


def main_level_1():

    # 0 - wall
    # 1 - space
    # 2 - jewel
    # 3 - box
    # 4 - man

    panel_walls = np.array([[0, 0, 0, 0, 0, 0, 0, 0],
                            [0, 0, 0, 0, 2, 0, 0, 0],
                            [0, 0, 0, 0, 3, 0, 0, 0],
                            [0, 2, 1, 3, 1, 0, 0, 0],
                            [0, 0, 0, 4, 1, 3, 2, 0],
                            [0, 0, 0, 3, 0, 0, 0, 0],
                            [0, 0, 0, 1, 0, 0, 0, 0],
                            [0, 0, 0, 2, 0, 0, 0, 0],
                            [0, 0, 0, 0, 0, 0, 0, 0]])

    main_solve_puzzle(panel_walls)


def main_level_2():

    # 0 - wall
    # 1 - space
    # 2 - jewel
    # 3 - box
    # 4 - man
    # 5 - box and jewel
    # 6 - man and jewel

    # panel_walls = np.array(
    # [[0, 1, 0, 0, 2, 2],
    #  [1, 1, 1, 3, 3, 2],
    #  [1, 3, 1, 1, 1, 3],
    #  [0, 0, 0, 1, 1, 6]]
    # )

    panel_walls = np.array([[0, 4, 0, 0, 2, 2],
                  [1, 3, 3, 1, 3, 2],
                  [1, 1, 1, 3, 1, 1],
                  [0, 0, 0, 1, 1, 2]])

    main_solve_puzzle(panel_walls)


def main_level_3():

    # 0 - wall
    # 1 - space
    # 2 - jewel
    # 3 - box
    # 4 - man
    # 5 - box and jewel
    # 6 - man and jewel

    panel_walls = np.array([[0, 0, 0, 2, 2, 2],
                  [1, 4, 0, 0, 3, 1],
                  [1, 3, 1, 1, 1, 1],
                  [0, 1, 0, 0, 1, 1],
                  [0, 1, 1, 1, 0, 3],
                  [0, 0, 0, 1, 1, 1]])

    main_solve_puzzle(panel_walls)


def main_level_4():

    # 0 - wall
    # 1 - space
    # 2 - jewel
    # 3 - box
    # 4 - man
    # 5 - box and jewel
    # 6 - man and jewel

    panel_walls = np.array([[0, 0, 4, 1, 0],
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
    #main_test()
    main_level_1()
    #main_level_2()
#    main_level_3()
#    main_level_4()
