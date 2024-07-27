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

def find_where_man_can_touch(panel_walls, stage):
    boxes = stage['boxes']
    man = stage['man']

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

def add_new_stage(stages_all, stage, b, box_move_0, box_move_1):

    boxes = stage['boxes']
    boxes_new = boxes.copy()
    boxes_new[b, 0] = boxes[b, 0] + box_move_0
    boxes_new[b, 1] = boxes[b, 1] + box_move_1

    if len([1 for b in boxes if (b == boxes_new).all()]) == 0:
        stages_all.append({'boxes': boxes_new, 'man': boxes[b, :], 'seq': stage['seq'] + [len(stages_all)]})

    return stages_all

def print_boxes(stage, panel_walls, jewels):
    # 0 - space
    # 2 - box
    # 4 - man
    # +1 - jewel
    # 9 - wall

    boxes = stage['boxes']
    B = len(boxes)
    man = stage['man']
    panel_print = 9 * panel_walls.copy()
    panel_print[man[0], man[1]] += 4
    for b in range(B):
        panel_print[boxes[b, 0], boxes[b, 1]] += 2
        panel_print[jewels[b, 0], jewels[b, 1]] += 1

    print(f'seq: {stage['seq']}')
    print(panel_print)


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

    panel_walls = 1 * (panel_in == 0)
    jewels = np.argwhere(panel_in == 2)
    stages_all = [{'boxes': np.argwhere(panel_in == 3), 'man': np.argwhere(panel_in == 4)[0], 'seq': [0]}]

    print('starting panel')
    print_boxes(stages_all[0], panel_walls, jewels)

    p = 0
    for i in range(10000):

        # if (i/1000) == np.round(i/1000):
        #     print(i)

        len_stages_old = len(stages_all)

        stage = stages_all[p]
        panel_man = find_where_man_can_touch(panel_walls, stage)
        boxes = stage['boxes']
        for b in range(len(boxes)):

            if (panel_man[boxes[b, 0] + 1, boxes[b, 1]] == 1) and (panel_walls[boxes[b, 0] - 1, boxes[b, 1]] == 0):
                stages_all = add_new_stage(stages_all, stage, b, -1, 0)

            if (panel_man[boxes[b, 0] - 1, boxes[b, 1]] == 1) and (panel_walls[boxes[b, 0] + 1, boxes[b, 1]] == 0):
                stages_all = add_new_stage(stages_all, stage, b, +1, 0)

            if (panel_man[boxes[b, 0], boxes[b, 1] + 1] == 1) and (panel_walls[boxes[b, 0], boxes[b, 1] - 1] == 0):
                stages_all = add_new_stage(stages_all, stage, b, 0, -1)

            if (panel_man[boxes[b, 0], boxes[b, 1] - 1] == 1) and (panel_walls[boxes[b, 0], boxes[b, 1] + 1] == 0):
                stages_all = add_new_stage(stages_all, stage, b, 0, +1)

        print(f'len(stages_all): {len(stages_all)}, done stage[{p}], added {len(stages_all) - len_stages_old} stages, new stages add ...')
        for p_c in range(len_stages_old, len(stages_all)):

            if (stages_all[p_c]['boxes'] == jewels).all():
                print('PUZZLE COMPLETE !!!')
            print(f'FROM stage number: {p}')
            print_boxes(stages_all[p], panel_walls, jewels)
            print(f'TO stage number: {p_c}')
            print_boxes(stages_all[p_c], panel_walls, jewels)


        p = p + 1
def main_level_1():

    # 0 - wall
    # 1 - space
    # 2 - jewel
    # 3 - box
    # 4 - man

    panel_walls = np.array([[0, 0, 0, 0, 0, 0, 0, 0],
                            [0, 0, 0, 0, 2, 0, 0, 0],
                            [0, 0, 0, 0, 1, 0, 0, 0],
                            [0, 2, 1, 3, 3, 0, 0, 0],
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
