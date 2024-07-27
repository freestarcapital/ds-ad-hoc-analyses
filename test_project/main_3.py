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

#TODO : make man move panel more efficient by cut off when no change

def find_where_man_can_touch(panel_walls, boxes, man):
    (N0, N1) = np.shape(panel_walls)
    panel_man = np.zeros([N0, N1], np.int8)
    panel_man[man[0], man[1]] = 1
    for c in range(max(N0, N1)):
        sum_old = panel_man.sum()
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
        if panel_man.sum() == sum_old:
            #print(f'breaking: {c}, found: {sum_old}')
            return panel_man

    assert False, "incomplete panel_man"
def are_same(a, b, N=100):
    A = a[:, 0] * N + a[:, 1]
    A.sort()
    B = b[:, 0] * N + b[:, 1]
    B.sort()
    return (A == B).all()

def add_new_stage(stages_all, stage, b, box_move_0, box_move_1, move, panel_walls):

    boxes = stage['boxes']
    boxes_new = boxes.copy()
    boxes_new[b, 0] = boxes[b, 0] + box_move_0
    boxes_new[b, 1] = boxes[b, 1] + box_move_1

    man_new = boxes[b, :]
    panel_man_new = find_where_man_can_touch(panel_walls, boxes_new, man_new)

    already_in_list = False
    for st_ in stages_all:
        if are_same(st_['boxes'], boxes_new) and (st_['panel_man'] == panel_man_new).all():
            already_in_list = True
            break
    if not already_in_list:
        stages_all.append({'boxes': boxes_new, 'panel_man': panel_man_new, 'seq': stage['seq'] + [len(stages_all)], 'moves': stage['moves'] + move})

    return stages_all

def print_boxes(stage, panel_walls, jewels):
    # 0 - space
    # 2 - box
    # 4 - panel_man
    # +1 - jewel
    # 9 - wall

    boxes = stage['boxes']
    B = len(boxes)
    panel_print = 9 * panel_walls.copy() + 4 * stage['panel_man'].copy()
    for b in range(B):
        panel_print[boxes[b, 0], boxes[b, 1]] += 2
        panel_print[jewels[b, 0], jewels[b, 1]] += 1

    print(f'seq: {stage['seq']}, moves: {stage["moves"]}')
    print(panel_print)


def main_solve_puzzle(panel_in, verbose=False):
    # 0 - space
    # 2 - box
    # 4 - man
    # 1 - jewel
    # 9 - wall

    # i0 down/up
    # i1 right/left

    assert (panel_in==1).sum() == (panel_in==2).sum()
    assert (panel_in==4).sum() == 1

    panel_walls = 1 * (panel_in == 9)
    jewels = np.argwhere(panel_in == 1)

    boxes_start = np.argwhere(panel_in == 2)
    man_start = np.argwhere(panel_in == 4)[0]
    panel_man_start = find_where_man_can_touch(panel_walls, boxes_start, man_start)
    stages_all = [{'boxes': boxes_start, 'panel_man': panel_man_start, 'seq': [0], 'moves': ''}]

    print('starting panel')
    print_boxes(stages_all[0], panel_walls, jewels)

    solved = False
    p = 0
    for i in range(10000):

        # if (i/1000) == np.round(i/1000):
        #     print(i)

        len_stages_old = len(stages_all)

        stage = stages_all[p]
        panel_man = stage['panel_man']
        boxes = stage['boxes']
        for b in range(len(boxes)):

            if (panel_man[boxes[b, 0] + 1, boxes[b, 1]] == 1) and (panel_walls[boxes[b, 0] - 1, boxes[b, 1]] == 0):
                stages_all = add_new_stage(stages_all, stage, b, -1, 0, 'U', panel_walls)

            if (panel_man[boxes[b, 0] - 1, boxes[b, 1]] == 1) and (panel_walls[boxes[b, 0] + 1, boxes[b, 1]] == 0):
                stages_all = add_new_stage(stages_all, stage, b, +1, 0, 'D', panel_walls)

            if (panel_man[boxes[b, 0], boxes[b, 1] + 1] == 1) and (panel_walls[boxes[b, 0], boxes[b, 1] - 1] == 0):
                stages_all = add_new_stage(stages_all, stage, b, 0, -1, 'L', panel_walls)

            if (panel_man[boxes[b, 0], boxes[b, 1] - 1] == 1) and (panel_walls[boxes[b, 0], boxes[b, 1] + 1] == 0):
                stages_all = add_new_stage(stages_all, stage, b, 0, +1, 'R', panel_walls)

#        moves_str = ', '.join([f'{stages_all[p_c]["moves"]} ({stages_all[p_c]["seq"]})' for p_c in range(len_stages_old, len(stages_all))])
        moves_str = ', '.join([stages_all[p_c]["moves"] for p_c in range(len_stages_old, len(stages_all))])
        print(f'total stages stored: {len(stages_all)}, done stage {p}, {stages_all[p]["moves"]}, added {len(stages_all) - len_stages_old} stages: {moves_str}')
        for p_c in range(len_stages_old, len(stages_all)):

            if are_same(stages_all[p_c]['boxes'], jewels):
                print('PUZZLE COMPLETE !!!')
                winning_seq = stages_all[p_c]['seq']
                for seq_c in winning_seq:
                    print_boxes(stages_all[seq_c], panel_walls, jewels)
                solved = True
                break

            # print(f'FROM stage number: {p}')
            # print_boxes(stages_all[p], panel_walls, jewels)
            # print(f'TO stage number: {p_c}')
            # print_boxes(stages_all[p_c], panel_walls, jewels)

        if solved:
            break

        if len(stages_all[-1]['seq']) == 7:
            f = 0
        p = p + 1
def main_level_1():

    # 0 - space
    # 2 - box
    # 4 - man
    # 1 - jewel
    # 9 - wall

    panel_walls = np.array([[9, 9, 9, 9, 9, 9, 9, 9],
                            [9, 9, 9, 9, 1, 9, 9, 9],
                            [9, 9, 9, 9, 0, 9, 9, 9],
                            [9, 1, 0, 2, 2, 9, 9, 9],
                            [9, 9, 9, 0, 4, 2, 1, 9],
                            [9, 9, 9, 2, 9, 9, 9, 9],
                            [9, 9, 9, 1, 9, 9, 9, 9],
                            [9, 9, 9, 9, 9, 9, 9, 9]])

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
    main_level_1()
    #main_level_2()
#    main_level_3()
#    main_level_4()
