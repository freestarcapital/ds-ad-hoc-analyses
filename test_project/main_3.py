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

def find_where_man_can_touch_2(panel_walls, boxes, man, RL_list, DU_list):

    (N0, N1) = np.shape(panel_walls)
    panel_man = np.zeros([N0, N1], np.int8)
    panel_man[man[0], man[1]] = 1

    for c in range(3 * max(N0, N1)):
        sum_old = panel_man.sum()

        # look left and right
        for (row_i, R_inds, L_inds) in RL_list:
            if panel_man[row_i, :].sum() > 0:
                # looking right
                for i in R_inds:
                    if panel_man[row_i, i] == 1:
                        if not is_in(np.array([row_i, i + 1]), boxes):
                            panel_man[row_i, i + 1] = 1
                # looking left
                for i in L_inds:
                    if panel_man[row_i, i] == 1:
                        if not is_in(np.array([row_i, i - 1]), boxes):
                            panel_man[row_i, i - 1] = 1

        # look up and down
        for (col_i, D_inds, U_inds) in DU_list:
            if panel_man[:, col_i].sum() > 0:
                # looking down
                for i in D_inds:
                    if panel_man[i, col_i] == 1:
                        if not is_in(np.array([i + 1, col_i]), boxes):
                            panel_man[i + 1, col_i] = 1
                # looking up
                for i in U_inds:
                    if panel_man[i, col_i] == 1:
                        if not is_in(np.array([i - 1, col_i]), boxes):
                            panel_man[i - 1, col_i] = 1

        #print(f'sum_old: {sum_old}; sum: {panel_man.sum()}')
        if sum_old == panel_man.sum():
            return panel_man

    assert False, "incomplete panel_man"


def find_where_man_can_touch(panel_walls, boxes, man):
    (N0, N1) = np.shape(panel_walls)
    panel_man = np.zeros([N0, N1], np.int8)
    panel_man[man[0], man[1]] = 1
    for c in range(3 * max(N0, N1)):
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
    assert len(a) == len(b)
    A = a[:, 0] * N + a[:, 1]
    A.sort()
    B = b[:, 0] * N + b[:, 1]
    B.sort()
    return (A == B).all()

def is_in(a, b, N=100):
    assert len(a) == 2
    A = a[0] * N + a[1]
    B = b[:, 0] * N + b[:, 1]
    return A in B

def find_and_add_new_stage(stages_all, stage, b, box_move_0, box_move_1, move, panel_walls, RL_list, DU_list):

    panel_man = stage['panel_man']
    boxes = stage['boxes']

    # man cannot access box from behind, so cannot move box
    if panel_man[boxes[b, 0] - box_move_0, boxes[b, 1] - box_move_1] == 0:
        return stages_all

    # wall in space ahead, so cannot move box
    if panel_walls[boxes[b, 0] + box_move_0, boxes[b, 1] + box_move_1] == 1:
        return stages_all

    boxes_new = boxes.copy()
    boxes_new[b, 0] = boxes[b, 0] + box_move_0
    boxes_new[b, 1] = boxes[b, 1] + box_move_1

    # already a box in space ahead, so cannot move this box there
    if is_in(boxes_new[b, :], boxes):
        return stages_all

    man_new = boxes[b, :]
#    panel_man_new = find_where_man_can_touch(panel_walls, boxes_new, man_new)
    panel_man_new = find_where_man_can_touch_2(panel_walls, boxes_new, man_new, RL_list, DU_list)
    seq_new = stage['seq'] + [len(stages_all)]
    moves_new = f"{stage['moves']},{move}{b}"

    already_in_list = False
    for st_ in stages_all:
        if are_same(st_['boxes'], boxes_new) and (st_['panel_man'] == panel_man_new).all():
            already_in_list = True
            #print(f'already found, rejecting: {moves_new}')
            break
    if not already_in_list:
        stages_all.append({'boxes': boxes_new, 'panel_man': panel_man_new, 'seq': seq_new,'moves': moves_new})

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

    # for b in range(B):
    #     panel_print[boxes[b, 0], boxes[b, 1]] += 2
    #     panel_print[jewels[b, 0], jewels[b, 1]] += 1

    print(f'seq: {stage['seq']}, moves: {stage["moves"]}')
    #print(panel_print)

    pp = [['XX' if c == 9 else '..' if c == 4 else '  ' for c in r] for r in panel_print]
    for b in range(B):
        pp[jewels[b, 0]][jewels[b, 1]] = 'oo'
    for b in range(B):
        pp[boxes[b, 0]][boxes[b, 1]] = 'B' + str(b)
    for p in pp:
        print(' '.join(p))


    f=0

def main_solve_puzzle_2(panel_in, verbose=False):
    # 0 - space
    # 2 - box
    # 4 - man
    # 1 - jewel
    # 9 - wall

    # i0 down/up
    # i1 right/left

    assert (panel_in == 1).sum() == (panel_in == 2).sum()
    assert (panel_in == 4).sum() == 1

    panel_walls = 1 * (panel_in == 9)
    jewels = np.argwhere(panel_in == 1)

    boxes_start = np.argwhere(panel_in == 2)
    B = len(boxes_start)
    man_start = np.argwhere(panel_in == 4)[0]
    man = man_start

    RL_list = []
    for row_i, row in enumerate(panel_walls):
        inds = np.where(row == 0)[0]
        if len(inds) >= 2:
            RL_list.append((row_i,
                            inds[np.where((inds[1:] - inds[:-1]) == 1)[0]],
                            (inds[np.where((inds[1:] - inds[:-1]) == 1)[0] + 1])[::-1]))

    DU_list = []
    for col_i, col in enumerate(panel_walls.transpose()):
        inds = np.where(col == 0)[0]
        if len(inds) >= 2:
            DU_list.append((col_i,
                            inds[np.where((inds[1:] - inds[:-1]) == 1)[0]],
                            (inds[np.where((inds[1:] - inds[:-1]) == 1)[0] + 1])[::-1]))

    panel_man_start = find_where_man_can_touch(panel_walls, boxes_start, man_start)
    stage = {'boxes': boxes_start, 'panel_man': panel_man_start, 'seq': [0], 'moves': 'S'}
    print_boxes(stage, panel_walls, jewels)

    boxes = stage['boxes']
    (N0, N1) = np.shape(panel_walls)
    panel_man = np.zeros([N0, N1], np.int8)
    panel_man[man[0], man[1]] = 1

    for c in range(3 * max(N0, N1)):
        sum_old = panel_man.sum()

        # look left and right
        for (row_i, R_inds, L_inds) in RL_list:
            if panel_man[row_i, :].sum() > 0:
                # looking right
                for i in R_inds:
                    if panel_man[row_i, i] == 1:
                        if not is_in(np.array([row_i, i + 1]), boxes):
                            panel_man[row_i, i + 1] = 1
                # looking left
                for i in L_inds:
                    if panel_man[row_i, i] == 1:
                        if not is_in(np.array([row_i, i - 1]), boxes):
                            panel_man[row_i, i - 1] = 1

        # look up and down
        for (col_i, D_inds, U_inds) in DU_list:
            if panel_man[:, col_i].sum() > 0:
                # looking down
                for i in D_inds:
                    if panel_man[i, col_i] == 1:
                        if not is_in(np.array([i + 1, col_i]), boxes):
                            panel_man[i + 1, col_i] = 1
                # looking up
                for i in U_inds:
                    if panel_man[i, col_i] == 1:
                        if not is_in(np.array([i - 1, col_i]), boxes):
                            panel_man[i - 1, col_i] = 1

        print(f'sum_old: {sum_old}; sum: {panel_man.sum()}')
        if sum_old == panel_man.sum():
            break

    a = 0

def main_solve_puzzle(panel_in, verbose=False):
    # 0 - space
    # 2 - box
    # 4 - man
    # 1 - jewel
    # 9 - wall

    V_lines = 100

    assert (panel_in == 1).sum() == (panel_in == 2).sum()
    assert (panel_in == 4).sum() == 1

    panel_walls = 1 * (panel_in == 9)
    jewels = np.argwhere(panel_in == 1)

    boxes_start = np.argwhere(panel_in == 2)
    B = len(boxes_start)
    man_start = np.argwhere(panel_in == 4)[0]
    panel_man_start = find_where_man_can_touch(panel_walls, boxes_start, man_start)
    stages_all = [{'boxes': boxes_start, 'panel_man': panel_man_start, 'seq': [0], 'moves': 'S'}]

    RL_list = []
    for row_i, row in enumerate(panel_walls):
        inds = np.where(row == 0)[0]
        if len(inds) >= 2:
            RL_list.append((row_i,
                            inds[np.where((inds[1:] - inds[:-1]) == 1)[0]],
                            (inds[np.where((inds[1:] - inds[:-1]) == 1)[0] + 1])[::-1]))

    DU_list = []
    for col_i, col in enumerate(panel_walls.transpose()):
        inds = np.where(col == 0)[0]
        if len(inds) >= 2:
            DU_list.append((col_i,
                            inds[np.where((inds[1:] - inds[:-1]) == 1)[0]],
                            (inds[np.where((inds[1:] - inds[:-1]) == 1)[0] + 1])[::-1]))

    most_left = np.where(panel_walls == 0)[0].min()
    most_right = np.where(panel_walls == 0)[0].max()
    most_top = np.where(panel_walls == 0)[1].min()
    most_bottom = np.where(panel_walls == 0)[1].max()
    jewels_most_left = (jewels[:, 0] == most_left).sum()
    jewels_most_right = (jewels[:, 0] == most_right).sum()
    jewels_most_top = (jewels[:, 1] == most_top).sum()
    jewels_most_bottom = (jewels[:, 1] == most_bottom).sum()

    print('starting panel')
    print_boxes(stages_all[0], panel_walls, jewels)

    for p in range(100000):
        len_stages_old = len(stages_all)

        if p >= len(stages_all):
            print('searched everything and failed')
            return

        stage = stages_all[p]
        boxes = stage['boxes']

        stuck_wall = ''
        if (boxes[:, 0] == most_left).sum() > jewels_most_left:
            stuck_wall = 'L'
        elif (boxes[:, 0] == most_right).sum() > jewels_most_right:
            stuck_wall = 'R'
        elif (boxes[:, 1] == most_top).sum() > jewels_most_top:
            stuck_wall = 'T'
        elif (boxes[:, 1] == most_bottom).sum() > jewels_most_bottom:
            stuck_wall = 'B'

        if len(stuck_wall) > 0:
            if verbose or ((p / V_lines) == np.round(p / V_lines)):
                print(f'total stages: {len(stages_all)}; done stage: {p}; stages left: {len(stages_all) - p}; moves: {stages_all[p]["moves"]}; stuck wall {stuck_wall} so killing stage')
            continue

        stuck_box = ''
        for box in boxes:
            if not is_in(box, jewels):
                if panel_walls[box[0] - 1, box[1]] + panel_walls[box[0], box[1] - 1] == 2:
                    stuck_box = 'TL'
                    break
                elif panel_walls[box[0] - 1, box[1]] + panel_walls[box[0], box[1] + 1] == 2:
                    stuck_box = 'TR'
                    break
                elif panel_walls[box[0] + 1, box[1]] + panel_walls[box[0], box[1] - 1] == 2:
                    stuck_box = 'BL'
                    break
                elif panel_walls[box[0] + 1, box[1]] + panel_walls[box[0], box[1] + 1] == 2:
                    stuck_box = 'BR'
                    break

        if len(stuck_box) > 0:
            if verbose or ((p / V_lines) == np.round(p / V_lines)):
                print(f'total stages: {len(stages_all)}; done stage: {p}; stages left: {len(stages_all) - p}; moves: {stages_all[p]["moves"]}; stuck box {stuck_box} so killing stage')
            continue

        for b in range(B):
            stages_all = find_and_add_new_stage(stages_all, stage, b, -1, +0, 'U', panel_walls, RL_list, DU_list)
            stages_all = find_and_add_new_stage(stages_all, stage, b, +0, +1, 'R', panel_walls, RL_list, DU_list)
            stages_all = find_and_add_new_stage(stages_all, stage, b, +1, +0, 'D', panel_walls, RL_list, DU_list)
            stages_all = find_and_add_new_stage(stages_all, stage, b, +0, -1, 'L', panel_walls, RL_list, DU_list)

        if verbose or ((p / V_lines) == np.round(p / V_lines)):
            moves_str = '; '.join([f'{p_c}:{stages_all[p_c]["moves"]}' for p_c in range(len_stages_old, len(stages_all))])
            print(f'total stages: {len(stages_all)}; done stage: {p}; stages left: {len(stages_all) - p}; moves: {stages_all[p]["moves"]}; added {len(stages_all) - len_stages_old} stages: {moves_str}')

        for p_c in range(len_stages_old, len(stages_all)):
            if are_same(stages_all[p_c]['boxes'], jewels):
                print('PUZZLE COMPLETE !!!')
                winning_seq = stages_all[p_c]['seq']
                for seq_c in winning_seq:
                    print_boxes(stages_all[seq_c], panel_walls, jewels)
                return stages_all[p_c]['moves']


def main_level_1(verbose):

    # 0 - space
    # 2 - box
    # 4 - man
    # 1 - jewel
    # 9 - wall

    X = np.array([[9, 9, 9, 9, 9, 9, 9, 9],
                  [9, 9, 9, 9, 1, 9, 9, 9],
                  [9, 9, 9, 9, 0, 9, 9, 9],
                  [9, 1, 0, 2, 2, 9, 9, 9],
                  [9, 9, 9, 0, 4, 2, 1, 9],
                  [9, 9, 9, 2, 9, 9, 9, 9],
                  [9, 9, 9, 1, 9, 9, 9, 9],
                  [9, 9, 9, 9, 9, 9, 9, 9]])

    return main_solve_puzzle(X, verbose)


def main_level_2(verbose):

    # 0 - space
    # 2 - box
    # 4 - man
    # 1 - jewel
    # 9 - wall

    X = np.array([[9, 9, 9, 9, 9, 9, 9, 9],
                  [9, 9, 4, 9, 9, 1, 1, 9],
                  [9, 0, 2, 2, 0, 2, 1, 9],
                  [9, 0, 0, 0, 2, 0, 0, 9],
                  [9, 9, 9, 9, 0, 0, 1, 9],
                  [9, 9, 9, 9, 9, 9, 9, 9]])

    return main_solve_puzzle(X, verbose)

def main_level_3(verbose):

    # 0 - space
    # 2 - box
    # 4 - man
    # 1 - jewel
    # 9 - wall

    X = np.array([[9, 9, 9, 9, 9, 9, 9, 9],
                  [9, 9, 9, 9, 1, 1, 1, 9],
                  [9, 0, 4, 9, 9, 2, 0, 9],
                  [9, 0, 2, 0, 0, 0, 0, 9],
                  [9, 9, 0, 9, 9, 0, 0, 9],
                  [9, 9, 0, 0, 0, 9, 2, 9],
                  [9, 9, 9, 9, 0, 0, 0, 9],
                  [9, 9, 9, 9, 9, 9, 9, 9]])

    return main_solve_puzzle(X, verbose)


def main_level_4(verbose):

    # 0 - space
    # 2 - box
    # 4 - man
    # 1 - jewel
    # 9 - wall

    X = np.array([[9, 9, 9, 9, 9, 9, 9],
                  [9, 9, 9, 4, 0, 9, 9],
                  [9, 9, 9, 0, 2, 9, 9],
                  [9, 0, 2, 2, 1, 9, 9],
                  [9, 0, 2, 1, 1, 0, 9],
                  [9, 0, 2, 1, 1, 0, 9],
                  [9, 0, 0, 0, 9, 9, 9],
                  [9, 9, 9, 9, 9, 9, 9]])

    return main_solve_puzzle(X, verbose)


def main_level_5(verbose):

    # 0 - space
    # 2 - box
    # 4 - man
    # 1 - jewel
    # 9 - wall

    X = np.array([[9, 9, 9, 9, 9, 9, 9],
                  [9, 9, 0, 0, 9, 9, 9],
                  [9, 9, 0, 0, 9, 9, 9],
                  [9, 0, 2, 1, 0, 0, 9],
                  [9, 0, 1, 2, 0, 0, 9],
                  [9, 0, 4, 1, 2, 0, 9],
                  [9, 0, 9, 0, 0, 9, 9],
                  [9, 9, 9, 9, 9, 9, 9]])

    return main_solve_puzzle(X, verbose)

def main_level_6(verbose):

    # 0 - space
    # 2 - box
    # 4 - man
    # 1 - jewel
    # 9 - wall

    X = np.array([[9, 9, 9, 9, 9, 9, 9, 9],
                  [9, 9, 9, 9, 9, 0, 0, 9],
                  [9, 9, 9, 0, 0, 0, 0, 9],
                  [9, 1, 0, 0, 2, 9, 4, 9],
                  [9, 1, 1, 2, 0, 2, 0, 9],
                  [9, 9, 9, 1, 0, 2, 0, 9],
                  [9, 9, 9, 9, 9, 0, 0, 9],
                  [9, 9, 9, 9, 9, 9, 9, 9]])

    return main_solve_puzzle(X, verbose)

def main_level_7(verbose):

    # 0 - space
    # 2 - box
    # 4 - man
    # 1 - jewel
    # 9 - wall

    X = np.array([[9, 9, 9, 9, 9, 9, 9],
                  [9, 0, 0, 4, 0, 0, 9],
                  [9, 0, 2, 2, 2, 0, 9],
                  [9, 9, 1, 1, 1, 9, 9],
                  [9, 0, 1, 0, 1, 0, 9],
                  [9, 0, 2, 0, 2, 0, 9],
                  [9, 0, 0, 9, 0, 0, 9],
                  [9, 9, 9, 9, 9, 9, 9]])

    return main_solve_puzzle(X, verbose)

def main_level_8(verbose):

    # 0 - space
    # 2 - box
    # 4 - man
    # 1 - jewel
    # 9 - wall

    X = np.array([[9, 9, 9, 9, 9, 9, 9],
                  [9, 9, 0, 1, 0, 9, 9],
                  [9, 0, 2, 1, 2, 9, 9],
                  [9, 0, 0, 1, 2, 0, 9],
                  [9, 0, 2, 1, 0, 4, 9],
                  [9, 0, 2, 1, 2, 9, 9],
                  [9, 9, 0, 1, 0, 9, 9],
                  [9, 9, 9, 9, 9, 9, 9]])

    return main_solve_puzzle(X, verbose)

def main_level_9(verbose):

    # 0 - space
    # 2 - box
    # 4 - man
    # 1 - jewel
    # 9 - wall

    X = np.array([[9, 9, 9, 9, 9, 9, 9, 9],
                  [9, 1, 1, 0, 0, 0, 0, 9],
                  [9, 1, 1, 2, 0, 2, 0, 9],
                  [9, 2, 9, 2, 2, 2, 9, 9],
                  [9, 1, 1, 2, 0, 2, 4, 9],
                  [9, 1, 1, 0, 0, 0, 0, 9],
                  [9, 9, 9, 9, 9, 9, 9, 9]])

    # XX XX XX XX XX XX XX XX
    # XX oo oo             XX
    # XX oo oo B0    B1    XX
    # XX B2 XX B3 B4 B5 XX XX
    # XX oo oo B6 .. B7 .. XX
    # XX oo oo .. .. .. .. XX
    # XX XX XX XX XX XX XX XX

    return main_solve_puzzle(X, verbose)

def main_test():
    assert main_level_1(verbose=False) == 'S,U1,L0,L0,U1,R2,D3'
    assert main_level_2(verbose=False) == 'S,D0,R1,R3,R3,R0,R0,R0,U3,U3,R0,U2,D0,R1,R1'
    assert main_level_3(verbose=False) == 'S,R1,R1,U2,U2,U0,L0,R1,U1,U1,U2,U2'
    assert main_level_4(verbose=False) == 'S,D0,D0,D0,U2,R3,R3,D2,D2,D2,R1,R1,U2,R4'
    assert main_level_5(verbose=False) == 'S,U0,D0,D0,U1,L2'
    assert main_level_6(verbose=False) == 'S,D0,D0,L1,L1,L2,L2,L2,U3,R0,L3,L3,U3,L0,L0,L3,L3'
    assert main_level_7(verbose=False) == 'S,D0,D2,R1,U3,U4,L1,D1'
    assert main_level_8(verbose=False) == 'S,R0,R4,U3,L2,D1,D1,L1,L1,U5,U5,D4,R1,D3,D5,L2,U0,U1,U1,L5,R2,D3,R3'
    #assert main_level_9(verbose=False) == ''

    

if __name__ == "__main__":

    main_level_9(False)

#    main_test()

