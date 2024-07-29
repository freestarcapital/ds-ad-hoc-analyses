import numpy as np
import datetime as dt

class Puzzle:
    def __init__(self):
        pass
    def find_where_man_can_touch(self, boxes, man):

        boxes_compact = self.boxes_to_boxes_compact(boxes)

        panel_man = np.zeros([self.N_DU, self.N_RL], np.int8)
        panel_man[man[0], man[1]] = 1

        for c in range(3 * max(self.N_DU, self.N_RL)):
            sum_old = panel_man.sum()

            # look left and right
            for (row_i, R_inds, L_inds) in self.RL_list:
                if panel_man[row_i, :].sum() > 0:
                    # looking right
                    for i in R_inds:
                        if panel_man[row_i, i] == 1:
                            if self.box_to_box_compact(np.array([row_i, i + 1])) not in boxes_compact:
                                panel_man[row_i, i + 1] = 1
                    # looking left
                    for i in L_inds:
                        if panel_man[row_i, i] == 1:
                            if self.box_to_box_compact(np.array([row_i, i - 1])) not in boxes_compact:
                                panel_man[row_i, i - 1] = 1

            # look up and down
            for (col_i, D_inds, U_inds) in self.DU_list:
                if panel_man[:, col_i].sum() > 0:
                    # looking down
                    for i in D_inds:
                        if panel_man[i, col_i] == 1:
                            if self.box_to_box_compact(np.array([i + 1, col_i])) not in boxes_compact:
                                panel_man[i + 1, col_i] = 1
                    # looking up
                    for i in U_inds:
                        if panel_man[i, col_i] == 1:
                            if self.box_to_box_compact(np.array([i - 1, col_i])) not in boxes_compact:

                                panel_man[i - 1, col_i] = 1

            #print(f'sum_old: {sum_old}; sum: {panel_man.sum()}')
            if sum_old == panel_man.sum():
                return panel_man

        assert False, "incomplete panel_man"

    def find_and_add_new_stage(self, stage, b, box_move_0, box_move_1, move):

        panel_man = stage['panel_man']
        boxes = stage['boxes']
        boxes_compact = self.boxes_to_boxes_compact(boxes)

        # man cannot access box from behind, so cannot move box
        if panel_man[boxes[b, 0] - box_move_0, boxes[b, 1] - box_move_1] == 0:
            return

        # wall in space ahead, so cannot move box
        if self.panel_walls[boxes[b, 0] + box_move_0, boxes[b, 1] + box_move_1] == 1:
            return

        boxes_new = boxes.copy()
        boxes_new[b, 0] = boxes[b, 0] + box_move_0
        boxes_new[b, 1] = boxes[b, 1] + box_move_1
        box_new = boxes_new[b, :]
        box_compact_new = self.box_to_box_compact(box_new)

        # already a box in space ahead, so cannot move this box there
        if box_compact_new in boxes_compact:
            return

        # if box in is a corner, and it's not over a jewel impossible, so return
        if box_compact_new not in self.jewels_compact:
            if self.panel_walls[box_new[0] - 1, box_new[1]] + self.panel_walls[box_new[0], box_new[1] - 1] == 2:
                return
            elif self.panel_walls[box_new[0] - 1, box_new[1]] + self.panel_walls[box_new[0], box_new[1] + 1] == 2:
                return
            elif self.panel_walls[box_new[0] + 1, box_new[1]] + self.panel_walls[box_new[0], box_new[1] - 1] == 2:
                return
            elif self.panel_walls[box_new[0] + 1, box_new[1]] + self.panel_walls[box_new[0], box_new[1] + 1] == 2:
                return

        # in a lines adjacent to a wall, if there are more boxes than jewels impossible, so return
        if (boxes_new[:, 1] == self.most_left).sum() > self.jewels_most_left:
            return
        elif (boxes_new[:, 1] == self.most_right).sum() > self.jewels_most_right:
            return
        elif (boxes_new[:, 0] == self.most_top).sum() > self.jewels_most_top:
            return
        elif (boxes_new[:, 0] == self.most_bottom).sum() > self.jewels_most_bottom:
            return

        moves_new = f"{stage['moves']},{move}{b}"

        # if a line adjacent to a wall has fewer jewels than spaces, and that line cannot be reached (man on other side) and
        # when testing visualise using: self.print_boxes({'boxes': boxes_new, 'panel_man': panel_man, 'seq': '', 'moves': ''})
        panel_man = stage['panel_man']
        if ((panel_man[:, self.most_left].sum() == 0)
                and ((self.panel_walls[:, self.most_left + 1] == 0).sum() == (boxes_new[:, 1] == self.most_left + 1).sum())
                and (self.jewels_most_left < (self.panel_walls[:, self.most_left] == 0).sum() - (boxes_new[:, 1] == self.most_left).sum())):
            if moves_new in 'S,U2,U2,L6,L0,L0,L1,D3,D3,L3,L1,D4,U1,L1,L3,U4,U4,L4,L6,L7,U5,D7,L7,L7':
                f = 0
            return
        if ((panel_man[:, self.most_right].sum() == 0)
                and ((self.panel_walls[:, self.most_right - 1] == 0).sum() == (boxes_new[:, 1] == self.most_right - 1).sum())
                and (self.jewels_most_right < (self.panel_walls[:, self.most_right] == 0).sum() - (boxes_new[:, 1] == self.most_right).sum())):
            if moves_new in 'S,U2,U2,L6,L0,L0,L1,D3,D3,L3,L1,D4,U1,L1,L3,U4,U4,L4,L6,L7,U5,D7,L7,L7':
                f = 0
            return
        if ((panel_man[self.most_top, :].sum() == 0)
                and ((self.panel_walls[self.most_top + 1, :] == 0).sum() == (boxes_new[:, 0] == self.most_top + 1).sum())
                and (self.jewels_most_top < (self.panel_walls[self.most_top, :] == 0).sum() - (boxes_new[:, 0] == self.most_top).sum())):
            if moves_new in 'S,U2,U2,L6,L0,L0,L1,D3,D3,L3,L1,D4,U1,L1,L3,U4,U4,L4,L6,L7,U5,D7,L7,L7':
                f = 0
            return
        if ((panel_man[self.most_bottom, :].sum() == 0)
                and ((self.panel_walls[self.most_bottom - 1, :] == 0).sum() == (boxes_new[:, 0] == self.most_bottom - 1).sum())
                and (self.jewels_most_bottom < (self.panel_walls[self.most_bottom, :] == 0).sum() - (boxes_new[:, 0] == self.most_bottom).sum())):
            print('isolated wall B')
            if moves_new in 'S,U2,U2,L6,L0,L0,L1,D3,D3,L3,L1,D4,U1,L1,L3,U4,U4,L4,L6,L7,U5,D7,L7,L7':
                f = 0
            return

        man_new = boxes[b, :]
        panel_man_new = self.find_where_man_can_touch(boxes_new, man_new)
        seq_new = stage['seq'] + [len(self.stages_all)]
        moves_new = f"{stage['moves']},{move}{b}"
        stage_new = {'boxes': boxes_new, 'panel_man': panel_man_new, 'seq': seq_new, 'moves': moves_new}
        stage_compact_new = self.stage_to_stage_compact(stage_new)

        if stage_compact_new in self.stages_compact_all:
            return

        self.stages_all.append(stage_new)
        self.stages_compact_all.append(stage_compact_new)
#        self.stages_compact_all.add(stage_compact_new)

        if self.boxes_to_boxes_compact_code(boxes_new) == self.jewels_compact_code:
            print('PUZZLE COMPLETE !!!')
            for seq_c in seq_new:
                self.print_boxes(self.stages_all[seq_c])
            self.stage_winning = stage_new

    def print_boxes(self, stage):
        # 0 - space
        # 2 - box
        # 4 - panel_man
        # +1 - jewel
        # 9 - wall

        boxes = stage['boxes']
        B = len(boxes)
        panel_print = 9 * self.panel_walls.copy() + 4 * stage['panel_man'].copy()

        print(f'seq: {stage['seq']}, moves: {stage["moves"]}')
        pp = [['XX' if c == 9 else '..' if c == 4 else '  ' for c in r] for r in panel_print]
        for b in range(B):
            pp[self.jewels[b, 0]][self.jewels[b, 1]] = 'oo'
        for b in range(B):
            pp[boxes[b, 0]][boxes[b, 1]] = 'B' + str(b)
        for p in pp:
            print(' '.join(p))

    def box_to_box_compact(self, boxes):
        return boxes[0] * self.N_RL + boxes[1]

    def boxes_to_boxes_compact(self, boxes):
        return boxes[:, 0] * self.N_RL + boxes[:, 1]

    def boxes_to_boxes_compact_code(self, boxes, return_M=False):
        boxes_flatten = self.boxes_to_boxes_compact(boxes)
        boxes_flatten.sort()
        Z = 0
        M = 1
        for b in boxes_flatten:
            Z += (M * b)
            M *= (self.N_RL * self.N_DU)

        if return_M:
            return M, Z
        else:
            return Z
    def stage_to_stage_compact(self, stage):
        M, Z = self.boxes_to_boxes_compact_code(stage['boxes'], True)
        panel_man_first = np.argwhere(stage['panel_man'])[0]
        Z += (M * (panel_man_first[0] * self.N_RL + panel_man_first[1]))
        return Z

    def puzzle_init(self):
        self.V_lines = 1000

        assert (self.panel_in == 1).sum() == (self.panel_in == 2).sum()
        assert (self.panel_in == 4).sum() == 1

        self.stage_winning = None

        self.panel_walls = 1 * (self.panel_in == 9)
        (self.N_DU, self.N_RL) = np.shape(self.panel_walls)
        self.jewels = np.argwhere(self.panel_in == 1)
        self.jewels_compact = self.boxes_to_boxes_compact(self.jewels)
        self.jewels_compact_code = self.boxes_to_boxes_compact_code(self.jewels)

        self.RL_list = []
        for row_i, row in enumerate(self.panel_walls):
            inds = np.where(row == 0)[0]
            if len(inds) >= 2:
                self.RL_list.append((row_i,
                                inds[np.where((inds[1:] - inds[:-1]) == 1)[0]],
                                (inds[np.where((inds[1:] - inds[:-1]) == 1)[0] + 1])[::-1]))

        self.DU_list = []
        for col_i, col in enumerate(self.panel_walls.transpose()):
            inds = np.where(col == 0)[0]
            if len(inds) >= 2:
                self.DU_list.append((col_i,
                                inds[np.where((inds[1:] - inds[:-1]) == 1)[0]],
                                (inds[np.where((inds[1:] - inds[:-1]) == 1)[0] + 1])[::-1]))

        boxes_start = np.argwhere(self.panel_in == 2)
        self.B = len(boxes_start)
        man_start = np.argwhere(self.panel_in == 4)[0]
        panel_man_start = self.find_where_man_can_touch(boxes_start, man_start)
        stage = {'boxes': boxes_start, 'panel_man': panel_man_start, 'seq': [0], 'moves': 'S'}
        self.stages_all = [stage]
        self.stages_compact_all = [self.stage_to_stage_compact(stage)]

        # from sortedcontainers import SortedList
        # self.stages_compact_all = SortedList([self.stage_to_stage_compact(stage)])

        self.most_left = np.where(self.panel_walls == 0)[1].min()
        self.most_right = np.where(self.panel_walls == 0)[1].max()
        self.most_top = np.where(self.panel_walls == 0)[0].min()
        self.most_bottom = np.where(self.panel_walls == 0)[0].max()
        self.jewels_most_left = (self.jewels[:, 1] == self.most_left).sum()
        self.jewels_most_right = (self.jewels[:, 1] == self.most_right).sum()
        self.jewels_most_top = (self.jewels[:, 0] == self.most_top).sum()
        self.jewels_most_bottom = (self.jewels[:, 0] == self.most_bottom).sum()

        print('starting panel')
        self.print_boxes(self.stages_all[0])

    def main_solve_puzzle(self, panel_in, verbose=False):
        # 0 - space
        # 2 - box
        # 4 - man
        # 1 - jewel
        # 9 - wall

        self.panel_in = panel_in
        self.verbose = verbose
        self.puzzle_init()

        time_puzzle_start = dt.datetime.now()
        time_old = time_puzzle_start
        for p in range(100000):
            len_stages_old = len(self.stages_all)

            if p >= len(self.stages_all):
                print('searched everything and failed')
                return

            for b in range(self.B):
                self.find_and_add_new_stage(self.stages_all[p], b, -1, +0, 'U')
                self.find_and_add_new_stage(self.stages_all[p], b, +0, +1, 'R')
                self.find_and_add_new_stage(self.stages_all[p], b, +1, +0, 'D')
                self.find_and_add_new_stage(self.stages_all[p], b, +0, -1, 'L')

            if verbose or ((p / self.V_lines) == np.round(p / self.V_lines)):
                time_new = dt.datetime.now()
                moves_str = '; '.join([f'{p_c}:{self.stages_all[p_c]["moves"]}' for p_c in range(len_stages_old, len(self.stages_all))])
                print(f'time taken: {(time_new - time_old).seconds:0.1f}s; total stages: {len(self.stages_all)}; done stage: {p}; stages left: {len(self.stages_all) - p - 1}; moves: {self.stages_all[p]["moves"]}; added {len(self.stages_all) - len_stages_old} stages: {moves_str}')
                time_old = time_new

            if self.stage_winning is not None:
                print(f'total puzzle solve time: {(dt.datetime.now() - time_puzzle_start).seconds:0.1f}s')
                return self.stage_winning['moves']


def main_solve_puzzle(panel_in, verbose):
    puzzle = Puzzle()
    return puzzle.main_solve_puzzle(panel_in, verbose)

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

def main_level_test_isolated(verbose):

    # 0 - space
    # 2 - box
    # 4 - man
    # 1 - jewel
    # 9 - wall

    X1 = np.array([[9, 9, 9, 9, 9, 9, 9, 9],
                  [9, 0, 0, 2, 0, 0, 1, 9],
                  [9, 0, 2, 0, 0, 4, 1, 9],
                  [9, 0, 9, 0, 0, 0, 9, 9],
                  [9, 0, 2, 0, 0, 0, 1, 9],
                  [9, 0, 2, 0, 0, 0, 1, 9],
                  [9, 9, 9, 9, 9, 9, 9, 9]])

    X2 = np.array([[9, 9, 9, 9, 9, 9, 9, 9],
                  [9, 0, 1, 0, 2, 0, 0, 9],
                  [9, 4, 1, 0, 0, 2, 0, 9],
                  [9, 0, 9, 9, 0, 9, 9, 9],
                  [9, 0, 1, 0, 0, 2, 0, 9],
                  [9, 0, 1, 0, 0, 2, 0, 9],
                  [9, 9, 9, 9, 9, 9, 9, 9]])


    # main_solve_puzzle(X1, verbose)
    # main_solve_puzzle(X2, verbose)
    main_solve_puzzle(X1.transpose(), verbose)
    main_solve_puzzle(X2.transpose(), verbose)


def main_test():
    assert main_level_1(verbose=False) == 'S,U1,L0,L0,U1,R2,D3'
    assert main_level_2(verbose=False) == 'S,D0,R1,R3,R3,R0,R0,R0,U3,U3,R0,U2,D0,R1,R1'
    assert main_level_3(verbose=False) == 'S,R1,R1,U2,U2,U0,L0,R1,U1,U1,U2,U2'
    assert main_level_4(verbose=False) == 'S,D0,D0,D0,U2,R3,R3,D2,D2,D2,R1,R1,U2,R4'
    assert main_level_5(verbose=False) == 'S,U0,D0,D0,U1,L2'
    assert main_level_6(verbose=False) == 'S,D0,D0,L1,L1,L2,L2,L2,U3,R0,L3,L3,U3,L0,L0,L3,L3'
    assert main_level_7(verbose=False) == 'S,D0,D2,R1,U3,U4,L1,D1'
    assert main_level_8(verbose=False) == 'S,R0,R4,U3,L2,D1,D1,L1,L1,U5,U5,D4,R1,D3,D5,L2,U0,U1,U1,L5,R2,D3,R3'
    assert main_level_9(verbose=False) == 'S,U2,U2,L6,L0,L0,L1,D3,D3,L3,L1,D4,U1,L1,L3,U4,U4,L4,L6,L7,U5,D7,L7,L7'

if __name__ == "__main__":

#    main_level_test_isolated(True)#
    main_level_9(False)

    #main_test()

