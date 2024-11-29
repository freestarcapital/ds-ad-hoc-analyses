
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

np.set_printoptions(suppress=True, linewidth=10000)


def main():
    floor_prices = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 1.0, 1.1, 1.2, 1.3, 1.4]
    results_dict_list = [run_sim(fp) for fp in floor_prices]

    df = pd.DataFrame(results_dict_list).set_index('floor_price')

    f = 0

def run_sim(
    floor_price=1,
    cam_bid_low = [0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1],
    cam_bid_low_to_high = [0.05, 0.1, 0.1, 0.15, 0.15, 0.2, 0.2, 0.3, 0.4, 0.5, 0.5],
    cam_bid_prop = [1, 1, 0.9, 0.8, 0.75, 0.7, 0.65, 0.6, 0.55, 0.5, 0.4],
    N = 10000):

    print(f'running run_sim with floor_price={floor_price}')

    C = len(cam_bid_prop)
    assert len(cam_bid_low) == len(cam_bid_prop)
    assert len(cam_bid_low) == len(cam_bid_low_to_high)

    cam_bid_low_A = np.array(cam_bid_low).reshape([1, C]).repeat(N, 0)
    cam_bid_low_to_high_A = np.array(cam_bid_low_to_high).reshape([1, C]).repeat(N, 0)
    cam_bid_prop_A = np.array(cam_bid_prop).reshape([1, C]).repeat(N, 0)
    accept_low_bids_prop = 0.1

    cam_bids = cam_bid_low_to_high_A * np.random.random([N, C]) + cam_bid_low_A
    cam_no_bids = np.random.random([N, C]) > cam_bid_prop_A
    cam_bids[cam_no_bids] = 0
    accept_low_bids = np.random.random(N) <= accept_low_bids_prop

    results_list = []
    for n in range(N):
        accept_low_bids_n = accept_low_bids[n]
        bids = cam_bids[n, :]
        bids_satisfying_floor_price = bids[bids >= floor_price]
        if len(bids_satisfying_floor_price) > 0:
            winning_bid = bids_satisfying_floor_price.min()
            winning_bid_accept_low_bids = winning_bid
            filled = 1
            filled_accept_low_bids = 1
        else:
            winning_bid = 0
            filled = 0
            if accept_low_bids_n:

                filled_accept_low_bids = 1
                winning_bid_accept_low_bids = bids.max()
            else:
                filled_accept_low_bids = 0
                winning_bid_accept_low_bids = 0

        results = {'accept_low_bids': accept_low_bids_n,
                   'winning_bid': winning_bid,
                   'winning_bid_accept_low_bids': winning_bid_accept_low_bids,
                   'filled_accept_low_bids': filled_accept_low_bids,
                   'filled': filled}

        results_list.append(results)

#    df = pd.concat([pd.DataFrame(cam_bids), pd.DataFrame(results_list)], axis=1)
    df = pd.DataFrame(results_list)
    df_accept_low_bids = df[df['accept_low_bids']]

    return {'floor_price': floor_price,
                    'cpma': df['winning_bid'].mean(),
                    'fill_rate': df['filled'].mean(),
                    'cpma_accept_low_bids': df['winning_bid_accept_low_bids'].mean(),
                    'fill_rate_accept_low_bids': df['filled_accept_low_bids'].mean(),
                    'cpma_accept_low_bids_only': df_accept_low_bids['winning_bid_accept_low_bids'].mean(),
                    'fill_rate_accept_low_bids_only': df_accept_low_bids['filled_accept_low_bids'].mean()}



if __name__ == "__main__":
    main()

