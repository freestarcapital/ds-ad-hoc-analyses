
import dateutil.utils
import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
import configparser
from google.cloud import bigquery_storage
import os, sys
import numpy as np
import datetime

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

np.set_printoptions(suppress=True, linewidth=10000)


def main(force_recalc=False):

    N = 100000

    data_cache_filename = f'data_cache/results_list_{N}.pkl'
    if force_recalc or not os.path.exists(data_cache_filename):
        with open(data_cache_filename, 'wb') as f:
            results_list = [run_sim_and_plot(cam_bid_prop_offset, N) for cam_bid_prop_offset in np.arange(-0.6, 0.1, 0.1)]
            pickle.dump(results_list, f)

    with open(data_cache_filename, 'rb') as f:
        results_list = pickle.load(f)

    fig, ax = plt.subplots(figsize=(12, 9), ncols=2)
    for r in results_list:
        for ax_i, (c, i) in enumerate([('pp_sens', 'price_pressure_perc'), ('fr_sens', 'fill_rate')]):
            ax_ = ax[ax_i]
            x = pd.DataFrame(r[c]).transpose().set_index(i).rename(columns={'cpma_loss_perc': f'bid_offset: {r["bid_prop_offset"]:0.1f}'})
            x.plot(style='x-', ax=ax_, ylim=[-15, 0], ylabel='cpma_loss_perc')

    fig.suptitle('Optimal floor price setting using fill rate vs price pressure')
    fig.savefig(f'plots/sensitivity_{N}.png')

    f = 0




def run_sim_and_plot(cam_bid_prop_offset,
                     N = 100000,
                     floor_prices = np.arange(0, 2, 0.002)):

    print(f'doing run_sim_and_plot with: cam_bid_prop_offset: {cam_bid_prop_offset:0.1f}, N: {N}')

    results_dict_list = [run_sim(fp, N, cam_bid_prop_offset) for fp in floor_prices]

    df = pd.DataFrame(results_dict_list).set_index('floor_price')
    df_y = pd.DataFrame(index=df.index)
    df_y['cpma_loss_perc'] = (df['cpma'] / df['cpma'].max() - 1) * 100
    df_y['price_pressure_perc'] = (df['cpma_accept_low_bids'] / df['cpma'] - 1) * 100

    opt_floor_price = df['cpma'].idxmax()
    opt_vals = df.loc[opt_floor_price]
    opt_pressure = opt_vals['cpma_accept_low_bids'] / opt_vals['cpma'] - 1
    opt_fill_rate = opt_vals['fill_rate']

    fig, ax = plt.subplots(figsize=(12, 9))
    df.plot(ax=ax, title=f'bid_prop_offset: {cam_bid_prop_offset:0.1f}, optimal: floor_price: {opt_floor_price:0.2f}, pressure: {opt_pressure*100:0.1f}%, fill_rate: {opt_fill_rate*100:0.1f}%')
    df_y.plot(ax=ax, secondary_y=True, ylim=[-20, 20])
    fig.savefig(f'plots/price_pressure_bid_prop_offset_{cam_bid_prop_offset*100:0.0f}_{N}.png')

    df = pd.concat([df, df_y], axis=1)

    pp_sens_range = np.arange(10, 30, 2)
    fr_sens_range = np.arange(0.5, 0.9, 0.05)

    pp_sens = pd.concat([df.loc[abs(df['price_pressure_perc'] - pp_val).idxmin()][['cpma_loss_perc', 'price_pressure_perc']] for pp_val in pp_sens_range], axis=1)
    fr_sens = pd.concat([df.loc[abs(df['fill_rate'] - fr).idxmin()][['cpma_loss_perc', 'fill_rate']] for fr in fr_sens_range], axis=1)

    return {'bid_prop_offset': cam_bid_prop_offset,
            'pp_sens': pp_sens,
            'fr_sens': fr_sens}


def run_sim(
    floor_price=1,
    N = 100000,
    cam_bid_prop_offset = - 0.4,
    cam_bid_low = [0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1],
    cam_bid_low_to_high = [0.05, 0.1, 0.1, 0.15, 0.15, 0.2, 0.2, 0.3, 0.4, 0.5, 0.5],
    cam_bid_prop = [1, 1, 0.9, 0.8, 0.75, 0.7, 0.65, 0.6, 0.55, 0.5, 0.4]
    ):

    assert cam_bid_prop_offset >= - 0.6
    assert cam_bid_prop_offset <= 0.2

    cam_bid_low = [0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
    cam_bid_low_to_high = [0.05, 0.1, 0.1, 0.15, 0.15, 0.2, 0.2, 0.3, 0.4, 1, 1]
    cam_bid_prop = [1, 1, 0.9, 0.8, 0.75, 0.7, 0.7, 0.65, 0.6, 0.6, 0.5]


#    print(f'running run_sim with floor_price={floor_price}')

    C = len(cam_bid_prop)
    assert len(cam_bid_low) == len(cam_bid_prop)
    assert len(cam_bid_low) == len(cam_bid_low_to_high)

    cam_bid_low_A = np.array(cam_bid_low).reshape([1, C]).repeat(N, 0)
    cam_bid_low_to_high_A = np.array(cam_bid_low_to_high).reshape([1, C]).repeat(N, 0)
    cam_bid_prop_A = np.array(cam_bid_prop).reshape([1, C]).repeat(N, 0) + cam_bid_prop_offset
    #accept_low_bids_prop = 0.1

    cam_bids = cam_bid_low_to_high_A * np.random.random([N, C]) + cam_bid_low_A
    cam_no_bids = np.random.random([N, C]) > cam_bid_prop_A
    cam_bids[cam_no_bids] = 0
    #accept_low_bids = np.random.random(N) <= accept_low_bids_prop

    results_list = []
    for n in range(N):
     #   accept_low_bids_n = accept_low_bids[n]
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
      #      if accept_low_bids_n:

            filled_accept_low_bids = 1
            winning_bid_accept_low_bids = bids.max()
            # else:
            #     filled_accept_low_bids = 0
            #     winning_bid_accept_low_bids = 0

        results = {#'accept_low_bids': accept_low_bids_n,
                   'winning_bid': winning_bid,
                   'winning_bid_accept_low_bids': winning_bid_accept_low_bids,
                   'filled_accept_low_bids': filled_accept_low_bids,
                   'filled': filled}

        results_list.append(results)

#    df = pd.concat([pd.DataFrame(cam_bids), pd.DataFrame(results_list)], axis=1)
    df = pd.DataFrame(results_list)
   # df_accept_low_bids = df[df['accept_low_bids']]

    return {'floor_price': floor_price,
                    'cpma': df['winning_bid'].mean(),
                    'fill_rate': df['filled'].mean(),
                    'cpma_accept_low_bids': df['winning_bid_accept_low_bids'].mean(),
                    'fill_rate_accept_low_bids': df['filled_accept_low_bids'].mean()}#,
                    #'cpma_accept_low_bids_only': df_accept_low_bids['winning_bid_accept_low_bids'].mean(),
                    #'fill_rate_accept_low_bids_only': df_accept_low_bids['filled_accept_low_bids'].mean()}



if __name__ == "__main__":
    main()

