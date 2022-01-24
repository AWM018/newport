#!/usr/bin/python3
# -*- coding: utf-8 -*-

import plac
import bz2
import requests
import tqdm
import time
import os
import datetime as dt
import json


def main(
    no_compr : ("Do not pass compression algorithms in request's Accept-Encoding field", 'flag', '0'),
    from_addr : ("Start processing records from entry with this address", 'option', 'f', str, None, 'ADDDRESS'),
    epoch_cutoff : ("Epoch cut-off for address filtering. Only addresses"
                    " with the most recent transaction timestamp before"
                    " this cut-off value will be considered."
                    " 2013-09-01=1377993600"
                    " 2013-10-01=1380585600"
                    " 2013-11-01=1383264000"
                    " 2013-12-01=1385856000"
                    " 2014-01-01=1388534400",
                    'option', 'e', int, None)=1388534400,
    skip : ("Skip N lines from TSV file", 'option', 's', int, None)=0,
    delay : ("Delay, in seconds, between API calls", 'option', 'd', float, None)=5.0,
    tsv_file : ("Bzipped2 TSV file with addresses and balances", 'positional', None, str)='data/balances.tsv.bz2'
    ):

    assert skip >= 0, "Skip option value must be greater or equal 0."

    with bz2.open(tsv_file, 'rt') as ifile:
        records = [line.split('\t') for line in ifile.readlines()]

    print(f"[i] Read {len(records)} data entries from {tsv_file} .")

    if from_addr:
        from_idx = next((i for i, t in enumerate(records) if t[0] == from_addr), None)
        if from_idx is not None:
            print(f"[i] Will start from record {from_idx} with address {from_addr}")
        else:
            print(f"[w] Address {from_addr} was not found in input records.")
            from_idx = 0
        pass
    else:
        from_idx = 0

    if skip:
        print(f"[i] Will skip {skip} records.")

    try:
        os.makedirs('.logs')
    except FileExistsError:
        pass
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    now_iso = dt.datetime.fromtimestamp(time.time()).isoformat()
    ofname = f'.logs/result_{now_iso}.log'

    found = []
    progress = tqdm.tqdm(records[(from_idx + skip):])
    for address, balance in progress:
        if not (address.startswith('1') or address.startswith('3') or address.startswith('bc1q')):
            # skip non-compliant addresses
            continue

        fbalance = int(balance) / 1e8
        progress.set_description(f"{address:<62}  {fbalance:.8f}")

        url = f'https://blockchain.info/rawaddr/{address}'
        if no_compr:
            headers = {}
        else:
            headers = {'Accept-Encoding': 'deflate, br'}

        for _ in range(5): # 5 retries
            try:
                res = requests.get(url, headers=headers)
            except requests.exceptions.ChunkedEncodingError:
                time.sleep(delay)
                pass

            if 500 <= res.status_code and res.status_code <= 599:
                # 502 The web server reported a bad gateway error.
                # 524 The origin web server timed out responding to this request.
                # Please try again in a few minutes.
                time.sleep(delay)
                continue

            assert res.status_code == 200, f"Request error: {res}\n{res.text}"

            try:
                jres = res.json()
                break
            except json.decoder.JSONDecodeError:
                time.sleep(delay)
                pass

        txs = sorted(jres['txs'], key=lambda t: -t['time'])

        if len(txs) == 0:
            print(f"[!] Received empty transaction list for address {address}.")
            continue

        ts = txs[0]['time']
        if ts < epoch_cutoff:
            print(f'[i] Candidate found: {dt.datetime.utcfromtimestamp(ts).isoformat()} {address} {fbalance:.8f}')
            found.append((address, balance))
            with open(ofname, 'a') as ofile:
                ofile.write(f'{dt.datetime.utcfromtimestamp(ts).isoformat()} {address:<62} {fbalance:.8f}\n')

        time.sleep(delay)
        pass

    pass


if __name__ == "__main__":
    plac.call(main)
