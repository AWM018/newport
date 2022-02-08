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


def to_hash160(pubkey):
    import hashlib
    sha = hashlib.sha256()
    rip = hashlib.new('ripemd160')
    hex_str = bytearray.fromhex(pubkey)
    sha.update(hex_str)
    rip.update(sha.digest())
    return rip.hexdigest()


def pull_data(url, delay):
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

    return jres


def main(
    from_addr : ("Start processing records from entry with this address", 'option', 'f', str, None, 'ADDDRESS'),
    skip : ("Skip N lines from TSV file", 'option', 's', int, None)=0,
    delay : ("Delay, in seconds, between API calls", 'option', 'd', float, None)=5.0,
    tsv_file : ("Bzipped2 TSV file with addresses", 'positional', None, str)='data/addresses.tsv.bz2'
    ):

    assert skip >= 0, "Skip option value must be greater or equal 0."

    with bz2.open(tsv_file, 'rt') as ifile:
        addresses = ifile.read().splitlines() # gives lines without EOLs


    print(f"[i] Read {len(addresses)} data entries from {tsv_file} .")

    if from_addr:
        from_idx = next((i for i, t in enumerate(addresses) if t == from_addr), None)
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
    ofname_coinbase = f'.logs/result_{now_iso}_coinbase.log'
    ofname_pubkey = f'.logs/result_{now_iso}_pubkey.log'

    found = []
    progress = tqdm.tqdm(addresses[(from_idx + skip):])
    for address in progress:
        if not address.startswith('1'):
            # skip non-compliant addresses
            continue

        progress.set_description(f"{address:<62}")

        url = f'https://blockchain.info/rawaddr/{address}'
        jres = pull_data(url, delay)

        txs = jres['txs']

        if len(txs) == 0:
            print(f"[!] Received empty transaction list for address {address}.")
            continue

        offset = 0
        fbalance = int(jres['final_balance']) / 1e8
        if (jres['n_tx'] == 1 and jres['total_sent'] == 0 and
            len(txs[0]['inputs'][0]['prev_out']['script']) == 0):
            # coinbase address
            print(f'[i] Coinbase address: {address} {fbalance:.8f}')
            with open(ofname_coinbase, 'a') as ofile:
                ofile.write(f'{address:<62}\t{fbalance:.8f}\n')
        elif jres['total_sent'] != 0:
            # find an outgoing TX 
            ntx = jres['n_tx']
            offset += ntx
            found = False
            while ntx > 0 and not found:
                for tx in txs:
                    inputs = tx['inputs']
                    for input in inputs:
                        script = input['script']
                        len1 = int(script[:2], 16)
                        script = script[2 * (1 + len1):] # in 2 hex digits units
                        if len(script) == 0:
                            break
                        len2 = int(script[:2], 16)
                        if len2 == 65:
                            pubkey = script[2:(2 * (65 + 1))]
                            # is it ours?
                            h160 = to_hash160(pubkey)
                            if h160 == jres['hash160']:
                                print(f'[i] {address} {fbalance:.8f} Pubkey: {pubkey}')
                                with open(ofname_pubkey, 'a') as ofile:
                                    ofile.write(f'{address:<62}\t{fbalance:.8f}\t{pubkey}\n')
                                found = True
                                break
                            pass
                        pass
                    if found:
                        break
                if found:
                    break
                ntx -= len(txs)
                if ntx > 0:
                    # pull more data
                    time.sleep(delay)
                    url = f'https://blockchain.info/rawaddr/{address}?offset={offset}'
                    jres = pull_data(url, delay)
                pass

        time.sleep(delay)
        pass

    pass


if __name__ == "__main__":
    plac.call(main)
