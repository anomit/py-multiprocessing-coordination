import requests
import json
from web3.auto import w3
from eth_account.messages import defunct_hash_message
import random
import time

'''
curl -X POST "https://api.thundervigil.com/v0.1/contract/0x1070cc96ffaa7026baa0ebff9f692679f11aba8a/transfer" 
-H "accept: application/json" -H "X-API-KEY: get-your-own-api-key" 
-H "Content-Type: application/json" -d "{\"to\":\"0x4a20608f7821d13dCB61aE130753cD58B597b03b\",\"tokens\":10000}"
'''

REST_API_ENDPOINT = 'https://api.thundervigil.com/v0.1'

p_k = "ABCD345FEA"  # hex string without 0x prefix. Private key of 160-bit ethereum address associated with Thundervigil account
API_KEY = None
TOKEN_CONTRACT = None


def test_login():
    global API_KEY
    time.sleep(1)  # adding delay to not hit rate limiting
    msg = "Trying to login"
    message_hash = defunct_hash_message(text=msg)
    signed_msg = w3.eth.account.signHash(message_hash, p_k)
    # --THUNDERVIGIL API CALL---
    r = requests.post(REST_API_ENDPOINT + '/login', json={'msg': msg, 'sig': signed_msg.signature.hex()})
    print(r.text)
    assert (r.status_code == requests.codes.ok)
    r = r.json()
    assert(r['success'] is True)
    API_KEY = r['data']['key']


def test_deploy():
    global TOKEN_CONTRACT
    msg = "Trying to deploy"
    message_hash = defunct_hash_message(text=msg)
    # deploy from alpha account
    signed_msg = w3.eth.account.signHash(message_hash, p_k)
    with open(file='token.sol', mode='r') as w:
        code = w.read()
    deploy_json = {
        'msg': msg,
        'sig': signed_msg.signature.hex(),
        'name': 'FixedSupplyToken',
        'inputs': [
            ],
        'code': code
    }
    # --THUNDERVIGIL API CALL---
    r = requests.post(REST_API_ENDPOINT + '/deploy', json=deploy_json)
    print(r.text)
    assert (r.status_code == requests.codes.ok)
    r = r.json()
    assert (r['success'] is True)
    TOKEN_CONTRACT = r['data']['contract']


def test_transfer():
    print('Sleeping 15 seconds...')
    time.sleep(15)
    if API_KEY is None:
        raise Exception('Could not fetch API Key')
    if TOKEN_CONTRACT is None:
        raise Exception('Could not deploy token previously')
    TRANSFER_REST_ENDPOINT = f'{REST_API_ENDPOINT}/contract/{TOKEN_CONTRACT}/transfer'
    print(f'Calling transfer method on contract via {TRANSFER_REST_ENDPOINT}')
    headers = {'accept': 'application/json', 'Content-Type': 'application/json', 'X-API-KEY': API_KEY}
    # number of transfers to simulate
    transfers = random.randint(10, 20)
    print(f'About to make {transfers} transfers')
    txhashes = []
    for _ in range(transfers):
        transfer_params = {'to': '0x4a20608f7821d13dCB61aE130753cD58B597b03b', 'tokens': 100}
        # --THUNDERVIGIL API CALL---
        r = requests.post(TRANSFER_REST_ENDPOINT, json=transfer_params, headers=headers)
        assert (r.status_code == requests.codes.ok)
        r = r.json()
        txhashes.append(r['data'][0]['txHash'])
        print(f"Sent tx hash {r['data'][0]['txHash']}")
        delay = random.random()
        print(f'Next tx in {delay} seconds\n\n')
        time.sleep(random.random())
    # print('Txhashes generated: ')
    # print(txhashes)