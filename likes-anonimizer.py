#!/usr/bin/env python3
import json
import fileinput
import random

alphabet = 'abcdefghijklmnopqrstuvwxyz234567'  # base32 or whatever

claimed = set()
mapped = {}

def swap_did(did):
    if fake := mapped.get(did):
        return fake
    while True:
        fake = ''.join(random.choice(alphabet) for _ in range(24))
        fake = f'did:plc:{fake}'
        if fake not in claimed:
            break
    mapped[did] = fake
    claimed.add(fake)
    return fake


for line in fileinput.input():
    action, did, rkey, target = json.loads(line)
    fake_did = swap_did(did)

    if action == 'c':
        assert target.startswith('at://')
        target_did, rest = target[5:].split('/', maxsplit=1)
        fake_target_did = swap_did(target_did)
        fake_target = f'at://{fake_target_did}/{rest}'
        print(f'{action};{fake_target};{fake_did}!{rkey}')
    else:
        assert action == 'd'
        print(f'{action};{fake_did}!{rkey})')
