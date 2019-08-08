import argparse
import time
import random

# note that the track 2 python SDK is being used here
from azure.storage.blob import (
    BlobClient,
    LeaseClient,
)

from azure.core import (
    HttpResponseError,
)

LOCK = 'lock'
UNLOCK = 'unlock'


def get_raw_input():
    parser = argparse.ArgumentParser(description='Lock/unlock a distributed mutex (implemented with blob lease)')
    parser.add_argument('action', help='can be "lock" (attempt to acquire lease) or "unlock" (break lease)')
    parser.add_argument('mutex_url', help='points to a blob url (SAS included)')
    args = parser.parse_args()

    if args.action not in [LOCK, UNLOCK]:
        raise ValueError('invalid action, can only be "lock" or "unlock"')
    return args.action, args.mutex_url


def process():
    action, mutex_url = get_raw_input()

    # check whether the blob exists, if not quit right away to avoid wasting time
    blob_client = BlobClient(mutex_url)
    try:
        blob_client.get_blob_properties()
        print("INFO: validated mutex url")
    except HttpResponseError as e:
        raise ValueError('please provide an existing and valid blob URL, failed to get properties with error: ' + e)

    # get a handle on the lease
    lease_client = LeaseClient(blob_client)
    if action == UNLOCK:
        # make the lease free as soon as possible
        lease_client.break_lease(lease_break_period=1)
        print(f"INFO: successfully unlocked the mutex!")
        return

    # action is lock, attempt to acquire the lease continuously
    while True:
        # try to acquire and infinite lease
        try:
            lease_client.acquire(lease_duration=-1)

            # if we get here, the acquire call succeeded
            # if we don't get here it stalls forever, as expected
            print(f"INFO: successfully locked the mutex!")
            return
        except HttpResponseError:
            # failed to acquire lease, another agent holds the mutex
            # sleep a bit (randomly) and try again
            sleep_period = random.randint(1, 5)
            print(f"INFO: failed to lock mutex, wait for {sleep_period} and try again")
            time.sleep(sleep_period)


if __name__ == '__main__':
    process()
