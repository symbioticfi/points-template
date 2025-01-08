import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="eth_utils")

import requests
from web3 import Web3


class Helpers:
    @staticmethod
    def raise_for_status_with_log(response):
        try:
            response.raise_for_status()
        except requests.HTTPError as err:
            print("HTTP Error occurred:")
            print("Status Code:", response.status_code)
            print("Reason:", response.reason)
            print("Content:", response.text)
            print("Headers:", response.headers)
            print("Request URL:", response.request.url)
            print("Request Headers:", response.request.headers)
            raise

    @staticmethod
    def get_subnetwork(network, identifier=0):
        return f"{network}{hex(identifier)[2:].rjust(24, '0')}"

    @staticmethod
    def get_network(subnetwork):
        if len(subnetwork) == 66:
            return Web3.to_checksum_address(subnetwork[:42])
        elif len(subnetwork) == 64:
            return Web3.to_checksum_address(f"0x{subnetwork[:40]}")

    @staticmethod
    def get_identifier(subnetwork):
        if len(subnetwork) == 66:
            return int(subnetwork[42:], 16)
        elif len(subnetwork) == 64:
            return int(subnetwork[40:], 16)
