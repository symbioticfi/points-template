from functools import lru_cache
from web3 import Web3
import requests

from .constants import ABIS, Addresses
from .helpers import Helpers


class Web3Wrapper:
    def __init__(self, config, storage):
        self.config = config
        self.storage = storage
        self.w3 = Web3(Web3.HTTPProvider(self.config.get_rpc()))
        self.addresses = Addresses(self)

    @lru_cache(maxsize=None)
    def get_block_data(self, block_number, full=False):
        if full == False:
            block_data = self.storage.get_block_data(block_number)
            if block_data != None:
                return block_data
        block_data = self.w3.eth.get_block(block_number)
        self.storage.save_block_data(block_data)
        return block_data

    @lru_cache(maxsize=None)
    def get_block_timestamp(self, block_number):
        block_data = self.get_block_data(block_number)
        return block_data["timestamp"]

    def get_block_number(self):
        return self.w3.eth.block_number

    def get_finalized_block(self):
        return self.get_block_number() - 160

    @lru_cache(maxsize=None)
    def get_chain_id(self):
        return self.w3.eth.chain_id

    @lru_cache(maxsize=None)
    def get_creation_block(self, contract_address):
        api_url = f"https://{self.config.get_blockscout_api_url()}/api"

        params = {
            "module": "contract",
            "action": "getcontractcreation",
            "contractaddresses": contract_address,
            "apikey": self.config.get_blockscout_api_key(),
        }
        response = requests.get(api_url, params=params)
        Helpers.raise_for_status_with_log(response)
        data = response.json()

        params = {
            "module": "transaction",
            "action": "gettxinfo",
            "txhash": data["result"][0]["txHash"],
            "apikey": self.config.get_blockscout_api_key(),
        }
        response = requests.get(api_url, params=params)
        Helpers.raise_for_status_with_log(response)
        data = response.json()
        return int(data["result"]["blockNumber"])

    @lru_cache(maxsize=None)
    def get_token_decimals(self, token_address):
        token = self.w3.eth.contract(address=token_address, abi=ABIS["erc20"])
        return token.functions.decimals().call()
