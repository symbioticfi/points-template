from dotenv import load_dotenv
import os


from .constants import (
    CHAIN_IDS,
    BLOCKSCOUT_API_URLS,
    COINMARKETCAP_API_URL,
    ALCHEMY_PRICES_API_URL,
    ALCHEMY_NODE_API_URLS,
    MODULES_NAMES,
)


class Config:
    def __init__(self):
        load_dotenv()

        if self.get_chain() not in CHAIN_IDS:
            raise ValueError("Invalid chain")

    def get_rpc(self):
        return os.getenv("RPC")

    def get_debug(self):
        return os.getenv("DEBUG") == "true"

    def get_chain(self):
        return os.getenv("CHAIN")

    def get_chain_id(self):
        return CHAIN_IDS[self.get_chain()]

    def get_storage_data(self, test=False):
        if test:
            dbname = {
                "holesky": "holesky_test",
                "sepolia": "sepolia_test",
                "mainnet": "mainnet_test",
            }[self.get_chain()]
        else:
            dbname = {
                "holesky": "holesky",
                "sepolia": "sepolia",
                "mainnet": "mainnet",
            }[self.get_chain()]
        return {
            "dbname": os.getenv("PGSQL_NAME") if not test else dbname,
            "user": os.getenv("PGSQL_USER"),
            "password": os.getenv("PGSQL_PASSWORD"),
            "host": os.getenv("PGSQL_HOST"),
            "port": os.getenv("PGSQL_PORT"),
        }

    def get_blockscout_api_url(self):
        return BLOCKSCOUT_API_URLS[self.get_chain()]

    def get_coinmarketcap_api_url(self):
        return COINMARKETCAP_API_URL

    def get_coinmarketcap_api_key(self):
        return os.getenv("CMC_API_KEY")

    def get_alchemy_prices_api_url(self):
        return ALCHEMY_PRICES_API_URL

    def get_alchemy_api_key(self):
        return os.getenv("ALCHEMY_API_KEY")

    def get_blockscout_api_key(self):
        return os.getenv("BLOCKSCOUT_API_KEY")

    def get_points_module_name(self):
        return MODULES_NAMES["Points"]

    def get_blocks_module_name(self):
        return MODULES_NAMES["Blocks"]

    def get_prices_module_name(self):
        return MODULES_NAMES["Prices"]

    def get_state_module_name(self):
        return MODULES_NAMES["State"]

    def get_events_module_name(self):
        return MODULES_NAMES["Events"]
