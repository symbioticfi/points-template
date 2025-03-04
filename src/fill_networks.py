import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="eth_utils")

from web3 import Web3

from common.config import Config
from common.storage import Storage
from common.web3wrapper import Web3Wrapper


class NetworksHelper:
    def __init__(self, config, w3_wrapper, storage):
        self.config = config
        self.w3_wrapper = w3_wrapper
        self.storage = storage

    def run(self):
        networks = {
            "0x4535bd6fF24860b5fd2889857651a85fb3d3C6b1": {
                "1": {
                    "max_rate": 2500000000000000000000000000000000000000000000,
                    "target_stake": None,
                    "network_fee": None,
                    "operator_fee": 300,
                    "start_from": None,
                }
            },
        }
        networks = {
            Web3.to_checksum_address(network): {
                int(identifier): {
                    "max_rate": (
                        int(networks[network][identifier]["max_rate"])
                        if networks[network][identifier]["max_rate"] != None
                        else None
                    ),
                    "target_stake": (
                        int(networks[network][identifier]["target_stake"])
                        if networks[network][identifier]["target_stake"] != None
                        else None
                    ),
                    "network_fee": (
                        int(networks[network][identifier]["network_fee"])
                        if networks[network][identifier]["network_fee"] != None
                        else None
                    ),
                    "operator_fee": (
                        int(networks[network][identifier]["operator_fee"])
                        if networks[network][identifier]["operator_fee"] != None
                        else None
                    ),
                    "start_from": (
                        int(networks[network][identifier]["start_from"])
                        if networks[network][identifier]["start_from"] != None
                        else None
                    ),
                }
                for identifier in networks[network]
            }
            for network in networks
        }
        for network in networks:
            for identifier in networks[network]:
                self.storage.save_networks_points_data_safe(
                    {
                        "network": network,
                        "identifier": identifier,
                        "max_rate": networks[network][identifier][
                            "max_rate"
                        ],  # base - 1e48
                        "target_stake": networks[network][identifier][
                            "target_stake"
                        ],  # base - 1e24
                        "network_fee": networks[network][identifier][
                            "network_fee"
                        ],  # base - 10000
                        "operator_fee": networks[network][identifier][
                            "operator_fee"
                        ],  # base - 10000
                        "block_number_processed": networks[network][identifier][
                            "start_from"
                        ],
                    }
                )
        self.storage.commit()


if __name__ == "__main__":
    config = Config()
    storage = Storage(config, init=True)
    w3_wrapper = Web3Wrapper(config, storage)
    helper = NetworksHelper(config, w3_wrapper, storage)

    helper.run()
    storage.close()
