import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="eth_utils")

from web3 import Web3


CHAIN_IDS = {
    "holesky": 17000,
    "sepolia": 11155111,
    "mainnet": 1,
}

CHAIN_IDS_TO_NAMES = {v: k for k, v in CHAIN_IDS.items()}


PATH = "."


def load_abi(name):
    abis_path = f"{PATH}/public/abi"
    return open(f"{abis_path}/{name}ABI.json", "r").read()


ABIS = {
    "operator_registry": load_abi("OperatorRegistry"),
    "network_registry": load_abi("NetworkRegistry"),
    "operator_vault_opt_in_service": load_abi("OptInService"),
    "operator_network_opt_in_service": load_abi("OptInService"),
    "network_middleware_service": load_abi("NetworkMiddlewareService"),
    "vault_factory": load_abi("VaultFactory"),
    "entity": load_abi("Entity"),
    "delegator": load_abi("NetworkRestakeDelegator"),
    "network_restake_delegator": load_abi("NetworkRestakeDelegator"),
    "full_restake_delegator": load_abi("FullRestakeDelegator"),
    "operator_specific_delegator": load_abi("OperatorSpecificDelegator"),
    "vault": load_abi("Vault"),
    "erc20": load_abi("ERC20"),
}


ADDRESSES = {
    "holesky": {
        "operator_registry": "0x6F75a4ffF97326A00e52662d82EA4FdE86a2C548",
        "network_registry": "0x7d03b7343BF8d5cEC7C0C27ecE084a20113D15C9",
        "operator_vault_opt_in_service": "0x95CC0a052ae33941877c9619835A233D21D57351",
        "operator_network_opt_in_service": "0x58973d16FFA900D11fC22e5e2B6840d9f7e13401",
        "network_middleware_service": "0x62a1ddfD86b4c1636759d9286D3A0EC722D086e3",
        "vault_factory": "0x407A039D94948484D356eFB765b3c74382A050B4",
    },
    "sepolia": {
        "operator_registry": "0x6F75a4ffF97326A00e52662d82EA4FdE86a2C548",
        "network_registry": "0x7d03b7343BF8d5cEC7C0C27ecE084a20113D15C9",
        "operator_vault_opt_in_service": "0x95CC0a052ae33941877c9619835A233D21D57351",
        "operator_network_opt_in_service": "0x58973d16FFA900D11fC22e5e2B6840d9f7e13401",
        "network_middleware_service": "0x62a1ddfD86b4c1636759d9286D3A0EC722D086e3",
        "vault_factory": "0x407A039D94948484D356eFB765b3c74382A050B4",
    },
    "mainnet": {
        "operator_registry": "0x0000000000000000000000000000000000000000",
        "network_registry": "0x0000000000000000000000000000000000000000",
        "operator_vault_opt_in_service": "0x0000000000000000000000000000000000000000",
        "operator_network_opt_in_service": "0x0000000000000000000000000000000000000000",
        "network_middleware_service": "0x0000000000000000000000000000000000000000",
        "vault_factory": "0x0000000000000000000000000000000000000000",
    },
}

BLOCKSCOUT_API_URLS = {
    "holesky": "eth-holesky.blockscout.com",
    "sepolia": "eth-sepolia.blockscout.com",
    "mainnet": "eth-mainnet.blockscout.com",
}

COINMARKETCAP_API_URL = "pro-api.coinmarketcap.com"

ALCHEMY_PRICES_API_URL = "api.g.alchemy.com/prices"

ALCHEMY_NODE_API_URLS = {
    "holesky": "eth-holesky.g.alchemy.com",
    "sepolia": "eth-sepolia.g.alchemy.com",
    "mainnet": "eth-mainnet.g.alchemy.com",
}

MODULES_NAMES = {
    "Blocks": "blocks",
    "Prices": "prices",
    "Points": "points",
    "State": "state",
    "Events": "events",
}


class Address:
    def __init__(self, w3_wrapper, name, address=None):
        self.address = Web3.to_checksum_address(
            ADDRESSES[CHAIN_IDS_TO_NAMES[w3_wrapper.get_chain_id()]][name]
            if address == None
            else address
        )
        self.contract = w3_wrapper.w3.eth.contract(address=self.address, abi=ABIS[name])


class Addresses:
    def __init__(self, w3_wrapper):
        for name in ADDRESSES[CHAIN_IDS_TO_NAMES[w3_wrapper.get_chain_id()]]:
            setattr(self, name, Address(w3_wrapper, name))
