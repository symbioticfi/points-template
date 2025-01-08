import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="eth_utils")

from web3 import Web3
from w3multicall.multicall import W3Multicall

from common.config import Config
from common.storage import Storage
from common.web3wrapper import Web3Wrapper


class CollateralsHelper:
    def __init__(self, config, w3_wrapper, storage):
        self.config = config
        self.w3_wrapper = w3_wrapper
        self.storage = storage

    def run(self):
        collaterals = {
            "0x7322c24752f79c05ffd1e2a6fcb97020c1c264f1": {
                "cmcID": 15060,
            },
            "0xb4f5fc289a778b80392b86fa70a7111e5be0f859": {
                "cmcID": 27566,
            },
            "0xf603c5a3f774f05d4d848a9bb139809790890864": {
                "cmcID": 28476,
            },
            "0x8d09a4502Cc8Cf1547aD300E066060D043f6982D": {
                "cmcID": 12409,
            },
            "0x94373a4919B3240D86eA41593D5eBa789FEF3848": {
                "cmcID": 2396,
            },
            "0x640577074A5d054b011E0f05114078777646ca8e": {
                "cmcID": 3717,
            },
        }
        collaterals = {
            Web3.to_checksum_address(collateral): {
                "cmcID": int(collaterals[collateral]["cmcID"]),
            }
            for collateral in collaterals
        }
        for collateral in collaterals:
            w3_multicall = W3Multicall(self.w3_wrapper.w3)
            w3_multicall.add(W3Multicall.Call(collateral, "decimals()(uint256)"))
            w3_multicall.add(W3Multicall.Call(collateral, "symbol()(string)"))
            w3_multicall.add(W3Multicall.Call(collateral, "name()(string)"))
            result = w3_multicall.call(self.w3_wrapper.get_finalized_block())

            self.storage.save_collateral(
                {
                    "collateral": collateral,
                    "decimals": result[0],
                    "symbol": result[1],
                    "name": result[2],
                    "cmcID": collaterals[collateral]["cmcID"],
                }
            )
        self.storage.commit()


if __name__ == "__main__":
    config = Config()
    storage = Storage(config, init=True)
    w3_wrapper = Web3Wrapper(config, storage)
    helper = CollateralsHelper(config, w3_wrapper, storage)

    helper.run()
    storage.close()
