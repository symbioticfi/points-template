import requests
from retry import retry

from common.config import Config
from common.storage import Storage
from common.web3wrapper import Web3Wrapper
from common.helpers import Helpers


class Blocks:
    def __init__(self, config, w3_wrapper, storage):
        self.config = config
        self.w3_wrapper = w3_wrapper
        self.storage = storage
        self.name = self.config.get_blocks_module_name()
        self.chunk_size = 1000

    def get_start_block(self):
        print("Retrieving the starting block...")
        last_processed_block = self.storage.get_processed_timepoint(self.name)
        if last_processed_block is not None:
            print(f"  Last processed block found: {last_processed_block}")
            return last_processed_block + 1

        print(
            "  No last processed block found, retrieving creation block from VaultFactory..."
        )
        creation_block = self.w3_wrapper.get_creation_block(
            self.w3_wrapper.addresses.vault_factory.address
        )
        start_block = creation_block - 100

        print(f"  Using creation block minus 100: {start_block}")
        return start_block

    def get_end_block(self):
        return self.w3_wrapper.get_finalized_block()

    def parse_blocks(self, from_block, to_block):
        print(f"Parsing blocks from {from_block} to {to_block}...")
        json = [
            {
                "method": "eth_getBlockByNumber",
                "params": [hex(block_number), False],
                "id": block_number - from_block,
                "jsonrpc": "2.0",
            }
            for block_number in range(from_block, to_block + 1)
        ]

        print(f"  Sending batch request for {len(json)} blocks.")

        response = requests.post(
            self.config.get_rpc(),
            json=json,
            headers={"Content-Type": "application/json"},
        )
        Helpers.raise_for_status_with_log(response)
        data = response.json()

        print(f"  Received response for {len(data)} blocks.")

        for response_data in data:
            block_data = response_data["result"]
            number = int(block_data["number"], 16)
            timestamp = int(block_data["timestamp"], 16)
            block_hash = block_data["hash"]

            print(
                f"    Storing block number: {number}, timestamp: {timestamp}, hash: {block_hash}"
            )

            self.storage.save_block_data(
                {
                    "number": number,
                    "timestamp": timestamp,
                    "hash": block_hash,
                }
            )

        print(f"  Updating last processed block to {to_block}.")
        self.storage.save_processed_timepoint(self.name, to_block)
        self.storage.commit()

    @retry(
        tries=5,
        delay=1,
        backoff=2,
        jitter=(0, 0.5),
        exceptions=(Exception,),
    )
    def parse_all_blocks(self):
        print("Starting parse_all_blocks...")

        start_block = self.get_start_block()
        end_block = self.get_end_block()

        print(f"End block: {end_block}. Start block: {start_block}.")

        if start_block > end_block:
            print(
                "Start block is greater than the finalized block. Nothing to process."
            )
            return

        while True:
            from_block = self.get_start_block()
            to_block = min(end_block, from_block + self.chunk_size - 1)

            print(f"Processing chunk: from_block={from_block}, to_block={to_block}")

            if from_block > to_block:
                print("All blocks processed. Exiting loop.")
                break

            self.parse_blocks(from_block, to_block)


if __name__ == "__main__":
    config = Config()
    storage = Storage(config)
    w3_wrapper = Web3Wrapper(config, storage)
    blocks = Blocks(config, w3_wrapper, storage)

    blocks.parse_all_blocks()
    storage.close()
