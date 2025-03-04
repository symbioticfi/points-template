import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="eth_utils")

from web3 import Web3
from w3multicall.multicall import W3Multicall
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from eth_utils import event_signature_to_log_topic
from retry import retry

from common.config import Config
from common.storage import Storage
from common.constants import Address
from common.web3wrapper import Web3Wrapper


class Events:
    def __init__(self, config, w3_wrapper, storage):
        self.config = config
        self.w3_wrapper = w3_wrapper
        self.storage = storage
        self.name = self.config.get_events_module_name()
        self.chunk_size = 5000
        self.max_workers = 4
        self.debug = self.config.get_debug()

    def get_logs(
        self,
        contract_addresses,
        topics,
        from_block,
        to_block,
        max_workers=4,
        show_progress=False,
    ):
        logs = []
        total_blocks = to_block - from_block + 1
        chunks_to_process = [(from_block, to_block)]
        future_to_chunk = {}

        progress_bar = (
            tqdm(total=total_blocks, desc="Blocks processed") if show_progress else None
        )

        if self.debug:
            print(f"  Starting ThreadPoolExecutor with max_workers={max_workers}")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            while chunks_to_process or future_to_chunk:
                if self.debug:
                    print(
                        f"  Chunks to process: {chunks_to_process}, Futures: {len(future_to_chunk)}"
                    )
                while chunks_to_process and len(future_to_chunk) < max_workers:
                    chunk_from_block, chunk_to_block = chunks_to_process.pop()
                    if self.debug:
                        print(
                            f"    Submitting logs fetch for chunk: {chunk_from_block}-{chunk_to_block}"
                        )
                    future = executor.submit(
                        self.w3_wrapper.w3.eth.get_logs,
                        {
                            "fromBlock": chunk_from_block,
                            "toBlock": chunk_to_block,
                            "address": contract_addresses,
                            "topics": topics,
                        },
                    )
                    future_to_chunk[future] = (chunk_from_block, chunk_to_block)
                if not future_to_chunk:
                    continue
                done, not_done = wait(
                    future_to_chunk.keys(), return_when=FIRST_COMPLETED
                )
                for future in done:
                    chunk_from_block, chunk_to_block = future_to_chunk.pop(future)
                    chunk_size = chunk_to_block - chunk_from_block + 1
                    try:
                        result = future.result()
                        logs.extend(result)
                        if self.debug:
                            print(
                                f"    Successfully fetched {len(result)} logs for blocks {chunk_from_block}-{chunk_to_block}"
                            )

                        if progress_bar:
                            progress_bar.update(chunk_size)
                    except Exception as e:
                        print(
                            f"    Exception encountered for blocks {chunk_from_block}-{chunk_to_block}: {e}"
                        )
                        if chunk_from_block == chunk_to_block:
                            print(
                                f"Failed to get logs for block {chunk_from_block}: {e}"
                            )
                            if progress_bar:
                                progress_bar.update(1)
                        else:
                            mid_block = (chunk_from_block + chunk_to_block) // 2
                            if mid_block == chunk_from_block:
                                print(
                                    f"Failed to get logs for blocks {chunk_from_block}-{chunk_to_block}: {e}"
                                )
                                if progress_bar:
                                    progress_bar.update(chunk_size)
                            else:
                                if self.debug:
                                    print(
                                        f"    Splitting chunk {chunk_from_block}-{chunk_to_block} into {chunk_from_block}-{mid_block} and {mid_block + 1}-{chunk_to_block}"
                                    )
                                chunks_to_process.append((chunk_from_block, mid_block))
                                chunks_to_process.append(
                                    (mid_block + 1, chunk_to_block)
                                )
            if progress_bar:
                progress_bar.close()
        return logs

    def get_start_block(self):
        print("Retrieving the starting block for events parsing...")
        last_processed_block = self.storage.get_processed_timepoint(self.name)
        if last_processed_block is not None:
            if self.debug:
                print(f"  Last processed block found: {last_processed_block}")
            return last_processed_block + 1

        print(
            "  No last processed block found, retrieving creation block from VaultFactory..."
        )
        creation_block = self.w3_wrapper.get_creation_block(
            self.w3_wrapper.addresses.vault_factory.address
        )
        start_block = creation_block - 100
        if self.debug:
            print(f"  Using creation block minus 100: {start_block}")
        return start_block

    def get_end_block(self):
        return self.w3_wrapper.get_finalized_block()

    def decode_vault_factory_logs(self, raw_logs):
        if self.debug:
            print(f"Decoding {len(raw_logs)} raw logs from VaultFactory...")
        contract = self.w3_wrapper.addresses.vault_factory.contract

        def decode_log(raw_log):
            nonlocal contract

            if raw_log["topics"][0] == event_signature_to_log_topic(
                "AddEntity(address)"
            ):
                return contract.events.AddEntity().process_log(raw_log)
            else:
                raise ValueError("Unknown event signature")

        decoded = [decode_log(raw_log) for raw_log in raw_logs]
        if self.debug:
            print(f"  Decoded logs count: {len(decoded)}")
        return decoded

    def collect_global_vars(self, logs):
        if self.debug:
            print(f"Collecting global vars for {len(logs)} logs...")
        var_sets = []
        for log in logs:
            vault = log["args"]["entity"]
            w3_multicall = W3Multicall(self.w3_wrapper.w3)
            w3_multicall.add(W3Multicall.Call(vault, "delegator()(address)"))
            w3_multicall.add(W3Multicall.Call(vault, "collateral()(address)"))
            w3_multicall.add(W3Multicall.Call(vault, "epochDurationInit()(uint48)"))
            w3_multicall.add(W3Multicall.Call(vault, "epochDuration()(uint48)"))
            result = w3_multicall.call(self.w3_wrapper.get_finalized_block())
            delegator = Web3.to_checksum_address(result[0])
            collateral = Web3.to_checksum_address(result[1])

            if delegator == "0x0000000000000000000000000000000000000000":
                if self.debug:
                    print(f"  Skipping vault with zero delegator: {vault}")
                continue

            finalized_block = self.w3_wrapper.get_finalized_block()
            delegator_type = (
                Address(self.w3_wrapper, "delegator", delegator)
                .contract.functions.TYPE()
                .call({"block_identifier": finalized_block})
            )

            var_set = {
                "vault": vault,
                "delegator": delegator,
                "delegator_type": delegator_type,
                "collateral": collateral,
                "epochDurationInit": result[2],
                "epochDuration": result[3],
                "operator": None,
                "network": None,
            }

            if delegator_type == 2:
                w3_multicall = W3Multicall(self.w3_wrapper.w3)
                w3_multicall.add(W3Multicall.Call(delegator, "operator()(address)"))
                result = w3_multicall.call()

                var_set["operator"] = Web3.to_checksum_address(result[0])
            elif delegator_type == 3:
                w3_multicall = W3Multicall(self.w3_wrapper.w3)
                w3_multicall.add(W3Multicall.Call(delegator, "operator()(address)"))
                w3_multicall.add(W3Multicall.Call(delegator, "network()(address)"))
                result = w3_multicall.call(finalized_block)

                var_set["operator"] = Web3.to_checksum_address(result[0])
                var_set["network"] = Web3.to_checksum_address(result[1])

            var_sets.append(var_set)
        if self.debug:
            print(f"  Global vars collected: {len(var_sets)}")
        return var_sets

    def parse_vault_factory(self, from_block, to_block):
        if self.debug:
            print(f"Parsing VaultFactory events from {from_block} to {to_block}...")
        contract_address = self.w3_wrapper.addresses.vault_factory.address

        raw_logs = self.get_logs(
            [contract_address],
            [[event_signature_to_log_topic("AddEntity(address)")]],
            from_block,
            to_block,
            max_workers=self.max_workers,
        )
        logs = self.decode_vault_factory_logs(raw_logs)
        var_sets = self.collect_global_vars(logs)
        if self.debug:
            print(f"  Saving global vars for {len(var_sets)} new vaults...")
        self.storage.save_global_vars(var_sets)

        return [
            {"vault": var_set["vault"], "delegator": var_set["delegator"]}
            for var_set in var_sets
        ]

    def decode_operator_network_opt_in_service_logs(self, raw_logs):
        if self.debug:
            print(
                f"Decoding {len(raw_logs)} raw logs from OperatorNetworkOptInService..."
            )
        contract = self.w3_wrapper.addresses.operator_network_opt_in_service.contract

        def decode_log(raw_log):
            nonlocal contract

            if raw_log["topics"][0] == event_signature_to_log_topic(
                "OptIn(address,address)"
            ):
                return contract.events.OptIn().process_log(raw_log)
            elif raw_log["topics"][0] == event_signature_to_log_topic(
                "OptOut(address,address)"
            ):
                return contract.events.OptOut().process_log(raw_log)
            else:
                raise ValueError("Unknown event signature")

        decoded = [decode_log(raw_log) for raw_log in raw_logs]
        if self.debug:
            print(f"  Decoded logs count: {len(decoded)}")
        return decoded

    def parse_operator_network_opt_in_service_logs(self, from_block, to_block):
        if self.debug:
            print(
                f"Parsing OperatorNetworkOptInService logs from {from_block} to {to_block}..."
            )
        contract_address = (
            self.w3_wrapper.addresses.operator_network_opt_in_service.address
        )

        raw_logs = self.get_logs(
            [contract_address],
            [
                [
                    event_signature_to_log_topic("OptIn(address,address)"),
                    event_signature_to_log_topic("OptOut(address,address)"),
                ]
            ],
            from_block,
            to_block,
            max_workers=self.max_workers,
        )
        logs = self.decode_operator_network_opt_in_service_logs(raw_logs)
        if self.debug:
            print(f"  Saving {len(logs)} OperatorNetworkOptInService logs...")
        self.storage.save_operator_network_opt_in_service_logs(logs)

    def decode_operator_vault_opt_in_service_logs(self, raw_logs):
        if self.debug:
            print(
                f"Decoding {len(raw_logs)} raw logs from OperatorVaultOptInService..."
            )
        contract = self.w3_wrapper.addresses.operator_vault_opt_in_service.contract

        def decode_log(raw_log):
            nonlocal contract

            if raw_log["topics"][0] == event_signature_to_log_topic(
                "OptIn(address,address)"
            ):
                return contract.events.OptIn().process_log(raw_log)
            elif raw_log["topics"][0] == event_signature_to_log_topic(
                "OptOut(address,address)"
            ):
                return contract.events.OptOut().process_log(raw_log)
            else:
                raise ValueError("Unknown event signature")

        decoded = [decode_log(raw_log) for raw_log in raw_logs]
        if self.debug:
            print(f"  Decoded logs count: {len(decoded)}")
        return decoded

    def parse_operator_vault_opt_in_service_logs(self, from_block, to_block):
        if self.debug:
            print(
                f"Parsing OperatorVaultOptInService logs from {from_block} to {to_block}..."
            )
        contract_address = (
            self.w3_wrapper.addresses.operator_vault_opt_in_service.address
        )

        raw_logs = self.get_logs(
            [contract_address],
            [
                [
                    event_signature_to_log_topic("OptIn(address,address)"),
                    event_signature_to_log_topic("OptOut(address,address)"),
                ]
            ],
            from_block,
            to_block,
            max_workers=self.max_workers,
        )
        logs = self.decode_operator_vault_opt_in_service_logs(raw_logs)
        if self.debug:
            print(f"  Saving {len(logs)} OperatorVaultOptInService logs...")
        self.storage.save_operator_vault_opt_in_service_logs(logs)

    def decode_vault_logs(self, raw_logs):
        if self.debug:
            print(f"Decoding {len(raw_logs)} raw logs from vaults...")
        contract = Address(
            self.w3_wrapper, "vault", "0x0000000000000000000000000000000000000000"
        ).contract

        def decode_log(raw_log):
            nonlocal contract

            if raw_log["topics"][0] == event_signature_to_log_topic(
                "Deposit(address,address,uint256,uint256)"
            ):
                return contract.events.Deposit().process_log(raw_log)
            elif raw_log["topics"][0] == event_signature_to_log_topic(
                "Withdraw(address,address,uint256,uint256,uint256)"
            ):
                return contract.events.Withdraw().process_log(raw_log)
            elif raw_log["topics"][0] == event_signature_to_log_topic(
                "OnSlash(uint256,uint48,uint256)"
            ):
                return contract.events.OnSlash().process_log(raw_log)
            elif raw_log["topics"][0] == event_signature_to_log_topic(
                "Transfer(address,address,uint256)"
            ):
                return contract.events.Transfer().process_log(raw_log)
            else:
                raise ValueError("Unknown event signature")

        decoded = [decode_log(raw_log) for raw_log in raw_logs]
        if self.debug:
            print(f"  Decoded vault logs count: {len(decoded)}")
        return decoded

    def parse_vault_logs(self, vaults, from_block, to_block):
        if self.debug:
            print(
                f"Parsing vault logs for {len(vaults)} vaults, blocks {from_block}-{to_block}..."
            )
        raw_logs = self.get_logs(
            vaults,
            [
                [
                    event_signature_to_log_topic(
                        "Deposit(address,address,uint256,uint256)"
                    ),
                    event_signature_to_log_topic(
                        "Withdraw(address,address,uint256,uint256,uint256)"
                    ),
                    event_signature_to_log_topic("OnSlash(uint256,uint48,uint256)"),
                    event_signature_to_log_topic("Transfer(address,address,uint256)"),
                ]
            ],
            from_block,
            to_block,
            max_workers=self.max_workers,
        )
        logs = self.decode_vault_logs(raw_logs)
        if self.debug:
            print(f"  Saving {len(logs)} decoded vault logs...")
        self.storage.save_vault_logs(logs)

    def decode_delegator_logs(self, raw_logs):
        if self.debug:
            print(f"Decoding {len(raw_logs)} raw logs from delegators...")
        contract = Address(
            self.w3_wrapper, "delegator", "0x0000000000000000000000000000000000000000"
        ).contract
        network_restake_contract = Address(
            self.w3_wrapper,
            "network_restake_delegator",
            "0x0000000000000000000000000000000000000000",
        ).contract
        full_restake_contract = Address(
            self.w3_wrapper,
            "full_restake_delegator",
            "0x0000000000000000000000000000000000000000",
        ).contract

        def decode_log(raw_log):
            nonlocal contract

            if raw_log["topics"][0] == event_signature_to_log_topic(
                "SetMaxNetworkLimit(bytes32,uint256)"
            ):
                return contract.events.SetMaxNetworkLimit().process_log(raw_log)
            elif raw_log["topics"][0] == event_signature_to_log_topic(
                "SetNetworkLimit(bytes32,uint256)"
            ):
                return network_restake_contract.events.SetNetworkLimit().process_log(
                    raw_log
                )
            elif raw_log["topics"][0] == event_signature_to_log_topic(
                "SetOperatorNetworkShares(bytes32,address,uint256)"
            ):
                return network_restake_contract.events.SetOperatorNetworkShares().process_log(
                    raw_log
                )
            elif raw_log["topics"][0] == event_signature_to_log_topic(
                "SetOperatorNetworkLimit(bytes32,address,uint256)"
            ):
                return (
                    full_restake_contract.events.SetOperatorNetworkLimit().process_log(
                        raw_log
                    )
                )
            else:
                raise ValueError("Unknown event signature")

        decoded = [decode_log(raw_log) for raw_log in raw_logs]
        if self.debug:
            print(f"  Decoded delegator logs count: {len(decoded)}")
        return decoded

    def parse_delegator_logs(self, delegators, from_block, to_block):
        if self.debug:
            print(
                f"Parsing delegator logs for {len(delegators)} delegators, blocks {from_block}-{to_block}..."
            )
        raw_logs = self.get_logs(
            delegators,
            [
                [
                    event_signature_to_log_topic("SetMaxNetworkLimit(bytes32,uint256)"),
                    event_signature_to_log_topic("SetNetworkLimit(bytes32,uint256)"),
                    event_signature_to_log_topic(
                        "SetOperatorNetworkShares(bytes32,address,uint256)"
                    ),
                    event_signature_to_log_topic(
                        "SetOperatorNetworkLimit(bytes32,address,uint256)"
                    ),
                ]
            ],
            from_block,
            to_block,
            max_workers=self.max_workers,
        )
        logs = self.decode_delegator_logs(raw_logs)
        if self.debug:
            print(f"  Saving {len(logs)} decoded delegator logs...")
        self.storage.save_delegator_logs(logs)

    def parse_logs(self, from_block, to_block):
        if self.debug:
            print(f"parse_logs called for blocks {from_block}-{to_block}...")
        all_modules = self.storage.get_all_modules()
        all_modules.extend(self.parse_vault_factory(from_block, to_block))
        self.parse_operator_network_opt_in_service_logs(from_block, to_block)
        self.parse_operator_vault_opt_in_service_logs(from_block, to_block)
        if len(all_modules) != 0:
            if self.debug:
                print(
                    f"  Found {len(all_modules)} total modules to parse (existing + new)."
                )
            self.parse_vault_logs(
                [modules["vault"] for modules in all_modules], from_block, to_block
            )
            self.parse_delegator_logs(
                [modules["delegator"] for modules in all_modules], from_block, to_block
            )
        else:
            if self.debug:
                print("  No modules to parse logs for at this time.")

        if self.debug:
            print(f"  Updating last processed block to {to_block} and committing data.")
        self.storage.save_processed_timepoint(self.name, to_block)
        self.storage.commit()
        print(
            f"  Updated last processed block from {from_block} to {to_block} and committed data."
        )

    @retry(
        tries=5,
        delay=1,
        backoff=2,
        jitter=(0, 0.5),
        exceptions=(Exception,),
    )
    def parse_all_logs(self):
        print("Starting parse_all_logs...")
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

            self.parse_logs(from_block, to_block)


if __name__ == "__main__":
    config = Config()
    storage = Storage(config)
    w3_wrapper = Web3Wrapper(config, storage)
    events = Events(config, w3_wrapper, storage)

    events.parse_all_logs()
    storage.close()
