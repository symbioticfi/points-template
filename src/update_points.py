from retry import retry

from common.config import Config
from common.storage import Storage
from common.web3wrapper import Web3Wrapper
from common.state import State


class Points:
    def __init__(self, config, w3_wrapper, storage):
        self.config = config
        self.w3_wrapper = w3_wrapper
        self.storage = storage
        self.name = self.config.get_points_module_name()
        self.state = State(self.config, self.w3_wrapper, self.storage)
        self.debug = self.config.get_debug()

    def get_start_block(self):
        if self.debug:
            print("[Points] Retrieving the starting block for points parsing...")
        last_processed_block = self.storage.get_processed_timepoint(self.state.name)
        if last_processed_block is not None:
            zero_block = False
            start_block = last_processed_block + 1
            if self.debug:
                print(
                    f"  Last processed block found: {last_processed_block}, next start block={start_block}"
                )
        else:
            zero_block = True
            start_block = self.w3_wrapper.get_creation_block(
                self.w3_wrapper.addresses.vault_factory.address
            )
            if self.debug:
                print(
                    f"  No last processed block found, using creation block={start_block}"
                )

        return zero_block, start_block

    def get_end_block(self):
        last_events_block = self.storage.get_processed_timepoint(
            self.config.get_events_module_name()
        )
        if last_events_block is None:
            print("[Points] No events processed yet. Exiting.")
            exit(0)
        last_prices_timestamp = self.storage.get_processed_timepoint(
            self.config.get_prices_module_name()
        )
        if last_prices_timestamp is None:
            print("[Points] No prices processed yet. Exiting.")
            exit(0)
        last_prices_block = self.storage.get_block_number_by_timestamp(
            last_prices_timestamp
        )
        end_block = min(last_prices_block, last_events_block)
        if self.debug:
            print(f"[Points] end_block for points calculation={end_block}")

        return end_block

    def validate(self, stake_data):
        vault_data = self.storage.get_global_vars(stake_data["vault"])
        if self.debug:
            print(
                f"[Points] Validating stake_data for vault={stake_data['vault']}, delegator_type={vault_data['delegator_type']}"
            )
        if (
            vault_data["delegator_type"] == 1
        ):  # exclude FullRestakeDelegator from points calculation
            if self.debug:
                print("[Points] Validation failed: FullRestakeDelegator is excluded.")
            return False
        return True

    def get_delegation_related_stakes(self, network_points_data, collaterals_data):
        if self.debug:
            print(
                f"[Points] Fetching delegation-related stakes for network={network_points_data['network']}, "
                f"identifier={network_points_data['identifier']}"
            )
        stakes = self.storage.get_stakes(
            network_points_data["network"], network_points_data["identifier"]
        )

        s_onv = {stake_data["operator"]: {} for stake_data in stakes}
        s_on = {}
        s_vn = {}
        s_n = 0
        for stake_data in stakes:
            if not self.validate(stake_data):
                continue

            price = collaterals_data[stake_data["collateral"]].get(
                "price", 0
            )  # base - 1e24
            sr_onv = stake_data["stake"]  # base - 10 ^ decimals
            s_onv[stake_data["operator"]][stake_data["vault"]] = (
                sr_onv
                * price
                // 10 ** collaterals_data[stake_data["collateral"]]["decimals"]
            )  # base - 1e24

            s_on[stake_data["operator"]] = (
                s_on.get(stake_data["operator"], 0)
                + s_onv[stake_data["operator"]][stake_data["vault"]]
            )
            s_vn[stake_data["vault"]] = (
                s_vn.get(stake_data["vault"], 0)
                + s_onv[stake_data["operator"]][stake_data["vault"]]
            )

            s_n += s_onv[stake_data["operator"]][stake_data["vault"]]

        return s_onv, s_on, s_vn, s_n

    def get_deposit_related_stakes(self, vaults, collaterals_data):
        if self.debug:
            print(f"[Points] Fetching deposit-related stakes for vaults")
        s_uv = {vault: {} for vault in vaults}
        s_v = {}
        for vault in vaults:
            vault_data = self.storage.get_global_vars(vault)
            active_balances_of = self.storage.get_active_balances_of(vault)

            price = collaterals_data[vault_data["collateral"]].get(
                "price", 0
            )  # base - 1e24
            for active_balance_of_data in active_balances_of:
                sr_uv = active_balance_of_data[
                    "active_balance_of"
                ]  # base - 10 ^ decimals
                s_uv[vault][active_balance_of_data["user"]] = (
                    sr_uv
                    * price
                    // 10 ** collaterals_data[vault_data["collateral"]]["decimals"]
                )  # base - 1e24

                s_v[vault] = (
                    s_v.get(vault, 0) + s_uv[vault][active_balance_of_data["user"]]
                )

        return s_uv, s_v

    def parse_points_per_network(
        self,
        previous_block_number,
        block_number,
        network_points_data,
        collaterals_data,
    ):
        if self.debug:
            print(
                f"[Points] parse_points_per_network called for network={network_points_data['network']}, "
                f"identifier={network_points_data['identifier']}, block_range={previous_block_number}-{block_number}"
            )

        if (
            network_points_data["block_number_processed"] is not None
            and network_points_data["block_number_processed"] >= block_number
        ):
            if self.debug:
                print(
                    "[Points] Network already processed at or beyond this block. Skipping."
                )
            return

        s_onv, s_on, s_vn, s_n = self.get_delegation_related_stakes(
            network_points_data, collaterals_data
        )
        if s_n == 0:
            if self.debug:
                print("[Points] Total stake is zero, skipping points calculation.")
            return
        s_uv, s_v = self.get_deposit_related_stakes(
            [vault for vault in s_vn], collaterals_data
        )

        r = network_points_data["max_rate"]  # base - 1e48
        operator_fee = network_points_data["operator_fee"]  # base - 10000

        block_timestamp = self.w3_wrapper.get_block_timestamp(block_number)
        previous_block_timestamp = self.w3_wrapper.get_block_timestamp(
            previous_block_number
        )

        p_nt = (
            r * s_n * (block_timestamp - previous_block_timestamp) // (10**24 * 3600)
        )  # base - 1e48
        p_no = {
            operator: operator_fee * p_nt * s_on[operator] // (10000 * s_n)
            for operator in s_on
        }  # base - 1e48
        p_onv = {
            operator: {
                vault: (
                    (p_no[operator] * s_onv[operator][vault] // s_on[operator])
                    if s_on[operator] > 0
                    else 0
                )
                for vault in s_onv[operator]
            }
            for operator in p_no
        }  # base - 1e48
        p_nv = {
            vault: (10000 - operator_fee) * p_nt * s_vn[vault] // (10000 * s_n)
            for vault in s_vn
        }  # base - 1e48
        p_nvu = {
            vault: {
                user: (
                    (p_nv[vault] * s_uv[vault][user] // s_v[vault])
                    if s_v[vault] > 0
                    else 0
                )
                for user in s_uv[vault]
            }
            for vault in s_vn
        }  # base - 1e48

        if self.debug:
            print(
                f"[Points] Points calculation complete for network={network_points_data['network']}, applying updates..."
            )

        if p_onv:
            self.storage.save_network_operator_vault_points_batch(
                [
                    {
                        "network": network_points_data["network"],
                        "identifier": network_points_data["identifier"],
                        "operator": operator,
                        "vault": vault,
                        "points": p_onv[operator][vault],
                    }
                    for operator in p_onv
                    for vault in p_onv[operator]
                ]
            )
        if p_nvu:
            self.storage.save_network_vault_user_points_batch(
                [
                    {
                        "network": network_points_data["network"],
                        "identifier": network_points_data["identifier"],
                        "vault": vault,
                        "user": user,
                        "points": p_nvu[vault][user],
                    }
                    for vault in p_nvu
                    for user in p_nvu[vault]
                ]
            )
        self.storage.save_networks_points_data(
            {
                "network": network_points_data["network"],
                "identifier": network_points_data["identifier"],
                "max_rate": network_points_data["max_rate"],
                "target_stake": network_points_data["target_stake"],
                "network_fee": network_points_data["network_fee"],
                "operator_fee": network_points_data["operator_fee"],
                "block_number_processed": block_number,
            }
        )
        self.storage.commit()
        if self.debug:
            print(
                f"[Points] Points updates committed for network={network_points_data['network']} at block={block_number}"
            )

    def process_block(self, previous_block_number, block_number):
        if self.debug:
            print(
                f"[Points] process_block called for block range: {previous_block_number}-{block_number}"
            )

        last_processed_block = self.storage.get_processed_timepoint(self.name)
        if last_processed_block is not None and last_processed_block >= block_number:
            if self.debug:
                print("[Points] Block already processed. Skipping.")
            return

        networks_points_data = self.storage.get_all_networks_points_data()
        collaterals_data = self.storage.get_collaterals()
        collaterals_data = {
            collateral["collateral"]: collateral for collateral in collaterals_data
        }
        prices_data = self.storage.get_prices(previous_block_number)
        prices_data = {
            price_data["collateral"]: price_data for price_data in prices_data
        }
        if self.debug:
            print(
                f"[Points] Merging collateral data with prices at block={previous_block_number}"
            )
        for collateral in collaterals_data:
            if collateral in prices_data:
                collaterals_data[collateral] = {
                    **collaterals_data[collateral],
                    **prices_data[collateral],
                }

        for network_points_data in networks_points_data:
            self.parse_points_per_network(
                previous_block_number,
                block_number,
                network_points_data,
                collaterals_data,
            )
        print(f"[Points] Saving processed timepoint for block_number={block_number}")
        self.storage.save_processed_timepoint(self.name, block_number)
        self.storage.commit()

    def parse_points(self, previous_block_number, block_number):
        if self.debug:
            print(
                f"[Points] parse_points called, block_range={previous_block_number}-{block_number}"
            )
        # 1. Calculate the points at the current block number given the state and prices from the previous block number
        self.process_block(previous_block_number, block_number)
        # 2. Snapshot the points each 200 blocks
        if block_number % 200 == 0:
            last_snapshot_block = self.storage.get_last_snapshot_block_number()
            if last_snapshot_block is None or last_snapshot_block < block_number:
                if self.debug:
                    print(
                        f"[Points] Taking a snapshot of points at block={block_number}"
                    )
                self.storage.snapshot_points(block_number)
        # 3. Calculate a new state at the current block number
        #    (We update the state after the points to use the "previous" state data for points calculation)
        self.state.process_block(block_number)

    @retry(
        tries=5,
        delay=1,
        backoff=2,
        jitter=(0, 0.5),
        exceptions=(Exception,),
    )
    def parse_all_points(self):
        print("[Points] Starting parse_all_points...")
        zero_block, start_block = self.get_start_block()
        end_block = self.get_end_block()

        if start_block > end_block:
            print(
                "[Points] Start block is greater than the last block. Nothing to process."
            )
            return

        previous_block_number = start_block if zero_block else start_block - 1
        print(
            f"[Points] Beginning main loop from block={start_block} to block={end_block}"
        )

        for block_number in range(start_block, end_block + 1):
            self.parse_points(previous_block_number, block_number)
            previous_block_number = block_number


if __name__ == "__main__":
    config = Config()
    storage = Storage(config)
    w3_wrapper = Web3Wrapper(config, storage)
    points = Points(config, w3_wrapper, storage)

    points.parse_all_points()
    storage.close()
