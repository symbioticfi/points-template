class State:
    def __init__(self, config, w3_wrapper, storage):
        self.config = config
        self.w3_wrapper = w3_wrapper
        self.storage = storage
        self.name = self.config.get_state_module_name()

    def get_logs(self, block_number):
        print(f"[State] get_logs called for block_number={block_number}")
        raw_logs = sorted(
            [
                {
                    "address": self.w3_wrapper.addresses.operator_network_opt_in_service.address,
                    **log,
                }
                for log in self.storage.get_operator_network_opt_in_service_logs(
                    block_number, block_number
                )
            ]
            + [
                {
                    "address": self.w3_wrapper.addresses.operator_vault_opt_in_service.address,
                    **log,
                }
                for log in self.storage.get_operator_vault_opt_in_service_logs(
                    block_number, block_number
                )
            ]
            + self.storage.get_vault_logs(block_number, block_number)
            + self.storage.get_delegator_logs(block_number, block_number),
            key=lambda x: (x["blockNumber"], x["logIndex"]),
        )

        print(f"[State] Fetched {len(raw_logs)} raw logs at block {block_number}")
        return raw_logs

    def get_epoch_at(self, vault_address, timestamp):

        global_vars = self.storage.get_global_vars(vault_address)
        return (timestamp - global_vars["epochDurationInit"]) // global_vars[
            "epochDuration"
        ]

    def process_operator_network_opt_in_service_log_opt_in(self, log):
        print(
            f"[State] Processing OperatorNetworkOptInService OptIn event. Operator={log['args']['who']}, Network={log['args']['where']}"
        )
        self.storage.save_operator_network_opt_in_service_state(
            {
                "operator": log["args"]["who"],
                "network": log["args"]["where"],
                "status": True,
            }
        )

    def process_operator_network_opt_in_service_log_opt_out(self, log):
        print(
            f"[State] Processing OperatorNetworkOptInService OptOut event. Operator={log['args']['who']}, Network={log['args']['where']}"
        )
        self.storage.save_operator_network_opt_in_service_state(
            {
                "operator": log["args"]["who"],
                "network": log["args"]["where"],
                "status": False,
            }
        )

    def process_operator_vault_opt_in_service_log_opt_in(self, log):
        print(
            f"[State] Processing OperatorVaultOptInService OptIn event. Operator={log['args']['who']}, Vault={log['args']['where']}"
        )
        self.storage.save_operator_vault_opt_in_service_state(
            {
                "operator": log["args"]["who"],
                "vault": log["args"]["where"],
                "status": True,
            }
        )

    def process_operator_vault_opt_in_service_log_opt_out(self, log):
        print(
            f"[State] Processing OperatorVaultOptInService OptOut event. Operator={log['args']['who']}, Vault={log['args']['where']}"
        )
        self.storage.save_operator_vault_opt_in_service_state(
            {
                "operator": log["args"]["who"],
                "vault": log["args"]["where"],
                "status": False,
            }
        )

    def process_vault_log_deposit(self, log):
        print(
            f"[State] Processing Vault Deposit event. Vault={log['address']}, User={log['args']['onBehalfOf']}"
        )

        vault_global_state = self.storage.get_vault_global_state(log["address"])
        vault_user_state = self.storage.get_vault_user_state(
            log["address"], log["args"]["onBehalfOf"]
        )
        self.storage.save_vault_global_state(
            {
                "vault": log["address"],
                "activeShares": vault_global_state["activeShares"]
                + log["args"]["shares"],
                "activeStake": vault_global_state["activeStake"]
                + log["args"]["amount"],
            }
        )
        self.storage.save_vault_user_state(
            {
                "vault": log["address"],
                "user": log["args"]["onBehalfOf"],
                "activeSharesOf": vault_user_state["activeSharesOf"]
                + log["args"]["shares"],
            }
        )

    def process_vault_log_withdraw(self, log):
        print(
            f"[State] Processing Vault Withdraw event. Vault={log['address']}, User={log['args']['withdrawer']}"
        )

        vault_global_state = self.storage.get_vault_global_state(log["address"])
        self.storage.save_vault_global_state(
            {
                "vault": log["address"],
                "activeShares": vault_global_state["activeShares"]
                - log["args"]["burnedShares"],
                "activeStake": vault_global_state["activeStake"]
                - log["args"]["amount"],
            }
        )
        vault_user_state = self.storage.get_vault_user_state(
            log["address"], log["args"]["withdrawer"]
        )
        self.storage.save_vault_user_state(
            {
                "vault": log["address"],
                "user": log["args"]["withdrawer"],
                "activeSharesOf": vault_user_state["activeSharesOf"]
                - log["args"]["burnedShares"],
            }
        )

        withdrawal_epoch = (
            self.get_epoch_at(
                log["address"], self.w3_wrapper.get_block_timestamp(log["blockNumber"])
            )
            + 1
        )
        vault_global_withdrawals_state = (
            self.storage.get_vault_global_withdrawals_state(
                log["address"], withdrawal_epoch
            )
        )
        self.storage.save_vault_global_withdrawals_state(
            {
                "vault": log["address"],
                "epoch": withdrawal_epoch,
                "withdrawalShares": vault_global_withdrawals_state["withdrawalShares"]
                + log["args"]["mintedShares"],
                "withdrawals": vault_global_withdrawals_state["withdrawals"]
                + log["args"]["amount"],
            }
        )
        vault_user_withdrawals_state = self.storage.get_vault_user_withdrawals_state(
            log["address"], withdrawal_epoch, log["args"]["claimer"]
        )

        self.storage.save_vault_user_withdrawals_state(
            {
                "vault": log["address"],
                "epoch": withdrawal_epoch,
                "user": log["args"]["claimer"],
                "withdrawalSharesOf": vault_user_withdrawals_state["withdrawalSharesOf"]
                + log["args"]["mintedShares"],
            }
        )

    def process_vault_log_on_slash(self, log):
        print(f"[State] Processing Vault OnSlash event. Vault={log['address']}")

        event_epoch = self.get_epoch_at(
            log["address"], self.w3_wrapper.get_block_timestamp(log["blockNumber"])
        )
        vault_global_state = self.storage.get_vault_global_state(log["address"])
        vault_global_withdrawals_state_next = (
            self.storage.get_vault_global_withdrawals_state(
                log["address"], event_epoch + 1
            )
        )

        print(f"[State] OnSlash event_epoch={event_epoch}")

        if event_epoch != self.get_epoch_at(
            log["address"], log["args"]["captureTimestamp"]
        ):
            print(
                "[State] Slash event in previous epoch or forced slash scenario. Adjusting accordingly."
            )
            vault_global_withdrawals_state_next = (
                self.storage.get_vault_global_withdrawals_state(
                    log["address"], event_epoch
                )
            )
            activeStake_ = vault_global_state["activeStake"]
            nextWithdrawals = vault_global_withdrawals_state_next["withdrawals"]
            withdrawals_ = vault_global_withdrawals_state_next["withdrawals"]

            slashableAmount = activeStake_ + withdrawals_ + nextWithdrawals

            activeSlashed = (
                log["args"]["slashedAmount"] * activeStake_
            ) // slashableAmount
            nextWithdrawalsSlashed = (
                log["args"]["slashedAmount"] * nextWithdrawals
            ) // slashableAmount
            withdrawalsSlashed = (
                log["args"]["slashedAmount"] - activeSlashed - nextWithdrawalsSlashed
            )

            if withdrawals_ < withdrawalsSlashed:
                nextWithdrawalsSlashed += withdrawalsSlashed - withdrawals_
                withdrawalsSlashed = withdrawals_

            self.storage.save_vault_global_state(
                {
                    "vault": log["address"],
                    "activeShares": vault_global_state["activeShares"],
                    "activeStake": activeStake_ - activeSlashed,
                }
            )
            self.storage.save_vault_global_withdrawals_state(
                {
                    "vault": log["address"],
                    "epoch": event_epoch + 1,
                    "withdrawalShares": vault_global_withdrawals_state_next[
                        "withdrawalShares"
                    ],
                    "withdrawals": nextWithdrawals - nextWithdrawalsSlashed,
                }
            )
            self.storage.save_vault_global_withdrawals_state(
                {
                    "vault": log["address"],
                    "epoch": event_epoch,
                    "withdrawalShares": vault_global_withdrawals_state_next[
                        "withdrawalShares"
                    ],
                    "withdrawals": withdrawals_ - withdrawalsSlashed,
                }
            )
        else:
            print("[State] Slash event in the current epoch.")
            activeStake_ = vault_global_state["activeStake"]
            nextWithdrawals = vault_global_withdrawals_state_next["withdrawals"]
            slashableAmount = activeStake_ + nextWithdrawals

            activeSlashed = (
                log["args"]["slashedAmount"] * activeStake_
            ) // slashableAmount
            nextWithdrawalsSlashed = log["args"]["slashedAmount"] - activeSlashed

            self.storage.save_vault_global_state(
                {
                    "vault": log["address"],
                    "activeShares": vault_global_state["activeShares"],
                    "activeStake": activeStake_ - activeSlashed,
                }
            )
            self.storage.save_vault_global_withdrawals_state(
                {
                    "vault": log["address"],
                    "epoch": event_epoch + 1,
                    "withdrawalShares": vault_global_withdrawals_state_next[
                        "withdrawalShares"
                    ],
                    "withdrawals": nextWithdrawals - nextWithdrawalsSlashed,
                }
            )

        print("[State] OnSlash processed successfully.")

    def process_vault_log_transfer(self, log):
        print(
            f"[State] Processing Vault Transfer event. Vault={log['address']}, From={log['args']['from']}, To={log['args']['to']}"
        )
        if (
            log["args"]["from"] == "0x0000000000000000000000000000000000000000"
            or log["args"]["to"] == "0x0000000000000000000000000000000000000000"
        ):
            print(
                "[State] Transfer from or to zero address. No user state update needed."
            )
            return

        vault_user_state_from = self.storage.get_vault_user_state(
            log["address"], log["args"]["from"]
        )
        self.storage.save_vault_user_state(
            {
                "vault": log["address"],
                "user": log["args"]["from"],
                "activeSharesOf": vault_user_state_from["activeSharesOf"]
                - log["args"]["value"],
            }
        )

        vault_user_state_to = self.storage.get_vault_user_state(
            log["address"], log["args"]["to"]
        )
        self.storage.save_vault_user_state(
            {
                "vault": log["address"],
                "user": log["args"]["to"],
                "activeSharesOf": vault_user_state_to["activeSharesOf"]
                + log["args"]["value"],
            }
        )

    def process_delegator_log_set_max_network_limit(self, log):
        print("[State] Processing SetMaxNetworkLimit event for delegator.")
        global_vars = self.storage.get_global_vars(log["address"])
        self.storage.save_delegator_network_state(
            {
                "delegator": log["address"],
                "network": log["args"]["network"],
                "identifier": log["args"]["identifier"],
                "maxNetworkLimit": log["args"]["amount"],
            }
        )
        if global_vars["delegator_type"] == 0:
            delegator_network_state = self.storage.get_delegator0_network_state(
                log["address"], log["args"]["network"], log["args"]["identifier"]
            )
            self.storage.save_delegator0_network_state(
                {
                    "delegator": log["address"],
                    "network": log["args"]["network"],
                    "identifier": log["args"]["identifier"],
                    "networkLimit": min(
                        log["args"]["amount"],
                        delegator_network_state["networkLimit"],
                    ),
                    "totalOperatorNetworkShares": delegator_network_state[
                        "totalOperatorNetworkShares"
                    ],
                }
            )
        elif global_vars["delegator_type"] == 1:
            delegator_network_state = self.storage.get_delegator1_network_state(
                log["address"], log["args"]["network"], log["args"]["identifier"]
            )
            self.storage.save_delegator1_network_state(
                {
                    "delegator": log["address"],
                    "network": log["args"]["network"],
                    "identifier": log["args"]["identifier"],
                    "networkLimit": min(
                        log["args"]["amount"],
                        delegator_network_state["networkLimit"],
                    ),
                }
            )
        elif global_vars["delegator_type"] == 2:
            delegator_network_state = self.storage.get_delegator2_network_state(
                log["address"], log["args"]["network"], log["args"]["identifier"]
            )
            self.storage.save_delegator2_network_state(
                {
                    "delegator": log["address"],
                    "network": log["args"]["network"],
                    "identifier": log["args"]["identifier"],
                    "networkLimit": min(
                        log["args"]["amount"],
                        delegator_network_state["networkLimit"],
                    ),
                }
            )
        elif global_vars["delegator_type"] == 3:
            pass
        else:
            raise Exception("Unsupported delegator type")

    def process_delegator_log_set_network_limit(self, log):
        print("[State] Processing SetNetworkLimit event for delegator.")
        global_vars = self.storage.get_global_vars(log["address"])
        if global_vars["delegator_type"] == 0:
            delegator_network_state = self.storage.get_delegator0_network_state(
                log["address"], log["args"]["network"], log["args"]["identifier"]
            )
            self.storage.save_delegator0_network_state(
                {
                    "delegator": log["address"],
                    "network": log["args"]["network"],
                    "identifier": log["args"]["identifier"],
                    "networkLimit": log["args"]["amount"],
                    "totalOperatorNetworkShares": delegator_network_state[
                        "totalOperatorNetworkShares"
                    ],
                }
            )
        elif global_vars["delegator_type"] == 1:
            delegator_network_state = self.storage.get_delegator1_network_state(
                log["address"], log["args"]["network"], log["args"]["identifier"]
            )
            self.storage.save_delegator1_network_state(
                {
                    "delegator": log["address"],
                    "network": log["args"]["network"],
                    "identifier": log["args"]["identifier"],
                    "networkLimit": log["args"]["amount"],
                }
            )
        elif global_vars["delegator_type"] == 2:
            delegator_network_state = self.storage.get_delegator2_network_state(
                log["address"], log["args"]["network"], log["args"]["identifier"]
            )
            self.storage.save_delegator2_network_state(
                {
                    "delegator": log["address"],
                    "network": log["args"]["network"],
                    "identifier": log["args"]["identifier"],
                    "networkLimit": log["args"]["amount"],
                }
            )
        else:
            raise Exception("Unsupported delegator type")

    def process_delegator_log_set_operator_network_shares(self, log):
        print("[State] Processing SetOperatorNetworkShares event for delegator.")
        global_vars = self.storage.get_global_vars(log["address"])
        if global_vars["delegator_type"] == 0:
            delegator_operator_network_state = (
                self.storage.get_delegator0_operator_network_state(
                    log["address"],
                    log["args"]["network"],
                    log["args"]["identifier"],
                    log["args"]["operator"],
                )
            )
            self.storage.save_delegator0_operator_network_state(
                {
                    "delegator": log["address"],
                    "network": log["args"]["network"],
                    "identifier": log["args"]["identifier"],
                    "operator": log["args"]["operator"],
                    "operatorNetworkShares": log["args"]["shares"],
                }
            )
            delegator_network_state = self.storage.get_delegator0_network_state(
                log["address"], log["args"]["network"], log["args"]["identifier"]
            )
            self.storage.save_delegator0_network_state(
                {
                    "delegator": log["address"],
                    "network": log["args"]["network"],
                    "identifier": log["args"]["identifier"],
                    "networkLimit": delegator_network_state["networkLimit"],
                    "totalOperatorNetworkShares": delegator_network_state[
                        "totalOperatorNetworkShares"
                    ]
                    - delegator_operator_network_state["operatorNetworkShares"]
                    + log["args"]["shares"],
                }
            )
        else:
            raise Exception("Unsupported delegator type")

    def process_delegator_log_set_operator_network_limit(self, log):
        print("[State] Processing SetOperatorNetworkLimit event for delegator.")
        global_vars = self.storage.get_global_vars(log["address"])
        if global_vars["delegator_type"] == 1:
            self.storage.save_delegator1_operator_network_state(
                {
                    "delegator": log["address"],
                    "network": log["args"]["network"],
                    "identifier": log["args"]["identifier"],
                    "operator": log["args"]["operator"],
                    "operatorNetworkLimit": log["args"]["amount"],
                }
            )
        else:
            raise Exception("Unsupported delegator type")

    def process_log(self, log):
        print(f"[State] Processing log event={log['event']}, address={log['address']}")
        if log["event"] == "OptIn":
            if (
                log["address"]
                == self.w3_wrapper.addresses.operator_network_opt_in_service.address
            ):
                self.process_operator_network_opt_in_service_log_opt_in(log)
            elif (
                log["address"]
                == self.w3_wrapper.addresses.operator_vault_opt_in_service.address
            ):
                self.process_operator_vault_opt_in_service_log_opt_in(log)
        elif log["event"] == "OptOut":
            if (
                log["address"]
                == self.w3_wrapper.addresses.operator_network_opt_in_service.address
            ):
                self.process_operator_network_opt_in_service_log_opt_out(log)
            elif (
                log["address"]
                == self.w3_wrapper.addresses.operator_vault_opt_in_service.address
            ):
                self.process_operator_vault_opt_in_service_log_opt_out(log)
        elif log["event"] == "Deposit":
            self.process_vault_log_deposit(log)
        elif log["event"] == "Withdraw":
            self.process_vault_log_withdraw(log)
        elif log["event"] == "OnSlash":
            self.process_vault_log_on_slash(log)
        elif log["event"] == "Transfer":
            self.process_vault_log_transfer(log)
        elif log["event"] == "SetMaxNetworkLimit":
            self.process_delegator_log_set_max_network_limit(log)
        elif log["event"] == "SetNetworkLimit":
            self.process_delegator_log_set_network_limit(log)
        elif log["event"] == "SetOperatorNetworkShares":
            self.process_delegator_log_set_operator_network_shares(log)
        elif log["event"] == "SetOperatorNetworkLimit":
            self.process_delegator_log_set_operator_network_limit(log)

    def process_block(self, block_number):
        print(f"[State] process_block called for block_number={block_number}")
        logs = self.get_logs(block_number)
        print(f"[State] Processing {len(logs)} logs for block_number={block_number}")

        for log in logs:
            self.process_log(log)

        print(f"[State] Saving processed timepoint for block_number={block_number}")
        self.storage.save_processed_timepoint(self.name, block_number)
        self.storage.commit()
