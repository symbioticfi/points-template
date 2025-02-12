import psycopg2
from psycopg2.extras import execute_values
from decimal import *

from .helpers import Helpers
from .constants import PATH


def int_to_numeric(value: int) -> Decimal:
    """
    Convert a Python int (up to 256-bit) into a string
    that can be inserted into a NUMERIC(78,0) column.
    """
    return Decimal(value)


def numeric_to_int(db_value) -> int:
    """
    Convert a PostgreSQL NUMERIC(78,0) or DECIMAL into Python int.
    If db_value is None, return 0.
    """
    if db_value is None:
        return 0
    return int(db_value)


class Storage:
    def __init__(self, config, test=False, copy=False, init=False):
        self.config = config

        if test and copy:
            test_data = self.config.get_storage_data(test=True)
            prod_data = self.config.get_storage_data(test=False)
            conn = psycopg2.connect(
                dbname="postgres",
                user="postgres",
                password=prod_data["password"],
                host=prod_data["host"],
                port=prod_data["port"],
            )
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f"DROP DATABASE IF EXISTS {test_data['dbname']}")
                cur.execute(
                    f"CREATE DATABASE {test_data['dbname']} TEMPLATE {prod_data['dbname']}"
                )
            conn.close()

        data = self.config.get_storage_data(test=test)
        self.connection = psycopg2.connect(
            dbname=data["dbname"],
            user=data["user"],
            password=data["password"],
            host=data["host"],
            port=data["port"],
        )
        self.cursor = self.connection.cursor()

        if init:
            self.create_postgres_functions()
            self.setup_tables()
            self.setup_indexes()

    def create_postgres_functions(self):
        # stake(...)
        self.cursor.execute(
            """
            CREATE OR REPLACE FUNCTION public.stake(
                delegator_type INT,
                isOptedInNetwork INT,
                isOptedInVault INT,
                activeStake NUMERIC(78,0),
                maxNetworkLimit NUMERIC(78,0),
                networkLimit NUMERIC(78,0),
                operatorNetworkShares NUMERIC(78,0),
                totalOperatorNetworkShares NUMERIC(78,0),
                operatorNetworkLimit NUMERIC(78,0),
                isValidOperator INT,
                isValidNetwork INT
            )
            RETURNS NUMERIC(78,0)
            LANGUAGE plpgsql
            IMMUTABLE
            AS $$
            DECLARE
                result NUMERIC(78,0) := 0;
            BEGIN
                IF isOptedInNetwork * isOptedInVault = 0 THEN
                    RETURN 0;
                END IF;

                IF delegator_type = 0 THEN
                    IF totalOperatorNetworkShares = 0 THEN
                        RETURN 0;
                    END IF;
                    result := DIV(
                        operatorNetworkShares * LEAST(activeStake, networkLimit), totalOperatorNetworkShares
                    );

                ELSIF delegator_type = 1 THEN
                    result := LEAST(activeStake, LEAST(networkLimit, operatorNetworkLimit));

                ELSIF delegator_type = 2 THEN
                    IF isValidOperator = 0 THEN
                        RETURN 0;
                    END IF;
                    result := LEAST(activeStake, networkLimit);

                ELSIF delegator_type = 3 THEN
                    IF isValidOperator = 0 THEN
                        RETURN 0;
                    END IF;
                    IF isValidNetwork = 0 THEN
                        RETURN 0;
                    END IF;
                    result := LEAST(activeStake, maxNetworkLimit);
                ELSE
                    result := 0;
                END IF;

                RETURN result;
            END;
            $$;
            """
        )

        # active_balance_of(...)
        self.cursor.execute(
            """
            CREATE OR REPLACE FUNCTION public.active_balance_of(
                activeSharesOf NUMERIC(78,0),
                activeStake NUMERIC(78,0),
                activeShares NUMERIC(78,0)
            )
            RETURNS NUMERIC(78,0)
            LANGUAGE plpgsql
            IMMUTABLE
            AS $$
            DECLARE
                result NUMERIC(78,0) := 0;
            BEGIN
                IF activeShares = 0 THEN
                    RETURN 0;
                END IF;
                result := DIV(
                    activeSharesOf * activeStake, activeShares
                );
                RETURN result;
            END;
            $$;
            """
        )

        self.commit()

    def setup_tables(self):
        # BlocksData
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS BlocksData (
                number BIGINT PRIMARY KEY,
                timestamp BIGINT,
                hash CHAR(66)
            );
            """
        )

        # ProcessedTimepoints
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS ProcessedTimepoints (
                name TEXT PRIMARY KEY,
                timepoint BIGINT
            );
            """
        )

        # Collaterals
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Collaterals (
                collateral CHAR(42) PRIMARY KEY,
                decimals INT,
                name TEXT,
                symbol TEXT,
                cmcID INT
            );
            """
        )

        # NetworksPointsData
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS NetworksPointsData (
                network CHAR(42),
                identifier NUMERIC(78,0),
                max_rate NUMERIC(78,0),
                target_stake NUMERIC(78,0),
                network_fee BIGINT,
                operator_fee BIGINT,
                block_number_processed BIGINT,
                PRIMARY KEY (network, identifier)
            );
            """
        )

        # GlobalVars
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS GlobalVars (
                vault CHAR(42) PRIMARY KEY,
                delegator CHAR(42),
                delegator_type INT,
                collateral CHAR(42),
                epochDurationInit BIGINT,
                epochDuration BIGINT,
                operator CHAR(42),
                network CHAR(42)
            );
            """
        )

        # Prices
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Prices (
                collateral CHAR(42),
                block_number BIGINT,
                price NUMERIC(78,0),
                PRIMARY KEY (collateral, block_number)
            );
            """
        )

        # OperatorNetworkOptInService logs
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS OperatorNetworkOptInServiceOptInLogs (
                block_number BIGINT,
                log_index BIGINT,
                who CHAR(42),
                where_ CHAR(42),
                PRIMARY KEY (block_number, log_index)
            );
            """
        )
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS OperatorNetworkOptInServiceOptOutLogs (
                block_number BIGINT,
                log_index BIGINT,
                who CHAR(42),
                where_ CHAR(42),
                PRIMARY KEY (block_number, log_index)
            );
            """
        )

        # OperatorVaultOptInService logs
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS OperatorVaultOptInServiceOptInLogs (
                block_number BIGINT,
                log_index BIGINT,
                who CHAR(42),
                where_ CHAR(42),
                PRIMARY KEY (block_number, log_index)
            );
            """
        )
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS OperatorVaultOptInServiceOptOutLogs (
                block_number BIGINT,
                log_index BIGINT,
                who CHAR(42),
                where_ CHAR(42),
                PRIMARY KEY (block_number, log_index)
            );
            """
        )

        # Vault logs
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS VaultDepositLogs (
                block_number BIGINT,
                log_index BIGINT,
                vault CHAR(42),
                depositor CHAR(42),
                onBehalfOf CHAR(42),
                amount NUMERIC(78,0),
                shares NUMERIC(78,0),
                PRIMARY KEY (block_number, log_index, vault)
            );
            """
        )
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS VaultWithdrawLogs (
                block_number BIGINT,
                log_index BIGINT,
                vault CHAR(42),
                withdrawer CHAR(42),
                claimer CHAR(42),
                amount NUMERIC(78,0),
                burnedShares NUMERIC(78,0),
                mintedShares NUMERIC(78,0),
                PRIMARY KEY (block_number, log_index, vault)
            );
            """
        )
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS VaultOnSlashLogs (
                block_number BIGINT,
                log_index BIGINT,
                vault CHAR(42),
                amount NUMERIC(78,0),
                captureTimestamp BIGINT,
                slashedAmount NUMERIC(78,0),
                PRIMARY KEY (block_number, log_index, vault)
            );
            """
        )
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS VaultTransferLogs (
                block_number BIGINT,
                log_index BIGINT,
                vault CHAR(42),
                from_ CHAR(42),
                to_ CHAR(42),
                value NUMERIC(78,0),
                PRIMARY KEY (block_number, log_index, vault)
            );
            """
        )

        # Delegator logs
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS DelegatorSetMaxNetworkLimitLogs (
                block_number BIGINT,
                log_index BIGINT,
                delegator CHAR(42),
                network CHAR(42),
                identifier NUMERIC(78,0),
                amount NUMERIC(78,0),
                PRIMARY KEY (block_number, log_index, delegator)
            );
            """
        )
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS DelegatorSetNetworkLimitLogs (
                block_number BIGINT,
                log_index BIGINT,
                delegator CHAR(42),
                network CHAR(42),
                identifier NUMERIC(78,0),
                amount NUMERIC(78,0),
                PRIMARY KEY (block_number, log_index, delegator)
            );
            """
        )
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS DelegatorSetOperatorNetworkSharesLogs (
                block_number BIGINT,
                log_index BIGINT,
                delegator CHAR(42),
                network CHAR(42),
                identifier NUMERIC(78,0),
                operator CHAR(42),
                shares NUMERIC(78,0),
                PRIMARY KEY (block_number, log_index, delegator)
            );
            """
        )
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS DelegatorSetOperatorNetworkLimitLogs (
                block_number BIGINT,
                log_index BIGINT,
                delegator CHAR(42),
                network CHAR(42),
                identifier NUMERIC(78,0),
                operator CHAR(42),
                amount NUMERIC(78,0),
                PRIMARY KEY (block_number, log_index, delegator)
            );
            """
        )

        # Opt-in/out state
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS OperatorNetworkOptInServiceState (
                operator CHAR(42),
                network CHAR(42),
                status BOOLEAN,
                PRIMARY KEY (operator, network)
            );
            """
        )
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS OperatorVaultOptInServiceState (
                operator CHAR(42),
                vault CHAR(42),
                status BOOLEAN,
                PRIMARY KEY (operator, vault)
            );
            """
        )

        # Vault state
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS VaultGlobalState (
                vault CHAR(42) PRIMARY KEY,
                activeShares NUMERIC(78,0),
                activeStake NUMERIC(78,0)
            );
            """
        )
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS VaultUserState (
                vault CHAR(42),
                staker CHAR(42),
                activeSharesOf NUMERIC(78,0),
                PRIMARY KEY (vault, staker)
            );
            """
        )
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS VaultGlobalWithdrawalsState (
                vault CHAR(42),
                epoch BIGINT,
                withdrawalShares NUMERIC(78,0),
                withdrawals NUMERIC(78,0),
                PRIMARY KEY (vault, epoch)
            );
            """
        )
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS VaultUserWithdrawalsState (
                vault CHAR(42),
                epoch BIGINT,
                staker CHAR(42),
                withdrawalSharesOf NUMERIC(78,0),
                PRIMARY KEY (vault, epoch, staker)
            );
            """
        )

        # Delegator states
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS DelegatorNetworkState (
                delegator CHAR(42),
                network CHAR(42),
                identifier NUMERIC(78,0),
                maxNetworkLimit NUMERIC(78,0),
                PRIMARY KEY (delegator, network, identifier)
            );
            """
        )
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Delegator0NetworkState (
                delegator CHAR(42),
                network CHAR(42),
                identifier NUMERIC(78,0),
                networkLimit NUMERIC(78,0),
                totalOperatorNetworkShares NUMERIC(78,0),
                PRIMARY KEY (delegator, network, identifier)
            );
            """
        )
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Delegator0OperatorNetworkState (
                delegator CHAR(42),
                network CHAR(42),
                identifier NUMERIC(78,0),
                operator CHAR(42),
                operatorNetworkShares NUMERIC(78,0),
                PRIMARY KEY (delegator, network, identifier, operator)
            );
            """
        )
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Delegator1NetworkState (
                delegator CHAR(42),
                network CHAR(42),
                identifier NUMERIC(78,0),
                networkLimit NUMERIC(78,0),
                PRIMARY KEY (delegator, network, identifier)
            );
            """
        )
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Delegator1OperatorNetworkState (
                delegator CHAR(42),
                network CHAR(42),
                identifier NUMERIC(78,0),
                operator CHAR(42),
                operatorNetworkLimit NUMERIC(78,0),
                PRIMARY KEY (delegator, network, identifier, operator)
            );
            """
        )
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Delegator2NetworkState (
                delegator CHAR(42),
                network CHAR(42),
                identifier NUMERIC(78,0),
                networkLimit NUMERIC(78,0),
                PRIMARY KEY (delegator, network, identifier)
            );
            """
        )

        # Points
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS NetworkVaultPoints (
                network CHAR(42),
                identifier NUMERIC(78,0),
                vault CHAR(42),
                points NUMERIC(78,0),
                PRIMARY KEY (network, identifier, vault)
            );
            """
        )
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS NetworkOperatorVaultPoints (
                network CHAR(42),
                identifier NUMERIC(78,0),
                operator CHAR(42),
                vault CHAR(42),
                points NUMERIC(78,0),
                PRIMARY KEY (network, identifier, operator, vault)
            );
            """
        )
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS NetworkVaultUserPoints (
                network CHAR(42),
                identifier NUMERIC(78,0),
                vault CHAR(42),
                staker CHAR(42),
                points NUMERIC(78,0),
                PRIMARY KEY (network, identifier, vault, staker)
            );
            """
        )

        # Historical
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS NetworkVaultPointsHistorical (
                block_number BIGINT,
                network CHAR(42),
                identifier NUMERIC(78,0),
                vault CHAR(42),
                points NUMERIC(78,0),
                PRIMARY KEY (block_number, network, identifier, vault)
            );
            """
        )
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS NetworkOperatorVaultPointsHistorical (
                block_number BIGINT,
                network CHAR(42),
                identifier NUMERIC(78,0),
                operator CHAR(42),
                vault CHAR(42),
                points NUMERIC(78,0),
                PRIMARY KEY (block_number, network, identifier, operator, vault)
            );
            """
        )
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS NetworkVaultUserPointsHistorical (
                block_number BIGINT,
                network CHAR(42),
                identifier NUMERIC(78,0),
                vault CHAR(42),
                staker CHAR(42),
                points NUMERIC(78,0),
                PRIMARY KEY (block_number, network, identifier, vault, staker)
            );
            """
        )

        self.commit()

    def setup_indexes(self):
        orig_autocommit = self.connection.autocommit
        self.connection.autocommit = True
        try:
            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_blocksdata_timestamp_number
                ON BlocksData(timestamp, number);
                """
            )

            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_collaterals_symbol
                ON Collaterals(symbol);
                """
            )
            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_collaterals_cmcid
                ON Collaterals(cmcID);
                """
            )

            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_networkspointsdata_block_number
                ON NetworksPointsData(block_number_processed);
                """
            )

            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_globalvars_delegator
                ON GlobalVars(delegator);
                """
            )
            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_globalvars_network
                ON GlobalVars(network);
                """
            )
            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_globalvars_collateral
                ON GlobalVars(collateral);
                """
            )

            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_prices_collateral_block_number_desc
                ON Prices(collateral, block_number DESC);
                """
            )

            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_onosoptinlogs_who
                ON OperatorNetworkOptInServiceOptInLogs(who);
                """
            )
            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_onosoptinlogs_where_
                ON OperatorNetworkOptInServiceOptInLogs(where_);
                """
            )

            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_onosoptoutlogs_who
                ON OperatorNetworkOptInServiceOptOutLogs(who);
                """
            )
            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_onosoptoutlogs_where
                ON OperatorNetworkOptInServiceOptOutLogs(where_);
                """
            )

            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ovosoptinlogs_who
                ON OperatorVaultOptInServiceOptInLogs(who);
                """
            )
            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ovosoptinlogs_where
                ON OperatorVaultOptInServiceOptInLogs(where_);
                """
            )

            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ovosoptoutlogs_who
                ON OperatorVaultOptInServiceOptOutLogs(who);
                """
            )
            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ovosoptoutlogs_where
                ON OperatorVaultOptInServiceOptOutLogs(where_);
                """
            )

            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_vaultdepositlogs_vault
                ON VaultDepositLogs(vault);
                """
            )
            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_vaultdepositlogs_depositor
                ON VaultDepositLogs(depositor);
                """
            )

            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_vaultwithdrawlogs_vault
                ON VaultWithdrawLogs(vault);
                """
            )
            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_vaultwithdrawlogs_withdrawer
                ON VaultWithdrawLogs(withdrawer);
                """
            )

            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_vaultonslashlogs_vault
                ON VaultOnSlashLogs(vault);
                """
            )
            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_vaultonslashlogs_capturetimestamp
                ON VaultOnSlashLogs(captureTimestamp);
                """
            )

            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_vaulttransferlogs_vault
                ON VaultTransferLogs(vault);
                """
            )
            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_vaulttransferlogs_from
                ON VaultTransferLogs(from_);
                """
            )
            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_vaulttransferlogs_to
                ON VaultTransferLogs(to_);
                """
            )

            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dsmnll_delegator_network
                ON DelegatorSetMaxNetworkLimitLogs(delegator, network);
                """
            )
            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dsmnll_identifier
                ON DelegatorSetMaxNetworkLimitLogs(identifier);
                """
            )

            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dsnl_delegator_network
                ON DelegatorSetNetworkLimitLogs(delegator, network);
                """
            )
            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dsnl_identifier
                ON DelegatorSetNetworkLimitLogs(identifier);
                """
            )

            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dsons_delegator_network
                ON DelegatorSetOperatorNetworkSharesLogs(delegator, network);
                """
            )
            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dsons_operator
                ON DelegatorSetOperatorNetworkSharesLogs(operator);
                """
            )

            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dsonl_delegator_network
                ON DelegatorSetOperatorNetworkLimitLogs(delegator, network);
                """
            )
            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_dsonl_operator
                ON DelegatorSetOperatorNetworkLimitLogs(operator);
                """
            )

            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_onos_state_status
                ON OperatorNetworkOptInServiceState(status);
                """
            )

            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ovos_state_status
                ON OperatorVaultOptInServiceState(status);
                """
            )

            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_vaultuserstate_staker
                ON VaultUserState(staker);
                """
            )

            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_vaultglobalwithdrawals_epoch
                ON VaultGlobalWithdrawalsState(epoch);
                """
            )

            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_vaultuserwithdrawals_staker
                ON VaultUserWithdrawalsState(staker);
                """
            )

            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_delegatornetworkstate_network_identifier
                ON DelegatorNetworkState(network, identifier);
                """
            )

            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_delegator0networkstate_network_identifier
                ON Delegator0NetworkState(network, identifier);
                """
            )

            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_delegator0opnetworkstate_operator
                ON Delegator0OperatorNetworkState(operator);
                """
            )

            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_delegator1networkstate_network_identifier
                ON Delegator1NetworkState(network, identifier);
                """
            )

            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_delegator1opnetworkstate_operator
                ON Delegator1OperatorNetworkState(operator);
                """
            )

            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_delegator2networkstate_network_identifier
                ON Delegator2NetworkState(network, identifier);
                """
            )

            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_networkvaultpoints_vault
                ON NetworkVaultPoints(vault);
                """
            )

            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_networkopvaultpoints_operator
                ON NetworkOperatorVaultPoints(operator);
                """
            )
            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_networkopvaultpoints_vault
                ON NetworkOperatorVaultPoints(vault);
                """
            )

            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_networkvaultuserpoints_staker
                ON NetworkVaultUserPoints(staker);
                """
            )
            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_networkvaultuserpoints_vault
                ON NetworkVaultUserPoints(vault);
                """
            )

            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_nvph_network
                ON NetworkVaultPointsHistorical(network);
                """
            )
            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_nvph_vault
                ON NetworkVaultPointsHistorical(vault);
                """
            )

            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_novph_operator
                ON NetworkOperatorVaultPointsHistorical(operator);
                """
            )
            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_novph_vault
                ON NetworkOperatorVaultPointsHistorical(vault);
                """
            )

            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_nvuph_staker
                ON NetworkVaultUserPointsHistorical(staker);
                """
            )
            self.cursor.execute(
                """
                CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_nvuph_vault
                ON NetworkVaultUserPointsHistorical(vault);
                """
            )
        except Exception as e:
            print(f"Error creating indexes: {str(e)}")
        self.connection.autocommit = orig_autocommit

    # -------------------------------------------------------------------------
    # Commit / Close
    # -------------------------------------------------------------------------
    def commit(self):
        self.connection.commit()

    def close(self):
        self.connection.close()

    # -------------------------------------------------------------------------
    # Drop State Data
    # -------------------------------------------------------------------------
    def drop_state_data(self):
        self.cursor.execute("DROP TABLE IF EXISTS OperatorNetworkOptInServiceState;")
        self.cursor.execute("DROP TABLE IF EXISTS OperatorVaultOptInServiceState;")
        self.cursor.execute("DROP TABLE IF EXISTS VaultGlobalState;")
        self.cursor.execute("DROP TABLE IF EXISTS VaultUserState;")
        self.cursor.execute("DROP TABLE IF EXISTS VaultGlobalWithdrawalsState;")
        self.cursor.execute("DROP TABLE IF EXISTS VaultUserWithdrawalsState;")
        self.cursor.execute("DROP TABLE IF EXISTS DelegatorNetworkState;")
        self.cursor.execute("DROP TABLE IF EXISTS Delegator0NetworkState;")
        self.cursor.execute("DROP TABLE IF EXISTS Delegator0OperatorNetworkState;")
        self.cursor.execute("DROP TABLE IF EXISTS Delegator1NetworkState;")
        self.cursor.execute("DROP TABLE IF EXISTS Delegator1OperatorNetworkState;")
        self.cursor.execute("DROP TABLE IF EXISTS Delegator2NetworkState;")

        self.cursor.execute(
            """
            DELETE FROM ProcessedTimepoints WHERE name=%s
            """,
            (self.config.get_state_module_name(),),
        )
        self.commit()

    # -------------------------------------------------------------------------
    # BlocksData
    # -------------------------------------------------------------------------
    def save_block_data(self, block: dict):
        """
        Upsert into BlocksData
        """
        self.cursor.execute(
            """
            INSERT INTO BlocksData (number, timestamp, hash)
            VALUES (%s, %s, %s)
            ON CONFLICT (number)
            DO UPDATE SET
                timestamp = EXCLUDED.timestamp,
                hash = EXCLUDED.hash
            """,
            (block["number"], block["timestamp"], block["hash"]),
        )

    def get_block_data(self, block_number: int):
        self.cursor.execute(
            "SELECT timestamp, hash FROM BlocksData WHERE number=%s",
            (block_number,),
        )
        row = self.cursor.fetchone()
        if row:
            return {
                "number": block_number,
                "timestamp": row[0],
                "hash": row[1],
            }
        return None

    def get_block_number_by_timestamp(self, timestamp: int):
        self.cursor.execute(
            "SELECT MAX(number) FROM BlocksData WHERE timestamp<=%s",
            (timestamp,),
        )
        row = self.cursor.fetchone()
        return row[0] if row else None

    # -------------------------------------------------------------------------
    # ProcessedTimepoints
    # -------------------------------------------------------------------------
    def save_processed_timepoint(self, name: str, timepoint: int):
        self.cursor.execute(
            """
            INSERT INTO ProcessedTimepoints (name, timepoint)
            VALUES (%s, %s)
            ON CONFLICT (name)
            DO UPDATE SET
                timepoint = EXCLUDED.timepoint
            """,
            (name, timepoint),
        )

    def get_processed_timepoint(self, name: str):
        self.cursor.execute(
            "SELECT timepoint FROM ProcessedTimepoints WHERE name=%s",
            (name,),
        )
        row = self.cursor.fetchone()
        return row[0] if row else None

    # -------------------------------------------------------------------------
    # Collaterals
    # -------------------------------------------------------------------------
    def save_collateral(self, collateral: dict):
        self.cursor.execute(
            """
            INSERT INTO Collaterals (collateral, decimals, name, symbol, cmcID)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (collateral)
            DO UPDATE SET
                decimals = EXCLUDED.decimals,
                name = EXCLUDED.name,
                symbol = EXCLUDED.symbol,
                cmcID = EXCLUDED.cmcID
            """,
            (
                collateral["collateral"],
                collateral["decimals"],
                collateral["name"],
                collateral["symbol"],
                collateral["cmcID"],
            ),
        )

    def get_collaterals(self):
        self.cursor.execute(
            "SELECT collateral, decimals, name, symbol, cmcID FROM Collaterals"
        )
        rows = self.cursor.fetchall()
        return [
            {
                "collateral": r[0],
                "decimals": r[1],
                "name": r[2],
                "symbol": r[3],
                "cmcID": r[4],
            }
            for r in rows
        ]

    # -------------------------------------------------------------------------
    # NetworksPointsData
    # -------------------------------------------------------------------------
    def save_networks_points_data(self, data: dict):
        self.cursor.execute(
            """
            INSERT INTO NetworksPointsData (
                network,
                identifier,
                max_rate,
                target_stake,
                network_fee,
                operator_fee,
                block_number_processed
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (network, identifier)
            DO UPDATE SET
                max_rate = EXCLUDED.max_rate,
                target_stake = EXCLUDED.target_stake,
                network_fee = EXCLUDED.network_fee,
                operator_fee = EXCLUDED.operator_fee,
                block_number_processed = EXCLUDED.block_number_processed
            """,
            (
                data["network"],
                int_to_numeric(data["identifier"]),
                int_to_numeric(data["max_rate"]),
                int_to_numeric(data["target_stake"]),
                data["network_fee"],
                data["operator_fee"],
                data["block_number_processed"],
            ),
        )

    def get_all_networks_points_data(self):
        self.cursor.execute(
            """
            SELECT 
                network,
                identifier,
                max_rate,
                target_stake,
                network_fee,
                operator_fee,
                block_number_processed
            FROM NetworksPointsData
            """
        )
        rows = self.cursor.fetchall()
        return [
            {
                "network": r[0],
                "identifier": numeric_to_int(r[1]),
                "max_rate": numeric_to_int(r[2]),
                "target_stake": numeric_to_int(r[3]),
                "network_fee": r[4],
                "operator_fee": r[5],
                "block_number_processed": r[6],
            }
            for r in rows
        ]

    # -------------------------------------------------------------------------
    # GlobalVars
    # -------------------------------------------------------------------------
    def save_global_vars(self, var_sets: list):
        for var_set in var_sets:
            self.cursor.execute(
                """
                INSERT INTO GlobalVars (
                    vault,
                    delegator,
                    delegator_type,
                    collateral,
                    epochDurationInit,
                    epochDuration,
                    operator,
                    network
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (vault)
                DO UPDATE SET
                    delegator = EXCLUDED.delegator,
                    delegator_type = EXCLUDED.delegator_type,
                    collateral = EXCLUDED.collateral,
                    epochDurationInit = EXCLUDED.epochDurationInit,
                    epochDuration = EXCLUDED.epochDuration,
                    operator = EXCLUDED.operator,
                    network = EXCLUDED.network
                """,
                (
                    var_set["vault"],
                    var_set["delegator"],
                    var_set["delegator_type"],
                    var_set["collateral"],
                    var_set["epochDurationInit"],
                    var_set["epochDuration"],
                    var_set["operator"],
                    var_set["network"],
                ),
            )

    def get_all_modules(self):
        self.cursor.execute("SELECT vault, delegator FROM GlobalVars")
        rows = self.cursor.fetchall()
        return [
            {
                "vault": r[0],
                "delegator": r[1],
            }
            for r in rows
        ]

    def get_global_vars(self, address: str):
        self.cursor.execute(
            """
            SELECT
                vault,
                delegator,
                delegator_type,
                collateral,
                epochDurationInit,
                epochDuration,
                operator,
                network
            FROM GlobalVars
            WHERE vault=%s OR delegator=%s
            """,
            (address, address),
        )
        row = self.cursor.fetchone()
        if row:
            return {
                "vault": row[0],
                "delegator": row[1],
                "delegator_type": row[2],
                "collateral": row[3],
                "epochDurationInit": row[4],
                "epochDuration": row[5],
                "operator": row[6],
                "network": row[7],
            }
        return None

    # -------------------------------------------------------------------------
    # Prices
    # -------------------------------------------------------------------------
    def save_price(self, price_data: dict):
        self.cursor.execute(
            """
            INSERT INTO Prices (collateral, block_number, price)
            VALUES (%s, %s, %s)
            ON CONFLICT (collateral, block_number)
            DO UPDATE SET
                price = EXCLUDED.price
            """,
            (
                price_data["collateral"],
                price_data["block_number"],
                int_to_numeric(price_data["price"]),
            ),
        )

    def get_price(self, collateral: str, block_number: int):
        """
        Return the *latest* price at or before block_number
        """
        self.cursor.execute(
            """
            SELECT price
            FROM Prices
            WHERE collateral=%s AND block_number <= %s
            ORDER BY block_number DESC
            LIMIT 1
            """,
            (collateral, block_number),
        )
        row = self.cursor.fetchone()
        return numeric_to_int(row[0]) if row else None

    def get_prices(self, block_number: int):
        """
        For each collateral, get the last known price at or before block_number.
        """
        self.cursor.execute(
            """
            WITH latest AS (
                SELECT collateral, MAX(block_number) AS max_block
                FROM Prices
                WHERE block_number <= %s
                GROUP BY collateral
            )
            SELECT p.collateral, p.price
            FROM Prices p
            INNER JOIN latest l
            ON p.collateral = l.collateral
            AND p.block_number = l.max_block
            """,
            (block_number,),
        )
        rows = self.cursor.fetchall()
        return [
            {
                "collateral": r[0],
                "price": numeric_to_int(r[1]),
            }
            for r in rows
        ]

    # -------------------------------------------------------------------------
    # OperatorNetworkOptInService logs
    # -------------------------------------------------------------------------
    def save_operator_network_opt_in_service_logs(self, logs: list):
        for log in logs:
            block_num = log["blockNumber"]
            log_index = log["logIndex"]
            who = log["args"]["who"]
            where_ = log["args"]["where"]

            if log["event"] == "OptIn":
                self.cursor.execute(
                    """
                    INSERT INTO OperatorNetworkOptInServiceOptInLogs (block_number, log_index, who, where_)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (block_number, log_index)
                    DO NOTHING
                    """,
                    (block_num, log_index, who, where_),
                )
            elif log["event"] == "OptOut":
                self.cursor.execute(
                    """
                    INSERT INTO OperatorNetworkOptInServiceOptOutLogs (block_number, log_index, who, where_)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (block_number, log_index)
                    DO NOTHING
                    """,
                    (block_num, log_index, who, where_),
                )

    def get_operator_network_opt_in_service_logs(self, from_block: int, to_block: int):
        self.cursor.execute(
            """
            SELECT block_number, log_index, who, where_, 'OptIn' AS event
            FROM OperatorNetworkOptInServiceOptInLogs
            WHERE block_number BETWEEN %s AND %s

            UNION ALL

            SELECT block_number, log_index, who, where_, 'OptOut' AS event
            FROM OperatorNetworkOptInServiceOptOutLogs
            WHERE block_number BETWEEN %s AND %s
            """,
            (from_block, to_block, from_block, to_block),
        )
        return [
            {
                "event": r[4],
                "blockNumber": r[0],
                "logIndex": r[1],
                "args": {
                    "who": r[2],
                    "where": r[3],
                },
            }
            for r in self.cursor.fetchall()
        ]

    # -------------------------------------------------------------------------
    # OperatorVaultOptInService logs
    # -------------------------------------------------------------------------
    def save_operator_vault_opt_in_service_logs(self, logs: list):
        for log in logs:
            block_num = log["blockNumber"]
            log_index = log["logIndex"]
            who = log["args"]["who"]
            where_ = log["args"]["where"]

            if log["event"] == "OptIn":
                self.cursor.execute(
                    """
                    INSERT INTO OperatorVaultOptInServiceOptInLogs (block_number, log_index, who, where_)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (block_number, log_index)
                    DO NOTHING
                    """,
                    (block_num, log_index, who, where_),
                )
            elif log["event"] == "OptOut":
                self.cursor.execute(
                    """
                    INSERT INTO OperatorVaultOptInServiceOptOutLogs (block_number, log_index, who, where_)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (block_number, log_index)
                    DO NOTHING
                    """,
                    (block_num, log_index, who, where_),
                )

    def get_operator_vault_opt_in_service_logs(self, from_block: int, to_block: int):
        self.cursor.execute(
            """
            SELECT block_number, log_index, who, where_, 'OptIn' AS event
            FROM OperatorVaultOptInServiceOptInLogs
            WHERE block_number BETWEEN %s AND %s

            UNION ALL

            SELECT block_number, log_index, who, where_, 'OptOut' AS event
            FROM OperatorVaultOptInServiceOptOutLogs
            WHERE block_number BETWEEN %s AND %s
            """,
            (from_block, to_block, from_block, to_block),
        )
        return [
            {
                "event": r[4],
                "blockNumber": r[0],
                "logIndex": r[1],
                "args": {
                    "who": r[2],
                    "where": r[3],
                },
            }
            for r in self.cursor.fetchall()
        ]

    # -------------------------------------------------------------------------
    # Vault Logs
    # -------------------------------------------------------------------------
    def save_vault_logs(self, logs: list):
        for log in logs:
            block_num = log["blockNumber"]
            log_index = log["logIndex"]
            vault = log["address"]

            if log["event"] == "Deposit":
                self.cursor.execute(
                    """
                    INSERT INTO VaultDepositLogs (
                        block_number,
                        log_index,
                        vault,
                        depositor,
                        onBehalfOf,
                        amount,
                        shares
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (block_number, log_index, vault)
                    DO NOTHING
                    """,
                    (
                        block_num,
                        log_index,
                        vault,
                        log["args"]["depositor"],
                        log["args"]["onBehalfOf"],
                        int_to_numeric(log["args"]["amount"]),
                        int_to_numeric(log["args"]["shares"]),
                    ),
                )

            elif log["event"] == "Withdraw":
                self.cursor.execute(
                    """
                    INSERT INTO VaultWithdrawLogs (
                        block_number,
                        log_index,
                        vault,
                        withdrawer,
                        claimer,
                        amount,
                        burnedShares,
                        mintedShares
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (block_number, log_index, vault)
                    DO NOTHING
                    """,
                    (
                        block_num,
                        log_index,
                        vault,
                        log["args"]["withdrawer"],
                        log["args"]["claimer"],
                        int_to_numeric(log["args"]["amount"]),
                        int_to_numeric(log["args"]["burnedShares"]),
                        int_to_numeric(log["args"]["mintedShares"]),
                    ),
                )

            elif log["event"] == "OnSlash":
                self.cursor.execute(
                    """
                    INSERT INTO VaultOnSlashLogs (
                        block_number,
                        log_index,
                        vault,
                        amount,
                        captureTimestamp,
                        slashedAmount
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (block_number, log_index, vault)
                    DO NOTHING
                    """,
                    (
                        block_num,
                        log_index,
                        vault,
                        int_to_numeric(log["args"]["amount"]),
                        log["args"]["captureTimestamp"],
                        int_to_numeric(log["args"]["slashedAmount"]),
                    ),
                )

            elif log["event"] == "Transfer":
                self.cursor.execute(
                    """
                    INSERT INTO VaultTransferLogs (
                        block_number,
                        log_index,
                        vault,
                        from_,
                        to_,
                        value
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (block_number, log_index, vault)
                    DO NOTHING
                    """,
                    (
                        block_num,
                        log_index,
                        vault,
                        log["args"]["from"],
                        log["args"]["to"],
                        int_to_numeric(log["args"]["value"]),
                    ),
                )

    def get_vault_logs(self, from_block: int, to_block: int):
        """
        Return the combined deposit, withdraw, onSlash, transfer logs between from_block and to_block.
        """
        # Deposit
        self.cursor.execute(
            """
            SELECT
                block_number,
                log_index,
                vault,
                depositor,
                onBehalfOf,
                amount,
                shares
            FROM VaultDepositLogs
            WHERE block_number BETWEEN %s AND %s
            """,
            (from_block, to_block),
        )
        deposit_rows = self.cursor.fetchall()
        deposit_logs = [
            {
                "address": row[2],
                "event": "Deposit",
                "blockNumber": row[0],
                "logIndex": row[1],
                "args": {
                    "depositor": row[3],
                    "onBehalfOf": row[4],
                    "amount": numeric_to_int(row[5]),
                    "shares": numeric_to_int(row[6]),
                },
            }
            for row in deposit_rows
        ]

        # Withdraw
        self.cursor.execute(
            """
            SELECT
                block_number,
                log_index,
                vault,
                withdrawer,
                claimer,
                amount,
                burnedShares,
                mintedShares
            FROM VaultWithdrawLogs
            WHERE block_number BETWEEN %s AND %s
            """,
            (from_block, to_block),
        )
        withdraw_rows = self.cursor.fetchall()
        withdraw_logs = [
            {
                "address": row[2],
                "event": "Withdraw",
                "blockNumber": row[0],
                "logIndex": row[1],
                "args": {
                    "withdrawer": row[3],
                    "claimer": row[4],
                    "amount": numeric_to_int(row[5]),
                    "burnedShares": numeric_to_int(row[6]),
                    "mintedShares": numeric_to_int(row[7]),
                },
            }
            for row in withdraw_rows
        ]

        # OnSlash
        self.cursor.execute(
            """
            SELECT
                block_number,
                log_index,
                vault,
                amount,
                captureTimestamp,
                slashedAmount
            FROM VaultOnSlashLogs
            WHERE block_number BETWEEN %s AND %s
            """,
            (from_block, to_block),
        )
        onslash_rows = self.cursor.fetchall()
        onslash_logs = [
            {
                "address": row[2],
                "event": "OnSlash",
                "blockNumber": row[0],
                "logIndex": row[1],
                "args": {
                    "vault": row[2],
                    "amount": numeric_to_int(row[3]),
                    "captureTimestamp": row[4],
                    "slashedAmount": numeric_to_int(row[5]),
                },
            }
            for row in onslash_rows
        ]

        # Transfer
        self.cursor.execute(
            """
            SELECT
                block_number,
                log_index,
                vault,
                from_,
                to_,
                value
            FROM VaultTransferLogs
            WHERE block_number BETWEEN %s AND %s
            """,
            (from_block, to_block),
        )
        transfer_rows = self.cursor.fetchall()
        transfer_logs = [
            {
                "address": row[2],
                "event": "Transfer",
                "blockNumber": row[0],
                "logIndex": row[1],
                "args": {
                    "vault": row[2],
                    "from": row[3],
                    "to": row[4],
                    "value": numeric_to_int(row[5]),
                },
            }
            for row in transfer_rows
        ]

        return deposit_logs + withdraw_logs + onslash_logs + transfer_logs

    # -------------------------------------------------------------------------
    # Delegator logs
    # -------------------------------------------------------------------------
    def save_delegator_logs(self, logs: list):
        """
        Similar upsert logic for delegator logs (SetMaxNetworkLimit, etc.).
        We replace the subnetwork-identifier logic with numeric columns in Postgres.
        """
        for log in logs:
            block_num = log["blockNumber"]
            log_index = log["logIndex"]
            delegator = log["address"]
            subnetwork_hex = log["args"]["subnetwork"].hex()
            network_str = Helpers.get_network(subnetwork_hex)
            identifier_int = Helpers.get_identifier(subnetwork_hex)
            amount_int = log["args"].get("amount", 0)

            if log["event"] == "SetMaxNetworkLimit":
                self.cursor.execute(
                    """
                    INSERT INTO DelegatorSetMaxNetworkLimitLogs (
                        block_number,
                        log_index,
                        delegator,
                        network,
                        identifier,
                        amount
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (block_number, log_index, delegator)
                    DO NOTHING
                    """,
                    (
                        block_num,
                        log_index,
                        delegator,
                        network_str,
                        int_to_numeric(identifier_int),
                        int_to_numeric(amount_int),
                    ),
                )

            elif log["event"] == "SetNetworkLimit":
                self.cursor.execute(
                    """
                    INSERT INTO DelegatorSetNetworkLimitLogs (
                        block_number,
                        log_index,
                        delegator,
                        network,
                        identifier,
                        amount
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (block_number, log_index, delegator)
                    DO NOTHING
                    """,
                    (
                        block_num,
                        log_index,
                        delegator,
                        network_str,
                        int_to_numeric(identifier_int),
                        int_to_numeric(amount_int),
                    ),
                )

            elif log["event"] == "SetOperatorNetworkShares":
                operator = log["args"]["operator"]
                shares_int = log["args"].get("shares", 0)
                self.cursor.execute(
                    """
                    INSERT INTO DelegatorSetOperatorNetworkSharesLogs (
                        block_number,
                        log_index,
                        delegator,
                        network,
                        identifier,
                        operator,
                        shares
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (block_number, log_index, delegator)
                    DO NOTHING
                    """,
                    (
                        block_num,
                        log_index,
                        delegator,
                        network_str,
                        int_to_numeric(identifier_int),
                        operator,
                        int_to_numeric(shares_int),
                    ),
                )

            elif log["event"] == "SetOperatorNetworkLimit":
                operator = log["args"]["operator"]
                amount_int = log["args"].get("amount", 0)
                self.cursor.execute(
                    """
                    INSERT INTO DelegatorSetOperatorNetworkLimitLogs (
                        block_number,
                        log_index,
                        delegator,
                        network,
                        identifier,
                        operator,
                        amount
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (block_number, log_index, delegator)
                    DO NOTHING
                    """,
                    (
                        block_num,
                        log_index,
                        delegator,
                        network_str,
                        int_to_numeric(identifier_int),
                        operator,
                        int_to_numeric(amount_int),
                    ),
                )

    def get_delegator_logs(self, from_block: int, to_block: int):
        """
        Return all four delegator logs between from_block and to_block.
        """
        # 1. SetMaxNetworkLimit
        self.cursor.execute(
            """
            SELECT
                block_number,
                log_index,
                delegator,
                network,
                identifier,
                amount
            FROM DelegatorSetMaxNetworkLimitLogs
            WHERE block_number BETWEEN %s AND %s
            """,
            (from_block, to_block),
        )
        max_net_rows = self.cursor.fetchall()
        set_max_network_limit_logs = [
            {
                "address": r[2],
                "event": "SetMaxNetworkLimit",
                "blockNumber": r[0],
                "logIndex": r[1],
                "args": {
                    "network": r[3],
                    "identifier": numeric_to_int(r[4]),
                    "amount": numeric_to_int(r[5]),
                },
            }
            for r in max_net_rows
        ]

        # 2. SetNetworkLimit
        self.cursor.execute(
            """
            SELECT
                block_number,
                log_index,
                delegator,
                network,
                identifier,
                amount
            FROM DelegatorSetNetworkLimitLogs
            WHERE block_number BETWEEN %s AND %s
            """,
            (from_block, to_block),
        )
        set_net_rows = self.cursor.fetchall()
        set_network_limit_logs = [
            {
                "address": r[2],
                "event": "SetNetworkLimit",
                "blockNumber": r[0],
                "logIndex": r[1],
                "args": {
                    "network": r[3],
                    "identifier": numeric_to_int(r[4]),
                    "amount": numeric_to_int(r[5]),
                },
            }
            for r in set_net_rows
        ]

        # 3. SetOperatorNetworkShares
        self.cursor.execute(
            """
            SELECT
                block_number,
                log_index,
                delegator,
                network,
                identifier,
                operator,
                shares
            FROM DelegatorSetOperatorNetworkSharesLogs
            WHERE block_number BETWEEN %s AND %s
            """,
            (from_block, to_block),
        )
        shares_rows = self.cursor.fetchall()
        set_operator_network_shares_logs = [
            {
                "address": r[2],
                "event": "SetOperatorNetworkShares",
                "blockNumber": r[0],
                "logIndex": r[1],
                "args": {
                    "network": r[3],
                    "identifier": numeric_to_int(r[4]),
                    "operator": r[5],
                    "shares": numeric_to_int(r[6]),
                },
            }
            for r in shares_rows
        ]

        # 4. SetOperatorNetworkLimit
        self.cursor.execute(
            """
            SELECT
                block_number,
                log_index,
                delegator,
                network,
                identifier,
                amount,
                operator
            FROM DelegatorSetOperatorNetworkLimitLogs
            WHERE block_number BETWEEN %s AND %s
            """,
            (from_block, to_block),
        )
        limit_rows = self.cursor.fetchall()
        set_operator_network_limit_logs = [
            {
                "address": r[2],
                "event": "SetOperatorNetworkLimit",
                "blockNumber": r[0],
                "logIndex": r[1],
                "args": {
                    "network": r[3],
                    "identifier": numeric_to_int(r[4]),
                    "operator": r[6],
                    "amount": numeric_to_int(r[5]),
                },
            }
            for r in limit_rows
        ]

        return (
            set_max_network_limit_logs
            + set_network_limit_logs
            + set_operator_network_shares_logs
            + set_operator_network_limit_logs
        )

    # -------------------------------------------------------------------------
    # OperatorNetworkOptInServiceState
    # -------------------------------------------------------------------------
    def save_operator_network_opt_in_service_state(self, state: dict):
        """
        Upsert operator, network, status (as boolean).
        """
        self.cursor.execute(
            """
            INSERT INTO OperatorNetworkOptInServiceState (
                operator,
                network,
                status
            )
            VALUES (%s, %s, %s)
            ON CONFLICT (operator, network)
            DO UPDATE SET
                status = EXCLUDED.status
            """,
            (
                state["operator"],
                state["network"],
                True if state["status"] else False,
            ),
        )

    # -------------------------------------------------------------------------
    # OperatorVaultOptInServiceState
    # -------------------------------------------------------------------------
    def save_operator_vault_opt_in_service_state(self, state: dict):
        self.cursor.execute(
            """
            INSERT INTO OperatorVaultOptInServiceState (
                operator,
                vault,
                status
            )
            VALUES (%s, %s, %s)
            ON CONFLICT (operator, vault)
            DO UPDATE SET
                status = EXCLUDED.status
            """,
            (
                state["operator"],
                state["vault"],
                True if state["status"] else False,
            ),
        )

    # -------------------------------------------------------------------------
    # VaultGlobalState
    # -------------------------------------------------------------------------
    def save_vault_global_state(self, state: dict):
        self.cursor.execute(
            """
            INSERT INTO VaultGlobalState (
                vault,
                activeShares,
                activeStake
            )
            VALUES (%s, %s, %s)
            ON CONFLICT (vault)
            DO UPDATE SET
                activeShares = EXCLUDED.activeShares,
                activeStake = EXCLUDED.activeStake
            """,
            (
                state["vault"],
                int_to_numeric(state["activeShares"]),
                int_to_numeric(state["activeStake"]),
            ),
        )

    def get_vault_global_state(self, vault_address: str):
        self.cursor.execute(
            """
            SELECT activeShares, activeStake
            FROM VaultGlobalState
            WHERE vault=%s
            """,
            (vault_address,),
        )
        row = self.cursor.fetchone()
        if row:
            return {
                "activeShares": numeric_to_int(row[0]),
                "activeStake": numeric_to_int(row[1]),
            }
        else:
            return {"activeShares": 0, "activeStake": 0}

    # -------------------------------------------------------------------------
    # VaultUserState
    # -------------------------------------------------------------------------
    def save_vault_user_state(self, state: dict):
        self.cursor.execute(
            """
            INSERT INTO VaultUserState (vault, staker, activeSharesOf)
            VALUES (%s, %s, %s)
            ON CONFLICT (vault, staker)
            DO UPDATE SET
                activeSharesOf = EXCLUDED.activeSharesOf
            """,
            (
                state["vault"],
                state["user"],
                int_to_numeric(state["activeSharesOf"]),
            ),
        )

    def get_vault_user_state(self, vault_address: str, user_address: str):
        self.cursor.execute(
            """
            SELECT activeSharesOf
            FROM VaultUserState
            WHERE vault=%s AND staker=%s
            """,
            (vault_address, user_address),
        )
        row = self.cursor.fetchone()
        if row:
            return {"activeSharesOf": numeric_to_int(row[0])}
        else:
            return {"activeSharesOf": 0}

    # -------------------------------------------------------------------------
    # VaultGlobalWithdrawalsState
    # -------------------------------------------------------------------------
    def save_vault_global_withdrawals_state(self, state: dict):
        self.cursor.execute(
            """
            INSERT INTO VaultGlobalWithdrawalsState (
                vault,
                epoch,
                withdrawalShares,
                withdrawals
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (vault, epoch)
            DO UPDATE SET
                withdrawalShares = EXCLUDED.withdrawalShares,
                withdrawals = EXCLUDED.withdrawals
            """,
            (
                state["vault"],
                state["epoch"],
                int_to_numeric(state["withdrawalShares"]),
                int_to_numeric(state["withdrawals"]),
            ),
        )

    def get_vault_global_withdrawals_state(self, vault_address: str, epoch: int):
        self.cursor.execute(
            """
            SELECT withdrawalShares, withdrawals
            FROM VaultGlobalWithdrawalsState
            WHERE vault=%s AND epoch=%s
            """,
            (vault_address, epoch),
        )
        row = self.cursor.fetchone()
        if row:
            return {
                "withdrawalShares": numeric_to_int(row[0]),
                "withdrawals": numeric_to_int(row[1]),
            }
        else:
            return {"withdrawalShares": 0, "withdrawals": 0}

    # -------------------------------------------------------------------------
    # VaultUserWithdrawalsState
    # -------------------------------------------------------------------------
    def save_vault_user_withdrawals_state(self, state: dict):
        self.cursor.execute(
            """
            INSERT INTO VaultUserWithdrawalsState (
                vault,
                epoch,
                staker,
                withdrawalSharesOf
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (vault, epoch, staker)
            DO UPDATE SET
                withdrawalSharesOf = EXCLUDED.withdrawalSharesOf
            """,
            (
                state["vault"],
                state["epoch"],
                state["user"],
                int_to_numeric(state["withdrawalSharesOf"]),
            ),
        )

    def get_vault_user_withdrawals_state(
        self, vault_address: str, epoch: int, user_address: str
    ):
        self.cursor.execute(
            """
            SELECT withdrawalSharesOf
            FROM VaultUserWithdrawalsState
            WHERE vault=%s AND epoch=%s AND staker=%s
            """,
            (vault_address, epoch, user_address),
        )
        row = self.cursor.fetchone()
        if row:
            return {"withdrawalSharesOf": numeric_to_int(row[0])}
        else:
            return {"withdrawalSharesOf": 0}

    # -------------------------------------------------------------------------
    # DelegatorNetworkState
    # -------------------------------------------------------------------------
    def save_delegator_network_state(self, state: dict):
        self.cursor.execute(
            """
            INSERT INTO DelegatorNetworkState (
                delegator,
                network,
                identifier,
                maxNetworkLimit
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (delegator, network, identifier)
            DO UPDATE SET
                maxNetworkLimit = EXCLUDED.maxNetworkLimit
            """,
            (
                state["delegator"],
                state["network"],
                int_to_numeric(state["identifier"]),
                int_to_numeric(state["maxNetworkLimit"]),
            ),
        )

    # -------------------------------------------------------------------------
    # Delegator0NetworkState
    # -------------------------------------------------------------------------
    def save_delegator0_network_state(self, state: dict):
        self.cursor.execute(
            """
            INSERT INTO Delegator0NetworkState (
                delegator,
                network,
                identifier,
                networkLimit,
                totalOperatorNetworkShares
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (delegator, network, identifier)
            DO UPDATE SET
                networkLimit = EXCLUDED.networkLimit,
                totalOperatorNetworkShares = EXCLUDED.totalOperatorNetworkShares
            """,
            (
                state["delegator"],
                state["network"],
                int_to_numeric(state["identifier"]),
                int_to_numeric(state["networkLimit"]),
                int_to_numeric(state["totalOperatorNetworkShares"]),
            ),
        )

    def get_delegator0_network_state(
        self, delegator_address: str, network: str, identifier: int
    ):
        self.cursor.execute(
            """
            SELECT networkLimit, totalOperatorNetworkShares
            FROM Delegator0NetworkState
            WHERE delegator=%s AND network=%s AND identifier=%s
            """,
            (delegator_address, network, int_to_numeric(identifier)),
        )
        row = self.cursor.fetchone()
        if row:
            return {
                "networkLimit": numeric_to_int(row[0]),
                "totalOperatorNetworkShares": numeric_to_int(row[1]),
            }
        else:
            return {"networkLimit": 0, "totalOperatorNetworkShares": 0}

    def save_delegator0_operator_network_state(self, state: dict):
        self.cursor.execute(
            """
            INSERT INTO Delegator0OperatorNetworkState (
                delegator,
                network,
                identifier,
                operator,
                operatorNetworkShares
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (delegator, network, identifier, operator)
            DO UPDATE SET
                operatorNetworkShares = EXCLUDED.operatorNetworkShares
            """,
            (
                state["delegator"],
                state["network"],
                int_to_numeric(state["identifier"]),
                state["operator"],
                int_to_numeric(state["operatorNetworkShares"]),
            ),
        )

    def get_delegator0_operator_network_state(
        self,
        delegator_address: str,
        network: str,
        identifier: int,
        operator_address: str,
    ):
        self.cursor.execute(
            """
            SELECT operatorNetworkShares
            FROM Delegator0OperatorNetworkState
            WHERE delegator=%s AND network=%s AND identifier=%s AND operator=%s
            """,
            (delegator_address, network, int_to_numeric(identifier), operator_address),
        )
        row = self.cursor.fetchone()
        if row:
            return {"operatorNetworkShares": numeric_to_int(row[0])}
        else:
            return {"operatorNetworkShares": 0}

    # -------------------------------------------------------------------------
    # Delegator1NetworkState
    # -------------------------------------------------------------------------
    def save_delegator1_network_state(self, state: dict):
        self.cursor.execute(
            """
            INSERT INTO Delegator1NetworkState (
                delegator,
                network,
                identifier,
                networkLimit
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (delegator, network, identifier)
            DO UPDATE SET
                networkLimit = EXCLUDED.networkLimit
            """,
            (
                state["delegator"],
                state["network"],
                int_to_numeric(state["identifier"]),
                int_to_numeric(state["networkLimit"]),
            ),
        )

    def get_delegator1_network_state(
        self, delegator_address: str, network: str, identifier: int
    ):
        self.cursor.execute(
            """
            SELECT networkLimit
            FROM Delegator1NetworkState
            WHERE delegator=%s AND network=%s AND identifier=%s
            """,
            (delegator_address, network, int_to_numeric(identifier)),
        )
        row = self.cursor.fetchone()
        if row:
            return {"networkLimit": numeric_to_int(row[0])}
        else:
            return {"networkLimit": 0}

    def save_delegator1_operator_network_state(self, state: dict):
        self.cursor.execute(
            """
            INSERT INTO Delegator1OperatorNetworkState (
                delegator,
                network,
                identifier,
                operator,
                operatorNetworkLimit
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (delegator, network, identifier, operator)
            DO UPDATE SET
                operatorNetworkLimit = EXCLUDED.operatorNetworkLimit
            """,
            (
                state["delegator"],
                state["network"],
                int_to_numeric(state["identifier"]),
                state["operator"],
                int_to_numeric(state["operatorNetworkLimit"]),
            ),
        )

    # -------------------------------------------------------------------------
    # Delegator2NetworkState
    # -------------------------------------------------------------------------
    def save_delegator2_network_state(self, state: dict):
        self.cursor.execute(
            """
            INSERT INTO Delegator2NetworkState (
                delegator,
                network,
                identifier,
                networkLimit
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (delegator, network, identifier)
            DO UPDATE SET
                networkLimit = EXCLUDED.networkLimit
            """,
            (
                state["delegator"],
                state["network"],
                int_to_numeric(state["identifier"]),
                int_to_numeric(state["networkLimit"]),
            ),
        )

    def get_delegator2_network_state(
        self, delegator_address: str, network: str, identifier: int
    ):
        self.cursor.execute(
            """
            SELECT networkLimit
            FROM Delegator2NetworkState
            WHERE delegator=%s AND network=%s AND identifier=%s
            """,
            (delegator_address, network, int_to_numeric(identifier)),
        )
        row = self.cursor.fetchone()
        if row:
            return {"networkLimit": numeric_to_int(row[0])}
        else:
            return {"networkLimit": 0}

    # -------------------------------------------------------------------------
    # get_stakes(...) and get_all_stakes(...)
    # -------------------------------------------------------------------------
    def get_stakes(self, network: str, identifier: int):
        self.cursor.execute(
            """
            WITH combined AS (
                SELECT 
                    gv.vault,
                    gv.delegator,
                    gv.delegator_type,
                    COALESCE(vgs.activeStake, 0) as activeStake,
                    COALESCE(dns.maxNetworkLimit, 0) as maxNetworkLimit,
                    COALESCE(d0ns.networkLimit, 0) AS networkLimit0,
                    COALESCE(d0ns.totalOperatorNetworkShares, 0) as totalOperatorNetworkShares,
                    COALESCE(d0ons.operatorNetworkShares, 0) as operatorNetworkShares,
                    COALESCE(d1ns.networkLimit, 0) AS networkLimit1,
                    COALESCE(d1ons.operatorNetworkLimit, 0) as operatorNetworkLimit,
                    COALESCE(d2ns.networkLimit, 0) AS networkLimit2,
                    COALESCE(d0ons.operator, d1ons.operator, gv.operator) AS operator,
                    COALESCE(onos.status, FALSE) AS isOptedInNetwork,
                    COALESCE(ovos.status, FALSE) AS isOptedInVault,
                    c.collateral AS collateral_address,
                    CASE
                        WHEN gv.delegator_type NOT IN (2,3) THEN 1
                        WHEN COALESCE(d0ons.operator, d1ons.operator, gv.operator) = gv.operator THEN 1
                        ELSE 0
                    END AS isValidOperator,
                    CASE
                        WHEN gv.delegator_type NOT IN (3) THEN 1
                        WHEN %s = gv.network THEN 1
                        ELSE 0
                    END AS isValidNetwork
                FROM GlobalVars gv
                JOIN VaultGlobalState vgs ON vgs.vault = gv.vault
                JOIN Collaterals c ON c.collateral = gv.collateral

                LEFT JOIN DelegatorNetworkState dns
                    ON dns.delegator = gv.delegator
                    AND dns.network = %s
                    AND dns.identifier = %s

                LEFT JOIN Delegator0NetworkState d0ns
                    ON d0ns.delegator = gv.delegator
                    AND d0ns.network = %s
                    AND d0ns.identifier = %s
                LEFT JOIN Delegator0OperatorNetworkState d0ons
                    ON d0ons.delegator = gv.delegator
                    AND d0ons.network = %s
                    AND d0ons.identifier = %s

                LEFT JOIN Delegator1NetworkState d1ns
                    ON d1ns.delegator = gv.delegator
                    AND d1ns.network = %s
                    AND d1ns.identifier = %s
                LEFT JOIN Delegator1OperatorNetworkState d1ons
                    ON d1ons.delegator = gv.delegator
                    AND d1ons.network = %s
                    AND d1ons.identifier = %s

                LEFT JOIN Delegator2NetworkState d2ns
                    ON d2ns.delegator = gv.delegator
                    AND d2ns.network = %s
                    AND d2ns.identifier = %s

                LEFT JOIN OperatorNetworkOptInServiceState onos
                    ON onos.operator = COALESCE(d0ons.operator, d1ons.operator, gv.operator)
                    AND onos.network = %s

                LEFT JOIN OperatorVaultOptInServiceState ovos
                    ON ovos.operator = COALESCE(d0ons.operator, d1ons.operator, gv.operator)
                    AND ovos.vault = gv.vault
            ),
            calculated AS (
                SELECT 
                    vault,
                    operator,
                    stake(
                        delegator_type,
                        COALESCE(isOptedInNetwork::int, 0),
                        COALESCE(isOptedInVault::int, 0),
                        activeStake,
                        maxNetworkLimit,
                        CASE delegator_type
                            WHEN 0 THEN networkLimit0
                            WHEN 1 THEN networkLimit1
                            WHEN 2 THEN networkLimit2
                            ELSE 0
                        END,
                        COALESCE(operatorNetworkShares, 0),
                        COALESCE(totalOperatorNetworkShares, 0),
                        COALESCE(operatorNetworkLimit, 0),
                        isValidOperator,
                        isValidNetwork
                    ) AS computed_stake,
                    collateral_address
                FROM combined
                WHERE operator IS NOT NULL
            )
            SELECT * FROM calculated WHERE computed_stake != 0
            """,
            (
                network,
                network,
                int_to_numeric(identifier),
                network,
                int_to_numeric(identifier),
                network,
                int_to_numeric(identifier),
                network,
                int_to_numeric(identifier),
                network,
                int_to_numeric(identifier),
                network,
                int_to_numeric(identifier),
                network,
            ),
        )
        return [
            {
                "network": network,
                "identifier": identifier,
                "vault": r[0],
                "operator": r[1],
                "stake": numeric_to_int(r[2]),
                "collateral": r[3],
            }
            for r in self.cursor.fetchall()
        ]

    def get_all_stakes(self):
        self.cursor.execute(
            """
            WITH combined AS (
                SELECT
                    gv.vault,
                    gv.delegator,
                    gv.delegator_type,
                    vgs.activeStake,
                    dns.maxNetworkLimit AS maxNetworkLimit,
                    d0ns.networkLimit AS networkLimit0,
                    d0ns.totalOperatorNetworkShares,
                    d0ons.operatorNetworkShares,
                    d1ns.networkLimit AS networkLimit1,
                    d1ons.operatorNetworkLimit,
                    d2ns.networkLimit AS networkLimit2,
                    COALESCE(d0ns.network, d1ns.network, d2ns.network, gv.network) AS network,
                    COALESCE(d0ns.identifier, d1ns.identifier, d2ns.identifier) AS identifier,
                    COALESCE(d0ons.operator, d1ons.operator, gv.operator) AS operator,
                    onos.status AS isOptedInNetwork,
                    ovos.status AS isOptedInVault,
                    CASE
                        WHEN gv.delegator_type NOT IN (2,3) THEN 1
                        WHEN COALESCE(d0ons.operator, d1ons.operator, gv.operator) = gv.operator THEN 1
                        ELSE 0
                    END AS isValidOperator,
                    CASE
                        WHEN gv.delegator_type NOT IN (3) THEN 1
                        WHEN (COALESCE(d0ns.network, d1ns.network, d2ns.network, gv.network)) = gv.network THEN 1
                        ELSE 0
                    END AS isValidNetwork

                FROM GlobalVars gv
                JOIN VaultGlobalState vgs
                    ON vgs.vault = gv.vault

                LEFT JOIN Delegator0NetworkState d0ns
                    ON d0ns.delegator = gv.delegator
                LEFT JOIN Delegator0OperatorNetworkState d0ons
                    ON d0ons.delegator = gv.delegator
                    AND d0ons.network = d0ns.network
                    AND d0ons.identifier = d0ns.identifier

                LEFT JOIN Delegator1NetworkState d1ns
                    ON d1ns.delegator = gv.delegator
                LEFT JOIN Delegator1OperatorNetworkState d1ons
                    ON d1ons.delegator = gv.delegator
                    AND d1ons.network = d1ns.network
                    AND d1ons.identifier = d1ns.identifier

                LEFT JOIN Delegator2NetworkState d2ns
                    ON d2ns.delegator = gv.delegator

                LEFT JOIN DelegatorNetworkState dns
                    ON dns.delegator = gv.delegator
                    AND dns.network = COALESCE(d0ns.network, d1ns.network, d2ns.network, gv.network)
                    AND dns.identifier = COALESCE(d0ns.identifier, d1ns.identifier, d2ns.identifier)

                LEFT JOIN OperatorNetworkOptInServiceState onos
                    ON onos.operator = COALESCE(d0ons.operator, d1ons.operator, gv.operator)
                    AND onos.network = COALESCE(d0ns.network, d1ns.network, d2ns.network, gv.network)

                LEFT JOIN OperatorVaultOptInServiceState ovos
                    ON ovos.operator = COALESCE(d0ons.operator, d1ons.operator, gv.operator)
                    AND ovos.vault = gv.vault
            )
            SELECT
                vault,
                operator,
                network,
                identifier,
                stake(
                    delegator_type,
                    COALESCE(isOptedInNetwork::int, 0),
                    COALESCE(isOptedInVault::int, 0),
                    activeStake,
                    maxNetworkLimit,
                    CASE delegator_type
                        WHEN 0 THEN networkLimit0
                        WHEN 1 THEN networkLimit1
                        WHEN 2 THEN networkLimit2
                        ELSE 0
                    END,
                    COALESCE(operatorNetworkShares, 0),
                    COALESCE(totalOperatorNetworkShares, 0),
                    COALESCE(operatorNetworkLimit, 0),
                    isValidOperator,
                    isValidNetwork
                ) AS computed_stake
            FROM combined
            WHERE operator IS NOT NULL
                AND network IS NOT NULL
                AND identifier IS NOT NULL
            """
        )
        return [
            {
                "vault": row[0],
                "operator": row[1],
                "network": row[2],
                "identifier": numeric_to_int(row[3]),
                "stake": numeric_to_int(row[4]),
            }
            for row in self.cursor.fetchall()
        ]

    # -------------------------------------------------------------------------
    # get_active_balances_of(...)
    # -------------------------------------------------------------------------
    def get_active_balances_of(self, vault_address: str):
        self.cursor.execute(
            """
            SELECT staker, active_balance_of_
            FROM (
                SELECT
                    vus.staker,
                    active_balance_of(
                        vus.activeSharesOf,
                        vgs.activeStake,
                        vgs.activeShares
                    ) as active_balance_of_
                FROM VaultUserState vus
                JOIN VaultGlobalState vgs ON vgs.vault = vus.vault
                WHERE vus.vault = %s
            ) subquery
            WHERE active_balance_of_ != 0
            """,
            (vault_address,),
        )
        return [
            {
                "user": row[0],
                "active_balance_of": numeric_to_int(row[1]),
            }
            for row in self.cursor.fetchall()
        ]

    # -------------------------------------------------------------------------
    # NetworkVaultPoints, NetworkOperatorVaultPoints, NetworkVaultUserPoints
    # and their Historical versions
    # -------------------------------------------------------------------------
    def save_network_vault_points(self, points_data: dict):
        self.cursor.execute(
            """
            INSERT INTO NetworkVaultPoints (network, identifier, vault, points)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (network, identifier, vault)
            DO UPDATE SET
                points = EXCLUDED.points
            """,
            (
                points_data["network"],
                int_to_numeric(points_data["identifier"]),
                points_data["vault"],
                int_to_numeric(points_data["points"]),
            ),
        )

    def save_network_vault_points_batch(self, points_data_list: list):
        execute_values(
            self.cursor,
            """
            INSERT INTO NetworkVaultPoints (network, identifier, vault, points)
            VALUES %s
            ON CONFLICT (network, identifier, vault)
            DO UPDATE SET
                points = NetworkVaultPoints.points + EXCLUDED.points
            """,
            [
                (
                    points_data["network"],
                    int_to_numeric(points_data["identifier"]),
                    points_data["vault"],
                    int_to_numeric(points_data["points"]),
                )
                for points_data in points_data_list
            ],
        )

    def get_network_vault_points(self, network: str, identifier: int, vault: str):
        self.cursor.execute(
            """
            SELECT points
            FROM NetworkVaultPoints
            WHERE network=%s AND identifier=%s AND vault=%s
            """,
            (network, int_to_numeric(identifier), vault),
        )
        row = self.cursor.fetchone()
        return numeric_to_int(row[0]) if row else 0

    def save_network_operator_vault_points(self, points_data: dict):
        self.cursor.execute(
            """
            INSERT INTO NetworkOperatorVaultPoints (
                network,
                identifier,
                operator,
                vault,
                points
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (network, identifier, operator, vault)
            DO UPDATE SET
                points = EXCLUDED.points
            """,
            (
                points_data["network"],
                int_to_numeric(points_data["identifier"]),
                points_data["operator"],
                points_data["vault"],
                int_to_numeric(points_data["points"]),
            ),
        )

    def save_network_operator_vault_points_batch(self, points_data_list: list):
        execute_values(
            self.cursor,
            """
            INSERT INTO NetworkOperatorVaultPoints (
                network,
                identifier,
                operator,
                vault,
                points
            )
            VALUES %s
            ON CONFLICT (network, identifier, operator, vault)
            DO UPDATE SET
                points = NetworkOperatorVaultPoints.points + EXCLUDED.points
            """,
            [
                (
                    points_data["network"],
                    int_to_numeric(points_data["identifier"]),
                    points_data["operator"],
                    points_data["vault"],
                    int_to_numeric(points_data["points"]),
                )
                for points_data in points_data_list
            ],
        )

    def get_network_operator_vault_points(
        self, network: str, identifier: int, operator: str, vault: str
    ):
        self.cursor.execute(
            """
            SELECT points
            FROM NetworkOperatorVaultPoints
            WHERE network=%s AND identifier=%s AND operator=%s AND vault=%s
            """,
            (network, int_to_numeric(identifier), operator, vault),
        )
        row = self.cursor.fetchone()
        return numeric_to_int(row[0]) if row else 0

    def save_network_vault_user_points(self, points_data: dict):
        self.cursor.execute(
            """
            INSERT INTO NetworkVaultUserPoints (
                network,
                identifier,
                vault,
                staker,
                points
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (network, identifier, vault, staker)
            DO UPDATE SET
                points = EXCLUDED.points
            """,
            (
                points_data["network"],
                int_to_numeric(points_data["identifier"]),
                points_data["vault"],
                points_data["user"],
                int_to_numeric(points_data["points"]),
            ),
        )

    def save_network_vault_user_points_batch(self, points_data_list: list):
        execute_values(
            self.cursor,
            """
            INSERT INTO NetworkVaultUserPoints (
                network,
                identifier,
                vault,
                staker,
                points
            )
            VALUES %s
            ON CONFLICT (network, identifier, vault, staker)
            DO UPDATE SET
                points = NetworkVaultUserPoints.points + EXCLUDED.points
            """,
            [
                (
                    points_data["network"],
                    int_to_numeric(points_data["identifier"]),
                    points_data["vault"],
                    points_data["user"],
                    int_to_numeric(points_data["points"]),
                )
                for points_data in points_data_list
            ],
        )

    def get_network_vault_user_points(
        self, network: str, identifier: int, vault: str, staker: str
    ):
        self.cursor.execute(
            """
            SELECT points
            FROM NetworkVaultUserPoints
            WHERE network=%s AND identifier=%s AND vault=%s AND staker=%s
            """,
            (network, int_to_numeric(identifier), vault, staker),
        )
        row = self.cursor.fetchone()
        return numeric_to_int(row[0]) if row else 0

    # -------------------------------------------------------------------------
    # snapshot_points(...)
    # -------------------------------------------------------------------------
    def snapshot_points(self, block_number: int):
        """
        Copy current points data into historical tables at the given block_number.
        """
        # 1. NetworkVaultPoints -> NetworkVaultPointsHistorical
        self.cursor.execute(
            """
            INSERT INTO NetworkVaultPointsHistorical (
                block_number,
                network,
                identifier,
                vault,
                points
            )
            SELECT
                %s,
                network,
                identifier,
                vault,
                points
            FROM NetworkVaultPoints
            ON CONFLICT (block_number, network, identifier, vault)
            DO UPDATE SET
                points = EXCLUDED.points
            """,
            (block_number,),
        )

        # 2. NetworkOperatorVaultPoints -> NetworkOperatorVaultPointsHistorical
        self.cursor.execute(
            """
            INSERT INTO NetworkOperatorVaultPointsHistorical (
                block_number,
                network,
                identifier,
                operator,
                vault,
                points
            )
            SELECT
                %s,
                network,
                identifier,
                operator,
                vault,
                points
            FROM NetworkOperatorVaultPoints
            ON CONFLICT (block_number, network, identifier, operator, vault)
            DO UPDATE SET
                points = EXCLUDED.points
            """,
            (block_number,),
        )

        # 3. NetworkVaultUserPoints -> NetworkVaultUserPointsHistorical
        self.cursor.execute(
            """
            INSERT INTO NetworkVaultUserPointsHistorical (
                block_number,
                network,
                identifier,
                vault,
                staker,
                points
            )
            SELECT
                %s,
                network,
                identifier,
                vault,
                staker,
                points
            FROM NetworkVaultUserPoints
            ON CONFLICT (block_number, network, identifier, vault, staker)
            DO UPDATE SET
                points = EXCLUDED.points
            """,
            (block_number,),
        )

        self.commit()

    # -------------------------------------------------------------------------
    # Last / closest snapshot
    # -------------------------------------------------------------------------
    def get_last_snapshot_block_number(self):
        self.cursor.execute(
            "SELECT MAX(block_number) FROM NetworkVaultPointsHistorical"
        )
        row = self.cursor.fetchone()
        return row[0] if row else None

    def get_closest_points_snapshot_block_number(self, block_number: int):
        self.cursor.execute(
            """
            SELECT MAX(block_number)
            FROM NetworkVaultPointsHistorical
            WHERE block_number <= %s
            """,
            (block_number,),
        )
        row = self.cursor.fetchone()
        return row[0] if row else None

    # -------------------------------------------------------------------------
    # Historical queries: NetworkVaultPointsHistorical
    # -------------------------------------------------------------------------
    def get_network_vault_points_historical_per_network(
        self, block_number: int, network: str
    ):
        """
        Return SUM(points) per vault at a given block_number for a specific network.
        """
        # Use SUM(...) since we no longer have the custom aggregator BLOB_SUM
        self.cursor.execute(
            """
            SELECT vault, SUM(points)
            FROM NetworkVaultPointsHistorical
            WHERE block_number=%s AND network=%s
            GROUP BY vault
            """,
            (block_number, network),
        )
        rows = self.cursor.fetchall()
        return [
            {
                "vault": r[0],
                "points": numeric_to_int(r[1]),
            }
            for r in rows
        ]

    def get_network_vault_points_historical_all(
        self, block_number: int, offset=0, limit=1000000000
    ):
        """
        Return SUM(points) per (network, vault) for the given block_number.
        """
        self.cursor.execute(
            """
            SELECT network, vault, SUM(points)
            FROM NetworkVaultPointsHistorical
            WHERE block_number=%s
            GROUP BY network, vault
            ORDER BY network, vault
            LIMIT %s OFFSET %s
            """,
            (block_number, limit, offset),
        )
        rows = self.cursor.fetchall()
        return [
            {
                "network": r[0],
                "vault": r[1],
                "points": numeric_to_int(r[2]),
            }
            for r in rows
        ]

    def get_network_vault_points_historical_stats(self, block_number: int):
        self.cursor.execute(
            """
            SELECT SUM(points), COUNT(DISTINCT network)
            FROM NetworkVaultPointsHistorical
            WHERE block_number=%s
            """,
            (block_number,),
        )
        row = self.cursor.fetchone()
        if row and row[0] is not None:
            return {
                "total_points": numeric_to_int(row[0]),
                "unique_receivers": row[1],
            }
        return None

    # -------------------------------------------------------------------------
    # Historical queries: NetworkOperatorVaultPointsHistorical
    # -------------------------------------------------------------------------
    def get_network_operator_vault_points_historical_per_operator(
        self, block_number: int, operator: str
    ):
        self.cursor.execute(
            """
            SELECT network, vault, SUM(points)
            FROM NetworkOperatorVaultPointsHistorical
            WHERE block_number=%s AND operator=%s
            GROUP BY network, vault
            """,
            (block_number, operator),
        )
        rows = self.cursor.fetchall()
        return [
            {
                "network": r[0],
                "vault": r[1],
                "points": numeric_to_int(r[2]),
            }
            for r in rows
        ]

    def get_network_operator_vault_points_historical_all(
        self, block_number: int, offset=0, limit=1000000000
    ):
        self.cursor.execute(
            """
            SELECT network, vault, operator, SUM(points)
            FROM NetworkOperatorVaultPointsHistorical
            WHERE block_number=%s
            GROUP BY network, vault, operator
            ORDER BY network, vault, operator
            LIMIT %s OFFSET %s
            """,
            (block_number, limit, offset),
        )
        rows = self.cursor.fetchall()
        return [
            {
                "network": r[0],
                "vault": r[1],
                "operator": r[2],
                "points": numeric_to_int(r[3]),
            }
            for r in rows
        ]

    def get_network_operator_points_historical_stats(self, block_number: int):
        self.cursor.execute(
            """
            SELECT SUM(points), COUNT(DISTINCT operator)
            FROM NetworkOperatorVaultPointsHistorical
            WHERE block_number=%s
            """,
            (block_number,),
        )
        row = self.cursor.fetchone()
        if row and row[0] is not None:
            return {
                "total_points": numeric_to_int(row[0]),
                "unique_receivers": row[1],
            }
        return None

    # -------------------------------------------------------------------------
    # Historical queries: NetworkVaultUserPointsHistorical
    # -------------------------------------------------------------------------
    def get_network_vault_user_points_historical_per_user(
        self, block_number: int, staker: str
    ):
        self.cursor.execute(
            """
            SELECT network, vault, SUM(points)
            FROM NetworkVaultUserPointsHistorical
            WHERE block_number=%s AND staker=%s
            GROUP BY network, vault
            """,
            (block_number, staker),
        )
        rows = self.cursor.fetchall()
        return [
            {
                "network": r[0],
                "vault": r[1],
                "points": numeric_to_int(r[2]),
            }
            for r in rows
        ]

    def get_network_vault_user_points_historical_all(
        self, block_number: int, offset=0, limit=1000000000
    ):
        self.cursor.execute(
            """
            SELECT network, vault, staker, SUM(points)
            FROM NetworkVaultUserPointsHistorical
            WHERE block_number=%s
            GROUP BY network, vault, staker
            ORDER BY network, vault, staker
            LIMIT %s OFFSET %s
            """,
            (block_number, limit, offset),
        )
        rows = self.cursor.fetchall()
        return [
            {
                "network": r[0],
                "vault": r[1],
                "staker": r[2],
                "points": numeric_to_int(r[3]),
            }
            for r in rows
        ]

    def get_network_vault_user_points_historical_stats(self, block_number: int):
        self.cursor.execute(
            """
            SELECT SUM(points), COUNT(DISTINCT staker)
            FROM NetworkVaultUserPointsHistorical
            WHERE block_number=%s
            """,
            (block_number,),
        )
        row = self.cursor.fetchone()
        if row and row[0] is not None:
            return {
                "total_points": numeric_to_int(row[0]),
                "unique_receivers": row[1],
            }
        return None

    # -------------------------------------------------------------------------
    # get_points_historical_all(...)
    # -------------------------------------------------------------------------
    def get_points_historical_all(self, block_number: int, offset=0, limit=1000000000):
        """
        The original code combined data from 3 historical tables
        (NetworkVaultPointsHistorical, NetworkOperatorVaultPointsHistorical,
         NetworkVaultUserPointsHistorical) and unioned them. We do a similar approach,
        but we use SUM() instead of a custom aggregator.
        """
        # We'll create a sub-select to unify them. Then we group by the fields.
        self.cursor.execute(
            """
            WITH AllData AS (
                SELECT
                    network,
                    vault,
                    network AS receiver,
                    SUM(points) AS points,
                    'network' AS receiver_type
                FROM NetworkVaultPointsHistorical
                WHERE block_number=%s
                GROUP BY network, vault

                UNION ALL

                SELECT
                    network,
                    vault,
                    operator AS receiver,
                    SUM(points) AS points,
                    'operator' AS receiver_type
                FROM NetworkOperatorVaultPointsHistorical
                WHERE block_number=%s
                GROUP BY network, vault, operator

                UNION ALL

                SELECT
                    network,
                    vault,
                    staker AS receiver,
                    SUM(points) AS points,
                    'staker' AS receiver_type
                FROM NetworkVaultUserPointsHistorical
                WHERE block_number=%s
                GROUP BY network, vault, staker
            )
            SELECT
                network,
                vault,
                receiver,
                points,
                receiver_type
            FROM AllData
            ORDER BY network, vault, receiver, receiver_type
            LIMIT %s OFFSET %s
            """,
            (block_number, block_number, block_number, limit, offset),
        )
        return [
            {
                "network": r[0],
                "vault": r[1],
                "receiver": r[2],
                "points": numeric_to_int(r[3]),
                "receiver_type": r[4],
            }
            for r in self.cursor.fetchall()
        ]
