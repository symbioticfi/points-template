import requests
from datetime import datetime
from retry import retry

from common.config import Config
from common.storage import Storage
from common.web3wrapper import Web3Wrapper
from common.helpers import Helpers


class Prices:
    def __init__(self, config, w3_wrapper, storage):
        self.config = config
        self.w3_wrapper = w3_wrapper
        self.storage = storage
        self.name = self.config.get_prices_module_name()
        self.chunk_range = 3 * 24 * 60 * 60
        self.provider = "coinmarketcap"

    def get_start_timestamp(self):
        last_processed_timestamp = self.storage.get_processed_timepoint(self.name)
        if last_processed_timestamp is not None:
            return last_processed_timestamp + 1

        return (
            self.w3_wrapper.get_block_timestamp(
                self.w3_wrapper.get_creation_block(
                    self.w3_wrapper.addresses.vault_factory.address
                )
            )
            - 5 * 60
        )

    def get_end_timestamp(self):
        latest_block_processed_block = self.storage.get_processed_timepoint(
            self.config.get_blocks_module_name()
        )
        if latest_block_processed_block is None:
            print("No block processed timestamp found. Exiting parse_all_prices.")
            exit(0)

        return self.w3_wrapper.get_block_timestamp(latest_block_processed_block)

    def sort_quote_data_list(self, quote_data_list, provider):
        if provider == "coinmarketcap":
            return sorted(
                quote_data_list,
                key=lambda quote_data: quote_data["quote"]["USD"]["timestamp"],
            )
        elif provider == "alchemy":
            return sorted(
                quote_data_list, key=lambda quote_data: quote_data["timestamp"]
            )

    def process_price_data(self, quote_data, collateral, provider):
        print(f"Processing price data for collateral: {collateral}")

        if provider == "coinmarketcap":
            time = quote_data["quote"]["USD"]["timestamp"]
            timestamp = int(
                datetime.fromisoformat(time.replace("Z", "+00:00")).timestamp()
            )
            price_float = float(quote_data["quote"]["USD"]["price"])
            price = int(price_float * 10**24)
        elif provider == "alchemy":
            time = quote_data["timestamp"]
            timestamp = int(
                datetime.fromisoformat(time.replace("Z", "+00:00")).timestamp()
            )
            price_float = float(quote_data["value"])
            price = int(price_float * 10**24)

        block_number = self.storage.get_block_number_by_timestamp(timestamp)

        print(f"  Saving price to storage: block_number={block_number}, price={price}")

        self.storage.save_price(
            {
                "collateral": collateral,
                "block_number": block_number,
                "price": price,
            }
        )

        return timestamp

    def parse_prices(
        self, collaterals_data, time_start, time_end, provider="coinmarketcap"
    ):
        print(
            f"Starting parse_prices with time_start={time_start}, time_end={time_end}"
        )

        if provider == "coinmarketcap":
            params = {
                "id": ",".join(
                    [
                        str(collaterals_data[collateral]["cmcID"])
                        for collateral in collaterals_data
                    ]
                ),
                "count": 10000,
                "interval": "5m",
                "time_start": time_start,
                "time_end": time_end,
                "skip_invalid": "false",
            }
            headers = {
                "X-CMC_PRO_API_KEY": self.config.get_coinmarketcap_api_key(),
            }
            api_url = f"https://{self.config.get_coinmarketcap_api_url()}/v3/cryptocurrency/quotes/historical"

            response = requests.get(api_url, params=params, headers=headers)
            Helpers.raise_for_status_with_log(response)

            data = response.json()

            for cmcID in data["data"]:
                quote_data_list = self.sort_quote_data_list(
                    data["data"][cmcID]["quotes"], provider
                )
                last_processed_timestamp = None

                print(
                    f"  Processing data for cmcID={cmcID}, quotes count={len(quote_data_list)}"
                )

                for quote_data in quote_data_list:
                    for collateral in collaterals_data:
                        if collaterals_data[collateral]["cmcID"] == int(cmcID):
                            last_processed_timestamp = self.process_price_data(
                                quote_data,
                                collateral,
                                provider,
                            )

                if last_processed_timestamp is None:
                    for collateral in collaterals_data:
                        if collaterals_data[collateral]["cmcID"] == int(cmcID):
                            if (
                                self.config.get_price(collateral, time_start)
                                is not None
                            ):
                                print(
                                    f"  ERROR: Missing price data for cmcID={cmcID}, time range={time_start}-{time_end}"
                                )
                                raise Exception(
                                    f"Price data for {cmcID} is missing for the time range {time_start} - {time_end}"
                                )
                elif time_end - last_processed_timestamp >= 5 * 60 + 60:
                    print(
                        f"  ERROR: Incomplete price data for cmcID={cmcID}, last_processed_timestamp={last_processed_timestamp}"
                    )
                    raise Exception(
                        f"Price data for {cmcID} is missing for the time range {time_start} - {time_end} (last processed timestamp: {last_processed_timestamp})"
                    )

        elif provider == "alchemy":
            api_url = f"https://{self.config.get_alchemy_prices_api_url()}/v1/{self.config.get_alchemy_api_key()}/tokens/historical"
            for collateral in collaterals_data:
                json = {
                    "symbol": collaterals_data[collateral]["symbol"],
                    "startTime": time_start,
                    "endTime": time_end,
                    "interval": "5m",
                }

                print(f"    Collateral: {collateral}")

                response = requests.post(api_url, json=json)
                Helpers.raise_for_status_with_log(response)

                data = response.json()
                quote_data_list = self.sort_quote_data_list(data["data"], provider)
                last_processed_timestamp = None

                print(
                    f"    Received {len(quote_data_list)} quotes for collateral: {collateral}"
                )

                for quote_data in quote_data_list:
                    last_processed_timestamp = self.process_price_data(
                        quote_data, collateral, provider
                    )
                if last_processed_timestamp is None:
                    if self.config.get_price(collateral, time_start) is not None:

                        print(
                            f"    ERROR: Missing price data for collateral={collateral}, time range={time_start}-{time_end}"
                        )
                        raise Exception(
                            f"Price data for {collateral} is missing for the time range {time_start} - {time_end}"
                        )
                elif time_end - last_processed_timestamp >= 5 * 60 + 60:

                    print(
                        f"    ERROR: Incomplete price data for collateral={collateral}, last_processed_timestamp={last_processed_timestamp}"
                    )
                    raise Exception(
                        f"Price data for {collateral} is missing for the time range {time_start} - {time_end} (last processed timestamp: {last_processed_timestamp})"
                    )
        else:
            print(f"  ERROR: Provider '{provider}' is not supported")
            raise Exception(f"Provider {provider} is not supported")

        print("  Saving final processed timepoint and committing.")
        self.storage.save_processed_timepoint(self.name, time_end)
        self.storage.commit()

    @retry(
        tries=5,
        delay=1,
        backoff=2,
        jitter=(0, 0.5),
        exceptions=(Exception,),
    )
    def parse_all_prices(self):

        print("Starting parse_all_prices...")

        collaterals = self.storage.get_collaterals()
        collaterals_data = {
            collateral_data["collateral"]: collateral_data
            for collateral_data in collaterals
        }

        start_timestmap = self.get_start_timestamp()
        end_timestamp = self.get_end_timestamp()

        print(f"Start timestamp: {start_timestmap}, Last timestamp: {end_timestamp}")

        if start_timestmap > end_timestamp:
            print("Start timestamp is greater than last timestamp. Nothing to parse.")
            return

        while True:
            from_timestamp = self.get_start_timestamp()
            to_timestamp = min(end_timestamp, from_timestamp + self.chunk_range - 1)

            print(f"Chunk range: {from_timestamp}-{to_timestamp}")

            if from_timestamp > to_timestamp:
                print("All timestamps processed. Breaking loop.")
                break

            self.parse_prices(
                collaterals_data, from_timestamp, to_timestamp, self.provider
            )


if __name__ == "__main__":
    config = Config()
    storage = Storage(config)
    w3_wrapper = Web3Wrapper(config, storage)
    prices = Prices(config, w3_wrapper, storage)

    prices.parse_all_prices()
    storage.close()
