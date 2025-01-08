# External points API template

It is a template repository for networks and vaults that simplifies the implementation process for their point mechanics, which will be shown on the Symbiotic website ([see details here](https://symbioticfi.notion.site/External-points-API-16581c079c17804395e2f12881ea8899)).

\* The repository is based on Symbiotic points logic implementation and may contain excessive for your case data/code, but at the same time, it allows you to implement most of the various mechanics.

## Prerequisites

- Python
- PostgreSQL

## Install

```bash
pip3 install -r requirements.txt
```

## Env

Create a `.env` file similar to the example below:

```
RPC=
CHAIN=holesky
CMC_API_KEY=
ALCHEMY_API_KEY=
BLOCKSCOUT_API_KEY=
PGSQL_NAME=holesky
PGSQL_USER=points-api
PGSQL_PASSWORD=1234
PGSQL_HOST=localhost
PGSQL_PORT=5432
```

## Metadata

See example data inside the [info.json](info.json) file.

### Helpers

#### Fill collaterals

Helps to fulfill the database with collaterals that need to be priced without doing it manually.

```
$ python3 src/fill_collaterals.py
```

#### Fill networks

Helps to fulfill the database with networks that will receive/distribute the points without doing it manually.

```
$ python3 src/fill_networks.py
```

## Usage

In the current implementation, the scripts' execution order during the first launch is important. More precisely, the events updater must be launched first to create all the needed tables, and the others' order may be any.

### Update events

All the events related to vault-network-operator delegations are parsed and saved into PostgreSQL DB.

**Start block:** Last processed block plus 1 (or Symbiotic deployment block minus 100)\
**End block:** Latest finalized block\*

**\*** There is an assumption the 160th block before the current one is finalized

```
$ python3 src/update_events.py
```

### Update blocks

Blocks are parsed and saved into PostgreSQL DB.

**Start block:** Last processed block plus 1 (or Symbiotic deployment block minus 100)\
**End block:** Latest finalized block\*

**\*** There is an assumption the 160th block before the current one is finalized

```
$ python3 src/update_blocks.py
```

### Update prices

Prices for all the filled collaterals ([see here](README.md#fill-collaterals)) are parsed using Alchemy and CoinMarketCap API and saved into PostgreSQL DB using block numbers as time points.

**Start time:** Last processed timestamp plus 1 (or Symbiotic deployment timestamp minus 5 minutes)\
**End time:** Last processed block's timestamp

\* You may need to use Alchemy API instead of CMC API (see `Price.provider` variable) depending on the Symbiotic deployment timestamp vs the timestamp of script execution and the pricing plan.

```
$ python3 src/update_prices.py
```

### Update points

1. Calculates the points for stakers, operators, and networks using the filled networks' ([see here](README.md#fill-networks)) stake amounts.
2. Snapshots points data each 200 blocks.
3. Performs a state transition given events from each block.

**Start block:** Last processed block plus 1 (or Symbiotic deployment block)\
**End block:** Min(last processed events' block, last processed prices' block)

**The most relevant functions to change:**

1. `parse_points_per_network()` (e.g., to adjust the points mechanic to your needs)
2. `validate()` (e.g., to skip vaults with particular delegator types)
3. `get_end_block()` (e.g., in case of pricing mechanic removal)

**Base implementation:** Points are distributed with the `max_rate` per $ per hour, where the `operator_fee` part (in percentages) goes to operators, and the remaining one goes to users.

```
$ python3 src/update_points.py
```

### Run API

Runs an API with the format according to [the external points API specification](https://symbioticfi.notion.site/External-points-API-16581c079c17804395e2f12881ea8899).

```
$ python3 src/api.py
```
