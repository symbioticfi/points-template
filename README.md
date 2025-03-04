# External points API template

**It is a template repository for networks and vaults that simplifies the implementation process for their point mechanics, which will be shown on the Symbiotic website ([see details here](https://symbioticfi.notion.site/External-Points-API-Specification-16981c079c178059b6b7e9740cf987a8)).**

The repository is based on Symbiotic points logic implementation and may contain excessive for your case data/code, but at the same time, it allows you to implement most of the various mechanics.

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
DEBUG=false
```

## Metadata

See example data inside the [info.json](info.json) file.

After the points' logic completion, proceed through the [given instruction](https://github.com/symbioticfi/metadata-mainnet) to integrate it into Symbiotic UI.

## How to use

There are three types of Python programs in this repository:

1. [**Fillers**](README.md#fillers) - one-time scripts that help to fill the db with collaterals that need pricing and networks in which stake should be accounted to calculate points (can be multiple times but only if necessary to add some new data).
2. [**Updaters**](README.md#updaters) - these scripts need to be executed after each particular period, e.g., every 15 minutes, to update the following data as time goes forward.
   - Blocks
   - Events
   - Prices
   - State/Points
3. [**API**](README.md#api) - an infinite-time running program that implements a REST API.

**Order of execution:**

1. **Fillers** must be called to create all the tables and fill in the global data about collaterals and networks.
2. After that, **updaters** may be called in any order with any frequency, e.g., as the following schedule via cronjobs:

   - [**Update blocks**](README.md#update-blocks) - 0,15,30,45 \* \* \* \*
   - [**Update events**](README.md#update-events) - 2,17,32,47 \* \* \* \*
   - [**Update prices**](README.md#update-prices) - 4,19,34,49 \* \* \* \*
   - [**Update points**](README.md#update-points) - 8,23,38,53 \* \* \* \*

3. **API** runner can be started any moment after the tables' creation (after the **fillers**' execution).

### Fillers

#### Fill collaterals

**Helps to fulfill the database with collaterals that need to be priced without doing it manually.**

It may be called several times when it is needed to add a new collateral to price, and the pricing data starts to be collected from the current processing block.

```
$ python3 src/fill_collaterals.py
```

#### Fill networks

**Helps to fulfill the database with networks that will receive/distribute the points without doing it manually.**

In most cases, you need to call it only once with only one network (if you are a network that wants to distribute your points) to add.

_`start_from` parameter is responsible for the block to start points calculation from (`None` means to start the calculation from the current processing block)_

```
$ python3 src/fill_networks.py
```

## Updaters

### Update blocks

**Blocks are parsed and saved into PostgreSQL DB to optimize all operations related to timestamps, including state transitions, price mappings, and points calculations.**

**Start block:** Last processed block plus 1 (or Symbiotic deployment block minus 100)\
**End block:** Latest finalized block\*

**\*** There is an assumption the 160th block before the current one is finalized

```
$ python3 src/update_blocks.py
```

### Update events

**All the events related to vault-network-operator delegations are parsed and saved into PostgreSQL DB to further recreate the needed state for points calculations in DB.**

**Start block:** Last processed block plus 1 (or Symbiotic deployment block minus 100)\
**End block:** Latest finalized block\*

**\*** There is an assumption the 160th block before the current one is finalized

```
$ python3 src/update_events.py
```

### Update prices

**Prices for all the filled collaterals ([see here](README.md#fill-collaterals)) are parsed using Alchemy and CoinMarketCap API and saved into PostgreSQL DB using block numbers as time points.**

**Start time:** Last processed timestamp plus 1 (or Symbiotic deployment timestamp minus 5 minutes)\
**End time:** Last processed block's timestamp

\* You may need to use Alchemy API instead of CMC API (see `Price.provider` variable) depending on the Symbiotic deployment timestamp vs the timestamp of script execution and the pricing plan.

```
$ python3 src/update_prices.py
```

### Update points

**1. Calculates the points for stakers, operators, and networks using the filled networks' ([see here](README.md#fill-networks)) stake amounts.\
2. Snapshots points data each 200 blocks.\
3. Performs a state transition given events from each block.**

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

## API

### Run API

**Runs an API with the format according to [the external points API specification](https://symbioticfi.notion.site/External-points-API-16581c079c17804395e2f12881ea8899).**

```
$ python3 src/api.py
```

## Docker

### Dockerfile

1. Installs Python, PostgreSQL, and other global dependencies\
2. Installs Python dependencies\
3. Copies all the needed files inside the container\
4. (default) Runs API

### Local usage

1. Build an image

```bash
docker build -t points-api:local .
```

2. Run the image and API

```bash
docker run -d \
  --name points-api \
  --env-file .env \
  -p 5000:5000 \
  points-api:local
```

3. Execute scripts

   - **Fill collaterals**

   ```bash
   docker exec points-api python src/fill_collaterals.py
   ```

   - **Fill networks**

   ```bash
   docker exec points-api python src/fill_networks.py
   ```

   - **Update blocks**

   ```bash
   docker exec points-api python src/update_blocks.py
   ```

   - **Update events**

   ```bash
   docker exec points-api python src/update_events.py
   ```

   - **Update prices**

   ```bash
   docker exec points-api python src/update_prices.py
   ```

   - **Update points**

   ```bash
   docker exec points-api python src/update_points.py
   ```

   - **Run API**

   ```bash
   docker exec points-api python src/update_points.py
   ```
