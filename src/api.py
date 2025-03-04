import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="eth_utils")

from web3 import Web3
from flask import Flask, request, jsonify

from common.config import Config
from common.storage import Storage

app = Flask(__name__)


@app.route("/api/health", methods=["GET"])
def api_health():
    try:
        return jsonify({"status": "ok", "message": "Service is healthy."}), 200
    except Exception as e:
        print(f"Error: {e}")
        return (
            jsonify(
                {
                    "error": "Unexpected error",
                    "request_id": "health_0",
                }
            ),
            500,
        )


@app.route("/api/last_block", methods=["GET"])
def api_last_block():
    try:
        config = Config()
        storage = Storage(config)
        try:
            last_block_number = storage.get_last_snapshot_block_number()
        except Exception as e:
            print(f"Error: {e}")
            return (
                jsonify(
                    {
                        "error": "Failed to get last processed block",
                        "request_id": "last_block_0",
                    }
                ),
                500,
            )
        return (
            jsonify(
                {
                    "last_block_number": (
                        last_block_number if last_block_number != None else 0
                    )
                }
            ),
            200,
        )
    except Exception as e:
        print(f"Error: {e}")
        return (
            jsonify(
                {
                    "error": "Unexpected error",
                    "request_id": "last_block_1",
                }
            ),
            500,
        )


@app.route("/api/<string:receiver_type>/<string:receiver_address>", methods=["GET"])
def api_points(receiver_type, receiver_address):
    try:
        if receiver_type not in ["staker", "network", "operator"]:
            return jsonify({"error": "Invalid receiver type"}), 400
        try:
            receiver_address = Web3.to_checksum_address(receiver_address)
        except Exception as e:
            print(f"Error: {e}")
            return jsonify({"error": "Invalid receiver address"}), 400
        block_number = request.args.get("block_number", default=None)
        try:
            block_number = int(block_number)
        except Exception as e:
            print(f"Error: {e}")
            return jsonify({"error": "Invalid block"}), 400

        config = Config()
        storage = Storage(config)
        try:
            updated_at = storage.get_closest_points_snapshot_block_number(block_number)
        except Exception as e:
            print(f"Error: {e}")
            return (
                jsonify(
                    {
                        "error": "Failed to get closest updated block",
                        "request_id": "points_0",
                    }
                ),
                500,
            )
        if updated_at == None:
            return jsonify(
                {
                    "receiver_address": receiver_address,
                    "receiver_type": receiver_type,
                    "block_number": 0,
                    "points": [],
                }
            )

        if receiver_type == "staker":
            try:
                points = storage.get_network_vault_user_points_historical_per_user(
                    updated_at, receiver_address
                )
            except Exception as e:
                print(f"Error: {e}")
                return (
                    jsonify(
                        {
                            "error": "Failed to get staker historical points",
                            "request_id": "points_1",
                        }
                    ),
                    500,
                )
        elif receiver_type == "network":
            try:
                points = storage.get_network_vault_points_historical_per_network(
                    updated_at, receiver_address
                )
            except Exception as e:
                print(f"Error: {e}")
                return (
                    jsonify(
                        {
                            "error": "Failed to get network historical points",
                            "request_id": "points_2",
                        }
                    ),
                    500,
                )
        elif receiver_type == "operator":
            try:
                points = (
                    storage.get_network_operator_vault_points_historical_per_operator(
                        updated_at, receiver_address
                    )
                )
            except Exception as e:
                print(f"Error: {e}")
                return (
                    jsonify(
                        {
                            "error": "Failed to get operator historical points",
                            "request_id": "points_3",
                        }
                    ),
                    500,
                )

        return jsonify(
            {
                "receiver_address": receiver_address,
                "receiver_type": receiver_type,
                "block_number": updated_at,
                "points": [
                    {
                        "network_address": (
                            data["network"]
                            if receiver_type != "network"
                            else receiver_address
                        ),
                        "vault_address": data["vault"],
                        "points": data["points"],
                    }
                    for data in points
                ],
            }
        )
    except Exception as e:
        print(f"Error: {e}")
        return (
            jsonify(
                {
                    "error": "Unexpected error",
                    "request_id": "points_4",
                }
            ),
            500,
        )


@app.route("/api/stats", methods=["GET"])
def api_stats():
    try:
        receiver_type = request.args.get("receiver_type", default=None)
        if receiver_type != None and receiver_type not in [
            "staker",
            "network",
            "operator",
        ]:
            return jsonify({"error": "Invalid receiver type"}), 400
        block_number = request.args.get("block_number", default=None)
        try:
            block_number = int(block_number)
        except Exception as e:
            print(f"Error: {e}")
            return jsonify({"error": "Invalid block"}), 400

        config = Config()
        storage = Storage(config)

        try:
            snapshot_block_number = storage.get_closest_points_snapshot_block_number(
                block_number
            )
        except Exception as e:
            print(f"Error: {e}")
            return (
                jsonify(
                    {
                        "error": "Failed to get closest updated block",
                        "request_id": "stats_0",
                    }
                ),
                500,
            )
        if snapshot_block_number == None:
            return (
                jsonify({"total_points": 0, "unique_receivers": 0}),
                200,
            )

        total_points = 0
        stakers = 0
        networks = 0
        operators = 0
        if receiver_type == None:
            try:
                stats = storage.get_network_vault_user_points_historical_stats(
                    snapshot_block_number
                )
            except Exception as e:
                print(f"Error: {e}")
                return (
                    jsonify(
                        {
                            "error": "Failed to get staker historical stats",
                            "request_id": "stats_1",
                        }
                    ),
                    500,
                )
            total_points += stats["total_points"]
            stakers = stats["unique_receivers"]
            try:
                stats = storage.get_network_vault_points_historical_stats(
                    snapshot_block_number
                )
            except Exception as e:
                print(f"Error: {e}")
                return (
                    jsonify(
                        {
                            "error": "Failed to get network historical stats",
                            "request_id": "stats_2",
                        }
                    ),
                    500,
                )
            total_points += stats["total_points"]
            networks = stats["unique_receivers"]
            try:
                stats = storage.get_network_operator_points_historical_stats(
                    snapshot_block_number
                )
            except Exception as e:
                print(f"Error: {e}")
                return (
                    jsonify(
                        {
                            "error": "Failed to get operator historical stats",
                            "request_id": "stats_3",
                        }
                    ),
                    500,
                )
            total_points += stats["total_points"]
            operators = stats["unique_receivers"]
        elif receiver_type == "staker":
            try:
                stats = storage.get_network_vault_user_points_historical_stats(
                    snapshot_block_number
                )
            except Exception as e:
                print(f"Error: {e}")
                return (
                    jsonify(
                        {
                            "error": "Failed to get staker historical stats",
                            "request_id": "stats_4",
                        }
                    ),
                    500,
                )
            total_points = stats["total_points"]
            stakers = stats["unique_receivers"]
        elif receiver_type == "network":
            try:
                stats = storage.get_network_vault_points_historical_stats(
                    snapshot_block_number
                )
            except Exception as e:
                print(f"Error: {e}")
                return (
                    jsonify(
                        {
                            "error": "Failed to get network historical stats",
                            "request_id": "stats_5",
                        }
                    ),
                    500,
                )
            total_points = stats["total_points"]
            networks = stats["unique_receivers"]
        elif receiver_type == "operator":
            try:
                stats = storage.get_network_operator_points_historical_stats(
                    snapshot_block_number
                )
            except Exception as e:
                print(f"Error: {e}")
                return (
                    jsonify(
                        {
                            "error": "Failed to get operator historical stats",
                            "request_id": "stats_6",
                        }
                    ),
                    500,
                )
            total_points = stats["total_points"]
            operators = stats["unique_receivers"]

        return (
            jsonify(
                {
                    "total_points": total_points,
                    "stakers": stakers,
                    "networks": networks,
                    "operators": operators,
                }
            ),
            200,
        )
    except Exception as e:
        print(f"Error: {e}")
        return (
            jsonify(
                {
                    "error": "Unexpected error",
                    "request_id": "stats_7",
                }
            ),
            500,
        )


@app.route("/api/all", methods=["GET"])
def api_all():
    try:
        offset = request.args.get("offset", default=None)
        try:
            offset = int(offset)
        except Exception as e:
            print(f"Error: {e}")
            return jsonify({"error": "Invalid offset"}), 400
        limit = request.args.get("limit", default=None)
        try:
            limit = int(limit)
        except Exception as e:
            print(f"Error: {e}")
            return jsonify({"error": "Invalid limit"}), 400
        receiver_type = request.args.get("receiver_type", default=None)
        if receiver_type != None and receiver_type not in [
            "staker",
            "network",
            "operator",
        ]:
            return jsonify({"error": "Invalid receiver type"}), 400
        block_number = request.args.get("block_number", default=None)
        try:
            block_number = int(block_number)
        except Exception as e:
            print(f"Error: {e}")
            return jsonify({"error": "Invalid block"}), 400

        config = Config()
        storage = Storage(config)

        try:
            updated_at = storage.get_closest_points_snapshot_block_number(block_number)
        except Exception as e:
            print(f"Error: {e}")
            return (
                jsonify(
                    {
                        "error": "Failed to get closest updated block",
                        "request_id": "all_0",
                    }
                ),
                500,
            )
        if updated_at == None:
            return (
                jsonify([]),
                200,
            )
        if receiver_type == None:
            try:
                points = storage.get_points_historical_all(updated_at, offset, limit)
            except Exception as e:
                print(f"Error: {e}")
                return (
                    jsonify(
                        {
                            "error": "Failed to get all historical points",
                            "request_id": "all_1",
                        }
                    ),
                    500,
                )
        if receiver_type == "staker":
            try:
                points = storage.get_network_vault_user_points_historical_all(
                    updated_at, offset, limit
                )
            except Exception as e:
                print(f"Error: {e}")
                return (
                    jsonify(
                        {
                            "error": "Failed to get stakers historical points",
                            "request_id": "all_2",
                        }
                    ),
                    500,
                )
        elif receiver_type == "network":
            try:
                points = storage.get_network_vault_points_historical_all(
                    updated_at, offset, limit
                )
            except Exception as e:
                print(f"Error: {e}")
                return (
                    jsonify(
                        {
                            "error": "Failed to get networks historical points",
                            "request_id": "all_3",
                        }
                    ),
                    500,
                )
        elif receiver_type == "operator":
            try:
                points = storage.get_network_operator_vault_points_historical_all(
                    updated_at, offset, limit
                )
            except Exception as e:
                print(f"Error: {e}")
                return (
                    jsonify(
                        {
                            "error": "Failed to get operators historical points",
                            "request_id": "all_4",
                        }
                    ),
                    500,
                )

        if receiver_type == None:
            return (
                jsonify(
                    [
                        {
                            "receiver_address": data["receiver"],
                            "receiver_type": data["receiver_type"],
                            "block_number": updated_at,
                            "network_address": data["network"],
                            "vault_address": data["vault"],
                            "points": data["points"],
                        }
                        for data in points
                    ]
                ),
                200,
            )
        elif receiver_type == "staker":
            return (
                jsonify(
                    [
                        {
                            "receiver_address": data["staker"],
                            "receiver_type": receiver_type,
                            "block_number": updated_at,
                            "network_address": data["network"],
                            "vault_address": data["vault"],
                            "points": data["points"],
                        }
                        for data in points
                    ]
                ),
                200,
            )
        elif receiver_type == "network":
            return (
                jsonify(
                    [
                        {
                            "receiver_address": data["network"],
                            "receiver_type": receiver_type,
                            "block_number": updated_at,
                            "network_address": data["network"],
                            "vault_address": data["vault"],
                            "points": data["points"],
                        }
                        for data in points
                    ]
                ),
                200,
            )
        elif receiver_type == "operator":
            return (
                jsonify(
                    [
                        {
                            "receiver_address": data["operator"],
                            "receiver_type": receiver_type,
                            "block_number": updated_at,
                            "network_address": data["network"],
                            "vault_address": data["vault"],
                            "points": data["points"],
                        }
                        for data in points
                    ]
                ),
                200,
            )
    except Exception as e:
        print(f"Error: {e}")
        return (
            jsonify(
                {
                    "error": "Unexpected error",
                    "request_id": "all_5",
                }
            ),
            500,
        )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
