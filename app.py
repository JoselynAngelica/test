import os
import json
import threading
import time
from flask import Flask, request, jsonify
from flask_cors import CORS
from web3 import Web3
from web3.middleware.proof_of_authority import ExtraDataToPOAMiddleware
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime

# ==============================================================================
# 1. INITIAL SETUP & CONFIGURATION
# ==============================================================================
print("Starting Flask backend...")

load_dotenv()

app = Flask(__name__)
CORS(app)

WEB3_PROVIDER = os.getenv("WEB3_PROVIDER")
PRIVATE_KEY = os.getenv("PRIVATE_KEY")
PUBLIC_ADDRESS = os.getenv("PUBLIC_ADDRESS")
CONTRACT_ADDRESS = os.getenv("CONTRACT_ADDRESS")
MONGO_URI = os.getenv("MONGO_URI")

if not all([WEB3_PROVIDER, PRIVATE_KEY, PUBLIC_ADDRESS, CONTRACT_ADDRESS, MONGO_URI]):
    raise EnvironmentError("One or more required environment variables are missing. Please check your .env file.")

w3 = None
contract = None
try:
    w3 = Web3(Web3.HTTPProvider(WEB3_PROVIDER))
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    
    if not w3.is_connected():
        raise ConnectionError("Failed to connect to Web3 provider.")

    with open("./abi.json") as f:
        abi = json.load(f)

    contract = w3.eth.contract(address=w3.to_checksum_address(CONTRACT_ADDRESS), abi=abi)

    print(f"Connected to Web3 provider: {WEB3_PROVIDER}")
    print(f"Contract loaded at: {CONTRACT_ADDRESS}")

except Exception as e:
    print(f"Error setting up Web3 or contract: {e}")
    exit(1)

client = None
auctions_collection = None
bids_collection = None
try:
    client = MongoClient(MONGO_URI)
    db = client.nft_auction
    auctions_collection = db.auctions
    bids_collection = db.bids
    print("Connected to MongoDB.")
except Exception as e:
    print(f"Error setting up MongoDB: {e}")
    exit(1)

# ==============================================================================
# 2. BACKGROUND EVENT LISTENER
# ==============================================================================

# A dictionary to store the last processed block for each event type.
# This makes the event listener more resilient and easier to manage.
last_processed_blocks = {
    "AuctionCreated": w3.eth.block_number,
    "NewBid": w3.eth.block_number,
    "AuctionEnded": w3.eth.block_number,
    "NFTTransferred": w3.eth.block_number,
}

# Corrected event listener with increased sleep time
def listen_for_events():
    print("Starting blockchain event listener...")
    while True:
        try:
            current_block = w3.eth.block_number
            
            # Process AuctionCreated events
            if current_block > last_processed_blocks["AuctionCreated"]:
                from_block = last_processed_blocks["AuctionCreated"] + 1
                to_block = current_block
                
                auction_created_logs = contract.events.AuctionCreated.get_logs(from_block=from_block, to_block=to_block)
                for log in auction_created_logs:
                    args = log.args
                    print(f"Event: AuctionCreated - Auction ID: {args.auctionId}")
                    auctions_collection.update_one(
                        {"auctionId": args.auctionId},
                        {"$set": {
                            "auctionId": args.auctionId, "seller": args.seller, "itemType": args.itemType,
                            "nftContract": args.nftContract, "tokenId": args.tokenId,
                            "startTime": args.startTime, "endTime": args.endTime,
                            "highestBid": 0, "highestBidder": "0x0000000000000000000000000000000000000000",
                            "ended": False, "txHash": log.transactionHash.hex(),
                            "blockNumber": log.blockNumber,
                            "timestamp": w3.eth.get_block(log.blockNumber).timestamp
                        }}, upsert=True
                    )
                last_processed_blocks["AuctionCreated"] = current_block

            # Process NewBid events
            if current_block > last_processed_blocks["NewBid"]:
                from_block = last_processed_blocks["NewBid"] + 1
                to_block = current_block

                new_bid_logs = contract.events.NewBid.get_logs(from_block=from_block, to_block=to_block)
                for log in new_bid_logs:
                    args = log.args
                    print(f"Event: NewBid - Auction ID: {args.auctionId}, Bidder: {args.bidder}, Amount: {w3.from_wei(args.amount, 'ether')} ETH")
                    auctions_collection.update_one(
                        {"auctionId": args.auctionId},
                        {"$set": {"highestBid": args.amount, "highestBidder": args.bidder}}
                    )
                    bids_collection.insert_one({
                        "auctionId": args.auctionId, "bidder": args.bidder, "amount": args.amount,
                        "txHash": log.transactionHash.hex(), "blockNumber": log.blockNumber,
                        "timestamp": w3.eth.get_block(log.blockNumber).timestamp
                    })
                last_processed_blocks["NewBid"] = current_block

            # Process AuctionEnded events
            if current_block > last_processed_blocks["AuctionEnded"]:
                from_block = last_processed_blocks["AuctionEnded"] + 1
                to_block = current_block
                
                auction_ended_logs = contract.events.AuctionEnded.get_logs(from_block=from_block, to_block=to_block)
                for log in auction_ended_logs:
                    args = log.args
                    print(f"Event: AuctionEnded - Auction ID: {args.auctionId}, Winner: {args.winner}, Amount: {w3.from_wei(args.amount, 'ether')} ETH")
                    auctions_collection.update_one(
                        {"auctionId": args.auctionId},
                        {"$set": {"ended": True, "winner": args.winner, "finalBid": args.amount}}
                    )
                last_processed_blocks["AuctionEnded"] = current_block

            # Process NFTTransferred events
            if current_block > last_processed_blocks["NFTTransferred"]:
                from_block = last_processed_blocks["NFTTransferred"] + 1
                to_block = current_block

                nft_transferred_logs = contract.events.NFTTransferred.get_logs(from_block=from_block, to_block=to_block)
                for log in nft_transferred_logs:
                    args = log.args
                    print(f"Event: NFTTransferred - Auction ID: {args.auctionId}, To: {args.to}, Token ID: {args.tokenId}")
                last_processed_blocks["NFTTransferred"] = current_block

            time.sleep(60)  # Increased sleep time to avoid Infura rate limits

        except Exception as e:
            print(f"Error in event listener: {e}")
            time.sleep(120)  # Increased sleep time on error to prevent constant retries

event_listener_thread = threading.Thread(target=listen_for_events, daemon=True)
event_listener_thread.start()

# ==============================================================================
# 3. FLASK API ENDPOINTS
# ==============================================================================

@app.route("/")
def home():
    return "Auction Backend is Live!"

@app.route("/create-auction", methods=["POST"])
def create_auction():
    try:
        data = request.json
        if not data:
            return jsonify({"error": "Invalid JSON payload"}), 400
        
        starting_bid = data.get("startingBid", 0)

        item_type_map = {"FLIGHT": 0, "HOTEL": 1, "TOUR_TICKET": 2, "TRANSPORT": 3}
        itemType_str = data.get("itemType")
        if itemType_str not in item_type_map:
            return jsonify({"error": "Invalid itemType. Must be FLIGHT, HOTEL, TOUR_TICKET, or TRANSPORT."}), 400
        itemType = item_type_map[itemType_str]

        nftContract = data.get("nftContract")
        tokenId = data.get("tokenId")
        startTime = data.get("startTime")
        endTime = data.get("endTime")

        if not all([nftContract, tokenId is not None, startTime is not None, endTime is not None]):
            return jsonify({"error": "Missing required fields: nftContract, tokenId, startTime, endTime"}), 400

        try:
            tokenId = int(tokenId)
            startTime = int(startTime)
            endTime = int(endTime)
            starting_bid = int(starting_bid)
        except ValueError:
            return jsonify({"error": "tokenId, startTime, endTime, and startingBid must be integers."}), 400

        current_timestamp = w3.eth.get_block("latest").timestamp
        if endTime <= startTime or startTime < current_timestamp:
            return jsonify({"error": "Invalid auction times. endTime must be after startTime, and startTime must be in the future."}), 400

        nonce = w3.eth.get_transaction_count(PUBLIC_ADDRESS)
        gas_price = w3.eth.gas_price

        tx = contract.functions.createAuction(
            itemType, w3.to_checksum_address(nftContract), tokenId, startTime, endTime, starting_bid
        ).build_transaction({
            'from': PUBLIC_ADDRESS, 'nonce': nonce, 'gas': 500000, 'gasPrice': gas_price
        })

        signed_tx = w3.eth.account.sign_transaction(tx, PRIVATE_KEY)
        tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        print(f"Create auction transaction sent: {tx_hash.hex()}")
        
        return jsonify({"status": "Auction transaction sent", "txHash": tx_hash.hex()}), 200

    except Exception as e:
        print(f"Error creating auction: {e}")
        return jsonify({"error": "Internal server error during auction creation."}), 500

@app.route("/prepare-bid", methods=["POST"])
def prepare_bid():
    try:
        data = request.json
        if not data:
            return jsonify({"error": "Invalid JSON payload"}), 400
        auctionId = data.get("auctionId")
        amount = data.get("amount")
        bidder = data.get("from")
        if not all([auctionId is not None, amount is not None, bidder]):
            return jsonify({"error": "Missing required fields: auctionId, amount, from"}), 400
        try:
            auctionId = int(auctionId)
            amount_ether = float(amount)
            amount_wei = w3.to_wei(amount_ether, 'ether')
        except ValueError:
            return jsonify({"error": "auctionId must be an integer, amount must be a number."}), 400
        if not w3.is_checksum_address(bidder):
            return jsonify({"error": "Invalid 'from' address (bidder)."}), 400
        nonce = w3.eth.get_transaction_count(bidder)
        gas_price = w3.eth.gas_price
        
        tx = contract.functions.bid(auctionId).build_transaction({
            'from': bidder, 'value': amount_wei, 'nonce': nonce,
            'gas': 300000, 'gasPrice': gas_price
        })
        
        return jsonify({"status": "Transaction prepared", "tx": tx}), 200
    except Exception as e:
        print(f"Error preparing bid: {e}")
        return jsonify({"error": "Internal server error during bid preparation."}), 500

@app.route("/send-signed-transaction", methods=["POST"])
def send_signed_transaction():
    try:
        data = request.json
        if not data or "signedTx" not in data:
            return jsonify({"error": "Missing signed transaction data."}), 400
        
        signed_tx_hex = data["signedTx"]
        signed_tx_bytes = w3.to_bytes(hexstr=signed_tx_hex)
        
        tx_hash = w3.eth.send_raw_transaction(signed_tx_bytes)
        
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
        return jsonify({"status": "Transaction sent and confirmed", "txHash": tx_hash.hex(), "blockNumber": receipt.blockNumber}), 200
    except Exception as e:
        print(f"Error broadcasting signed transaction: {e}")
        return jsonify({"error": "Failed to broadcast transaction.", "message": str(e)}), 500

@app.route("/end-auction", methods=["POST"])
def end_auction():
    try:
        data = request.json
        if not data:
            return jsonify({"error": "Invalid JSON payload"}), 400
        auctionId = data.get("auctionId")
        if auctionId is None:
            return jsonify({"error": "Missing required field: auctionId"}), 400
        try:
            auctionId = int(auctionId)
        except ValueError:
            return jsonify({"error": "auctionId must be an integer."}), 400
        nonce = w3.eth.get_transaction_count(PUBLIC_ADDRESS)
        gas_price = w3.eth.gas_price
        tx = contract.functions.endAuction(auctionId).build_transaction({
            'from': PUBLIC_ADDRESS, 'nonce': nonce, 'gas': 300000, 'gasPrice': gas_price
        })
        signed_tx = w3.eth.account.sign_transaction(tx, PRIVATE_KEY)
        tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        print(f"End auction transaction sent: {tx_hash.hex()}")
        
        return jsonify({"status": "Auction end transaction sent", "txHash": tx_hash.hex()}), 200
    except Exception as e:
        print(f"Error ending auction: {e}")
        return jsonify({"error": "Internal server error during auction ending."}), 500

@app.route("/auctions", methods=["GET"])
def get_auctions():
    try:
        status = request.args.get('status')
        query = {}
        if status == 'active':
            current_timestamp = w3.eth.get_block("latest").timestamp
            query["ended"] = False
            query["endTime"] = {"$gt": current_timestamp}
        elif status == 'ended':
            query["ended"] = True

        item_type = request.args.get('itemType')
        if item_type:
            item_type_map = {"FLIGHT": 0, "HOTEL": 1, "TOUR_TICKET": 2, "TRANSPORT": 3}
            if item_type in item_type_map:
                query["itemType"] = item_type_map[item_type]
        
        auctions_data = []
        for auction in auctions_collection.find(query).sort("timestamp", -1):
            auction["_id"] = str(auction["_id"])
            auction["startTimeFormatted"] = datetime.fromtimestamp(auction["startTime"]).isoformat()
            auction["endTimeFormatted"] = datetime.fromtimestamp(auction["endTime"]).isoformat()
            auction["highestBidEther"] = w3.from_wei(auction.get("highestBid", 0), 'ether')
            bids_count = bids_collection.count_documents({"auctionId": auction["auctionId"]})
            auction["bids"] = bids_count
            auctions_data.append(auction)
        return jsonify(auctions_data), 200
    except Exception as e:
        print(f"Error fetching auctions: {e}")
        return jsonify({"error": "Internal server error fetching auctions."}), 500

@app.route("/auctions/<int:auction_id>", methods=["GET"])
def get_auction_details(auction_id):
    try:
        auction = auctions_collection.find_one({"auctionId": auction_id})
        if not auction:
            return jsonify({"error": "Auction not found."}), 404
        auction["_id"] = str(auction["_id"])
        auction["startTimeFormatted"] = datetime.fromtimestamp(auction["startTime"]).isoformat()
        auction["endTimeFormatted"] = datetime.fromtimestamp(auction["endTime"]).isoformat()
        auction["highestBidEther"] = w3.from_wei(auction.get("highestBid", 0), 'ether')
        bid_history = []
        for bid in bids_collection.find({"auctionId": auction_id}).sort("timestamp", 1):
            bid["_id"] = str(bid["_id"])
            bid["amountEther"] = w3.from_wei(bid.get("amount", 0), 'ether')
            bid["timestampFormatted"] = datetime.fromtimestamp(bid["timestamp"]).isoformat()
            bid_history.append(bid)
        auction["bidHistory"] = bid_history
        return jsonify(auction), 200
    except Exception as e:
        print(f"Error fetching auction details: {e}")
        return jsonify({"error": "Internal server error fetching auction details."}), 500

if __name__ == "__main__":
    from waitress import serve
    print("Serving with Waitress on http://0.0.0.0:5000")
    serve(app, host='0.0.0.0', port=5000)