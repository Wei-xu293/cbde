from pymongo import MongoClient
from parts_suppliers_data import parts_suppliers_data
import re

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["TPCH"]
ps_collection = db["parts_suppliers"]

# Clear existing collection before inserting
ps_collection.drop()

# Insert the data
try:
    result = ps_collection.insert_many(parts_suppliers_data)
    print(f"Successfully inserted {len(result.inserted_ids)} documents")
    print(f"Inserted IDs: {result.inserted_ids}")
except Exception as e:
    print(f"Error inserting documents: {e}")


def q2(collection, size, type, region):
    pipeline = [
        {
            "$match": {
                "part.size": size,
                "part.type": {"$regex": f".*{re.escape(type)}$"},
                "supplier.regionName": region,
            }
        },
        {
            "$group": {
                "_id": "$part.mfgr",
                "minSupplyCost": {"$min": "$supplyCost"},
                "documents": {"$push": "$$ROOT"},
            }
        },
        {"$unwind": "$documents"},
        {  # filter only documents where supplycost equals minSupplyCost
            "$match": {"$expr": {"$eq": ["$documents.supplyCost", "$minSupplyCost"]}}
        },
        {"$replaceRoot": {"newRoot": "$documents"}},
        {
            "$project": {
                "_id": 0,
                "s_acctbal": "$supplier.acctbal",
                "s_name": "$supplier.name",
                "n_name": "$supplier.nationName",
                "p_partkey": "$partKey",
                "p_mfgr": "$part.mfgr",
                "s_address": "$supplier.address",
                "s_phone": "$supplier.phone",
                "s_comment": "$supplier.comment",
                "ps_supplycost": "$supplyCost",  # Para verificar si es lo mínimo
            }
        },
        {"$sort": {"s_acctbal": -1, "n_name": 1, "s_name": 1, "p_partkey": 1}},
    ]

    return list(collection.aggregate(pipeline))


if __name__ == "__main__":
    print("Query 2 for BRASS parts of size 10 in EUROPE")
    results = q2(ps_collection, 10, "BRASS", "EUROPE")
    print(f"Found {len(results)} results:")
    for i, result in enumerate(results, 1):
        print(f"\n--- Result {i} ---")
        print(f"Part Key: {result['p_partkey']}")
        print(f"Manufacturer: {result['p_mfgr']}")
        print(f"Supplier: {result['s_name']}")
        print(f"Nation: {result['n_name']}")
        print(f"Account Balance: {result['s_acctbal']}")
        print(f"Supply Cost: {result['ps_supplycost']}")
        print(f"Address: {result['s_address']}")
        print(f"Phone: {result['s_phone']}")
        print(f"Comment: {result['s_comment']}")
