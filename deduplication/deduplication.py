#
# Created on Sun May 04 2025 by Ziang Zhang
#
# Copyright (c) 2025 St. Jude Children's Research Hospital
#
# Use of this source code is governed by an GNU GPLv3
# license that can be found in the LICENSE file or at
# https://www.gnu.org/licenses/gpl-3.0.html
#
# Remove the duplications in a collection

from pymongo import MongoClient
from pymongo.collection import Collection
import argparse

def remove_duplicates(
    src_col: Collection, 
    dest_col: Collection, 
    field: str,
    batch_size: int = 1000
    ) -> int:
    """
    Copy unique documents from src_col to dest_col based on a specified field.

    Args:
        src_col (Collection): Source MongoDB collection.
        dest_col (Collection): Destination MongoDB collection.
        field (str): Field name on which to dedupe (e.g. "email").
        batch_size: How many docs to buffer before each bulk insert.
    """
    # 1. Build aggregation pipeline:
    #    - $group by the dedupe field, keep the first document for each value
    #    - $replaceRoot to unwrap the document
    pipeline = [
        {"$sort": {field: 1}},             # optional: ensure deterministic "first"
        {"$group": {
            "_id": f"${field}",
            "doc": {"$first": "$$ROOT"}
        }},
        {"$replaceRoot": {"newRoot": "$doc"}}
    ]
    cursor = src_col.aggregate(pipeline)
    # 2. Clean up the destination collection
    dest_col.delete_many({})
    
    # 3. Stream the aggregation, buffering into batches
    cursor = src_col.aggregate(pipeline, allowDiskUse=True)
    ops = []
    total = 0

    for doc in cursor:
        ops.append(InsertOne(doc))
        # once we reach batch_size, flush
        if len(ops) >= batch_size:
            dest_col.bulk_write(ops, ordered=False)
            total += len(ops)
            ops = []

    # 4. Flush any remaining ops
    if ops:
        dest_col.bulk_write(ops, ordered=False)
        total += len(ops)

    return total


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Remove duplicates from a MongoDB collection.")
    parser.add_argument("--uri", required=True, help="MongoDB URI")
    parser.add_argument("--db", required=True, help="MongoDB database name")
    parser.add_argument("--src", required=True, help="Source MongoDB collection name")
    parser.add_argument("--dest", required=True, help="Destination MongoDB collection name")
    parser.add_argument("--field", required=True, help="Field to deduplicate on")
    parser.add_argument("--batch_size", type=int, default=1000, help="Batch size for bulk insert")

    args = parser.parse_args()

    # Connect to MongoDB
    client = MongoClient(args.uri)
    db = client[args.db]
    src_col = db[args.src]
    dest_col = db[args.dest]
    # Ensure destination collection is empty
    if dest_col.count_documents({}) > 0:
        proceed = input(f"Destination collection {args.dest} is not empty. Proceed? (y/n): ")
        if proceed.lower() != 'y':
            print("Operation aborted.")
            exit(1)

    # Remove duplicates
    total_removed = remove_duplicates(src_col, dest_col, args.field, args.batch_size)
    print(f"Total documents removed: {total_removed}")
