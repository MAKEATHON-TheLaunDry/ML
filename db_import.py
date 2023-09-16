from aioarango import ArangoClient
import os
from aioarango import errno, DocumentInsertError

import pandas as pd

async def main():
    client = ArangoClient(hosts="http://localhost:8529")

    # Connect to "_system" database as root user.
    sys_db = await client.db("_system", username=str("root"), password=str("Blogchain"))

    # Create a new database named "DDI_MC2".

    if not await sys_db.has_database("Transactions"):
        await sys_db.create_database("Transactions")
    trx_db = await client.db("Transactions", username=str("root"), password=str("Blogchain"))

    if not await trx_db.has_graph("course"):
        await trx_db.create_graph("course")
    graph = trx_db.graph("course")

    if not await trx_db.has_collection("accounts"):
        await trx_db.create_collection("accounts")
    accounts = trx_db.collection("accounts")
    
    if not await trx_db.has_collection("transactions"):
        await trx_db.create_collection("transactions")
    transactions = trx_db.collection("transactions")

    if not await trx_db.has_collection("register"):
        await graph.create_edge_definition(
            edge_collection="register",
            from_vertex_collections=["accounts"],
            to_vertex_collections=["accounts"]
        )
    register = trx_db.collection("register")

    df = pd.read_csv("data_0.csv")
    # turn From Bank from int to string
    df["From Bank"] = df["From Bank"].astype(str)
    df["To Bank"] = df["To Bank"].astype(str)

    # get all accounts from df
    account_dict = dict()
    for index, row in df.iterrows():
        bank = row["From Bank"]
        acc = row["Account"]
        if acc not in account_dict.keys():
            account_dict[acc] = bank
        bank = row["To Bank"]
        acc = row["Account.1"]
        if acc not in account_dict:
            account_dict[acc] = bank

    print(f"Found {len(account_dict)} accounts")

    for acc, bank in account_dict.items():
        await accounts.insert({"_key": acc, "bank": bank})



if __name__ == '__main__':
    import asyncio
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()