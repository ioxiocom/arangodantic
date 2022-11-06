import asyncio
from uuid import uuid4

from aioarangodb import ArangoClient
from pydantic import BaseModel
from shylock import AsyncLock as Lock
from shylock import ShylockAioArangoDBBackend
from shylock import configure as configure_shylock

from arangodantic import ASCENDING, DocumentModel, EdgeModel, configure
from arangodantic.indexes import HashIndex


# Define models
class Owner(BaseModel):
    """Dummy owner Pydantic model."""

    first_name: str
    last_name: str


class Company(DocumentModel):
    """Dummy company Arangodantic model."""

    class ArangodanticConfig:
        indexes = [
            HashIndex(fields=["company_id"], unique=True),
            HashIndex(fields=["owner.first_name"]),
            HashIndex(fields=["owner.last_name"]),
        ]

    company_id: str
    owner: Owner


class Link(EdgeModel):
    """Dummy Link Arangodantic model."""

    class ArangodanticConfig:
        indexes = [HashIndex(fields=["_from", "_to", "type"], unique=True)]

    type: str


async def main():
    # Configure the database settings
    hosts = "http://localhost:8529"
    username = "root"
    password = ""
    database = "example"
    prefix = "example-"

    client = ArangoClient(hosts=hosts)
    # Connect to "_system" database and create the actual database if it doesn't exist
    # Only for demo, you likely want to create the database in advance.
    sys_db = await client.db("_system", username=username, password=password)
    if not await sys_db.has_database(database):
        await sys_db.create_database(database)

    # Configure Arangodantic and Shylock
    db = await client.db(database, username=username, password=password)
    configure_shylock(await ShylockAioArangoDBBackend.create(db, f"{prefix}shylock"))
    configure(db, prefix=prefix, key_gen=uuid4, lock=Lock)

    # Create collections and indexes if they don't yet exist
    # Only for demo, you likely want to create the collections in advance.
    await Company.ensure_collection()
    await Company.ensure_indexes()
    await Link.ensure_collection()
    await Link.ensure_indexes()

    # Clean up any existing data.
    await Company.truncate_collection()
    await Link.truncate_collection()

    # Let's create some example entries
    owner = Owner(first_name="John", last_name="Doe")
    company = Company(company_id="1234567-8", owner=owner)
    await company.save()
    print(f"Company saved with key: {company.key_}")

    second_owner = Owner(first_name="Jane", last_name="Doe")
    second_company = Company(company_id="2345678-9", owner=second_owner)
    await second_company.save()
    print(f"Second company saved with key: {second_company.key_}")

    link = Link(_from=company, _to=second_company, type="CustomerOf")
    await link.save()
    print(f"Link saved with key: {link.key_}")

    # Hold named locks while loading and doing changes
    async with Company.lock_and_load(company.key_) as c:
        assert c.owner == owner
        c.owner.first_name = "Joanne"
        await c.save()

    await company.reload()
    print(f"Updated owner of company to '{company.owner!r}'")

    # Let's explore the find functionality
    # Note: You likely want to add indexes to support the queries
    print("Finding companies owned by a person with last name 'Doe'")
    async with (await Company.find({"owner.last_name": "Doe"}, count=True)) as cursor:
        print(f"Found {len(cursor)} companies")
        async for found_company in cursor:
            print(f"Company: {found_company.company_id}")

    # Supported operators include: "==", "!=", "<", "<=", ">", ">="
    found_company = await Company.find_one(
        {"owner.last_name": "Doe", "_id": {"!=": company}}
    )
    print(f"Found the company {found_company.key_}")

    # Find also supports sorting and the cursor can easily be converted to a list
    companies = await (
        await Company.find(
            sort=[
                ("owner.last_name", ASCENDING),
                ("owner.first_name", ASCENDING),
            ]
        )
    ).to_list()
    print("Companies sorted by owner:")
    for c in companies:
        print(f"Company {c.company_id}, owner: {c.owner!r}")


if __name__ == "__main__":
    # Starting from Python 3.7 ->
    # asyncio.run(main())

    # Compatible with Python 3.6 ->
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(main())
