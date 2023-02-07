################################################################################
# Given the following `customer` table
#  implement the `get_customer_details` method to securely fetch
#  customer data by id
#
# Use any database syntax you would like
#
#                          Table "public.customer"
#      Column     |          Type          | Collation | Nullable | Default
# ----------------+------------------------+-----------+----------+---------
#  id             | integer                |           | not null |
#  first_name     | character varying(24)  |           |          |
#  last_name      | character varying(24)  |           |          |
#  street_address | character varying(256) |           |          |
#  postal_code    | character varying(16)  |           |          |
#  city           | character varying(24)  |           |          |
# Indexes:
#     "customer_pkey" PRIMARY KEY, btree (id)
#
#
# postgres=# select * from customer;
#  id | first_name | last_name | street_address | postal_code |  city
# ----+------------+-----------+----------------+-------------+--------
#   1 | John       | Doe       | 1 Martin Place | 2000        | Sydney
#   2 | Jane       | Doe       | 2 Martin Place | 2000        | Sydney
# (2 rows)
#
################################################################################
import asyncio
from typing import Any, Dict, Optional

import asyncpg

DATABASE_URL = "postgres://postgres:postgres@127.0.0.1:5436/airbnb"


async def get_customer_details(customer_id: int) -> Optional[Dict[str, Any]]:
    connection = await asyncpg.connect(dsn=DATABASE_URL)
    try:
        customer = await connection.fetchrow(
            """
            SELECT *
              FROM customer
             WHERE id = $1
            """,
            customer_id,
        )
    except asyncpg.PostgresError:
        print("Failed to get customer data for customer_id %s" % customer_id)
        raise
    finally:
        await connection.close()
    return dict(customer) if customer else None


################################################################################
#
# implement a generic cache mechanism
#  and use it in the `get_customer_details` method
#
################################################################################
from typing import cast, Any, Callable, TypedDict


def memoize(f: Callable) -> Any:
    cache = {}

    async def wrapper(*args):
        if args in cache:
            return cache[args]
        else:
            rv = await f(*args)
            cache[args] = rv
            return rv

    return wrapper


class Customer(TypedDict):
    id: int
    first_name: str
    last_name: str
    street_address: str
    zipcode: str
    city: str


@memoize
async def memo_get_customer_details(customer_id: int) -> Optional[Customer]:
    connection = await asyncpg.connect(dsn=DATABASE_URL)
    try:
        customer = await connection.fetchrow(
            """
            SELECT *
              FROM customer
             WHERE id = $1
            """,
            customer_id,
        )
    except asyncpg.PostgresError:
        print("Failed to get customer data for customer_id %s" % customer_id)
        raise
    finally:
        await connection.close()
    return cast(Customer, dict(customer)) if customer else None


if __name__ == "__main__":
    for i in [1, 2]:
        result = asyncio.run(memo_get_customer_details(i))
        print(result)
