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

DRIVER_NAME = "postgresql"
USERNAME = "postgres"
PASSWORD = USERNAME
HOST = "127.0.0.1"
PORT = 5436
DATABASE = "airbnb"
DATABASE_URL = f"{DRIVER_NAME}://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"


async def get_customer_details(customer_id: int) -> Optional[Dict[str, Any]]:
    connection = await asyncpg.connect(dsn=DATABASE_URL)
    print("fetching customer %d from database" % customer_id)
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
    postal_code: str
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


from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import registry
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

mapper_registry = registry()
Base = mapper_registry.generate_base()

url = URL.create(
    drivername=DRIVER_NAME,
    username=USERNAME,
    password=PASSWORD,
    host=HOST,
    port=PORT,
    database=DATABASE,
)

engine = create_engine(url)
Session = sessionmaker(bind=engine)


class DbCustomer(Base):
    __tablename__ = "customer"
    #  first_name     | character varying(24)  |           |          |
    #  last_name      | character varying(24)  |           |          |
    #  street_address | character varying(256) |           |          |
    #  postal_code    | character varying(16)  |           |          |
    #  city           | character varying(24)  |           |          |
    #
    id = Column(Integer(), primary_key=True)
    first_name = Column(String(24), nullable=False)
    last_name = Column(String(24), nullable=False)
    street_address = Column(String(256))
    postal_code = Column(String(16))
    city = Column(String(24))

    def __init__(
        self,
        customer_id: int,
        first_name: str,
        last_name: str,
        street_address: str,
        postal_code: str,
        city: str,
    ) -> None:
        super().__init__()
        self.customer_id = customer_id
        self.first_name = first_name
        self.last_name = last_name
        self.street_address = street_address
        self.postal_code = postal_code
        self.city = city


@memoize
async def memo_orm_get_customer(customer_id: int) -> Optional[Customer]:
    print("fetching ORM customer %d from database" % customer_id)
    session = Session()
    customer = session.query(DbCustomer).where(customer_id == customer_id).first()
    session.close()
    return customer


if __name__ == "__main__":
    arr = [1, 2]
    for l in arr:
        for i in arr:
            result = asyncio.run(memo_get_customer_details(i))
            print(result)
            result = asyncio.run(memo_orm_get_customer(i))
            print(result)
