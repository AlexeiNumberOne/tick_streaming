from sqlalchemy import Table
from asynch import Connection


async def insert_json_rows(conn: Connection, table: Table, data):
    async with conn.cursor() as cursor:
        await cursor.execute(f"INSERT INTO {table.name} FORMAT JSONEachRow", args=data)
