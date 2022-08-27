import asyncio
import aiohttp
import aiosqlite

from datetime import datetime
from pprint import pprint
from more_itertools import chunked

MAX_CHUNK = 10

async def get_people(connection_to_db, session, id):
    async with session.get(f'https://swapi.dev/api/people/{id}') as response:
        json_data = await response.json()
        try:
            name = json_data['name']
            films = json_data['films']
            sql_query = f"""
            INSERT INTO people(id, name, films)
            VALUES("{id}", "{name}", "{films}");
            """
            await connection_to_db.execute(sql_query)
            await connection_to_db.commit()
        except Exception as ex:
            print(ex)
        return { 'id': id, 'data': json_data }

start = datetime.now()
async def main():
    async with aiosqlite.connect('people.db') as db:

        await db.execute("""CREATE TABLE IF NOT EXISTS people(
    id INT PRIMARY KEY,
    name TEXT,
    films TEXT);
    """)
        await db.commit()
        
        async with aiohttp.ClientSession() as session:
            coroutines = ( get_people(db, session, i) for i in range(1,83) )
            for coroutines_chunk in chunked(coroutines, MAX_CHUNK):
                await asyncio.gather(*coroutines_chunk)

asyncio.run(main())
print(datetime.now() - start)