import asyncio
import aiohttp
from more_itertools import chunked

from models import Session, SwapiPeople, close_db, init_db

CHUNK_SIZE = 10


async def insert_people(people_list):
    people_list = [SwapiPeople(json=person) for person in people_list]
    async with Session() as session:
        session.add_all(people_list)
        await session.commit()


async def get_person(person_id):
    session = aiohttp.ClientSession()
    response = await session.get(f"https://swapi.py4e.com/api/people/{person_id}/")
    json_response = await response.json()
    await session.close()
    return json_response


async def main():
    await init_db()

    for person_id_chunk in chunked(range(1, 100), CHUNK_SIZE):
        coros = [get_person(person_id) for person_id in person_id_chunk]
        result = await asyncio.gather(*coros)
        result = [person for person in result if 'details' not in person.keys()]
        for quality in result:
            session = aiohttp.ClientSession()
            homeworld = await session.get(quality.get('homeworld'))
            home_json = await homeworld.json()
            await session.close()
            quality['homeworld'] = home_json['name']
        print(result)
        # asyncio.create_task(insert_people(result))
    tasks = asyncio.all_tasks() - {asyncio.current_task()}
    await asyncio.gather(*tasks)
    await close_db()


if __name__ == "__main__":
    asyncio.run(main())
