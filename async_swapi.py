import asyncio
import aiohttp
from pprint import pprint
from more_itertools import chunked

from models import Session, SwapiPeople, close_db, init_db

CHUNK_SIZE = 10


async def insert_people(people_list):
    people_list = [SwapiPeople(birth_year=person['birth_year'],
    eye_color=person['eye_color'],
    films=person['films'],
    gender=person['gender'],
    hair_color=person['hair_color'],
    height=person['height'],
    homeworld=person['homeworld'],
    mass=person['mass'],
    name=person['name'],
    skin_color=person['skin_color'],
    species=person['species'],
    starships=person['starships'],
    vehicles=person['vehicles']) for person in people_list]
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
        result = [person for person in result if 'Not found' not in person.values()]
        for person in result:
            session = aiohttp.ClientSession()
            homeworld = await session.get(person.get('homeworld'))
            home_json = await homeworld.json()
            for param in ['species', 'starships', 'vehicles', 'films', 'homeworld']:
                qualities = []
                for quality in person.get(param):
                    quality_name = await session.get(quality)
                    quality_json = await quality_name.json()
                    if param == 'films':
                        qualities.append(quality_json['title'])
                    else:
                        qualities.append(quality_json['name'])
                person[param] = ', '.join(qualities)
            person['homeworld'] = home_json['name']
            await session.close()
        asyncio.create_task(insert_people(result))
    await close_db()


if __name__ == "__main__":
    asyncio.run(main())
