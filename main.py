import asyncio
import click
from src.workerbps import WorkerBps

async def main(**kwargs):
    async with WorkerBps(**kwargs): 
            ...

@click.command()
@click.option('--headfull', is_flag=True, default=True)
def run(**kwargs):
    asyncio.run(main(**kwargs))
        
if(__name__ == '__main__'):
    run()