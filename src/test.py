import re
import asyncio
import json

from loguru import logger
from typing import Any, Coroutine
from configparser import ConfigParser

from playwright.async_api._context_manager import PlaywrightContextManager
from playwright.async_api._generated import Playwright as AsyncPlaywright, Playwright

from src.beanstalk import BeanStalk

class WorkerBps(PlaywrightContextManager):
    def __init__(self) -> None:
        super().__init__()
        (config := ConfigParser()).read('config.ini')

        self.beanstalk: BeanStalk = BeanStalk(
            (beanstalk := config["beanstalk"]).get("host"),
            beanstalk.get("port"),
            beanstalk.get("tube")
        )
    
    @staticmethod
    def clean_string(text):
        cleaned = re.sub(r'[^\w\s-]', '', text.lower())
        cleaned = re.sub(r'\s+', '_', cleaned)
        cleaned = cleaned.replace('/', '_')
        return cleaned.strip('_')

    async def __aenter__(self) -> Coroutine[Any, Any, Playwright]:
        self.browser = await (await super().__aenter__()).chromium.launch(headless=False)
        self.page = await (await self.browser.new_context()).new_page()
        
        while(job := self.beanstalk.watch.reserve()):
            data = json.loads(job.body)
            link = data["link"]
            await self._process_data(link)

            if(ranges := await self.__check_range()):
                await self.__handle_range(ranges)
    
    async def __handle_range(self, ranges):
        for i, range in enumerate(ranges):
            if i: await range.click()

            for btn in await self.page.query_selector_all('button'):
                if 'unduh' in (await btn.inner_text()).lower():
                    await btn.click()

            # await self.page.wait_for_selector("*[role='menuitem']")

            # async with self.page.expect_download(timeout=360000) as download_info:
            #     for btn in await self.page.query_selector_all("*[role='menuitem']"):
            #         if 'xlsx' in (await btn.inner_text()).lower():
            #             await btn.click()
            #     download = await download_info.value

            for btn in await self.page.query_selector_all('button'):
                if 'unduh' in (await btn.inner_text()).lower():             
                    await btn.click()

            await asyncio.sleep(20)
    
    async def __click_button(self, name):
        for btn in await self.page.query_selector_all('button'):
            if name in (await btn.inner_text()).lower():
                await btn.click()

    async def __check_range(self):
        try:
            await self.page.wait_for_selector('.bg-white.rounded-xl.p-4')
            # await self.page.wait_for_selector('div[role="dialog"]', timeout=10000   )   
            await self.__click_button("tutup")     
        except: ...

        try:    
            print('wait range')
            await self.page.wait_for_selector('div[class="flex flex-row gap-2 justify-between"] div[class="max-sm:hidden"] button.duration-75', timeout=10000)
            print('find range')
            return await self.page.query_selector_all('div[class="flex flex-row gap-2 justify-between"] div[class="max-sm:hidden"] button.duration-75')
        except Exception as e:
            logger.error(f"Error finding range buttons: {e}")
            return
    
    async def _process_data(self, url):
        print(url)
        await self.page.goto(url)

    async def __aexit__(self, *args: Any) -> None:
        return await super().__aexit__(*args)

if(__name__ == '__main__'):
    async def main():
        async with WorkerBps(): 
                ...
            
    asyncio.run(main())
