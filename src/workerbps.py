import re
import asyncio
import json

from loguru import logger
from typing import Any, Coroutine
from configparser import ConfigParser
from s3fs import S3FileSystem

from playwright.async_api._context_manager import PlaywrightContextManager
from playwright.async_api._generated import Playwright as AsyncPlaywright, Playwright

from src.beanstalk import BeanStalk
from src.handleClick import handleDownload

from src.exceptions import DownloadError, S3Error, TableNotFound

class WorkerBps(PlaywrightContextManager, handleDownload):
    def __init__(self, headfull = True) -> None:
        super().__init__()
        self.headfull = headfull 
        (config := ConfigParser()).read('config.ini')
        self.s3_path = (s3 := config["s3"]).get("s3_path")

        self.s3 = S3FileSystem(
            key=s3.get("key"),
            secret=s3.get("secret"),
            client_kwargs={'endpoint_url': s3.get("endpoint_url")}
        )

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
        self.browser = await (await super().__aenter__()).chromium.launch(headless=self.headfull)
        self.page = await (await self.browser.new_context()).new_page()
        
        while(job := self.beanstalk.watch.reserve()):
            self.data = json.loads(job.body)
            self.link = self.data["link"].replace(".beta","")
            
            await self.page.goto(self.link)
            
            try:
                if (ranges := await self.__check_range()):
                    logger.info("using range parser %s" % self.link)
                    await self.handle_range(ranges)
                    self.beanstalk.watch.delete(job)
                elif await self.__check_dropdown():
                    logger.info("using dropdown parser %s" % self.link)
                    await self.handle_dropdown()
                    self.beanstalk.watch.delete(job)    
                else:
                    try:
                        await self.page.wait_for_selector('#data-table', timeout=2000)
                    except:
                        raise TableNotFound("table not found")
                    logger.info("not use parser %s" % self.link)
                    await self.handle_no_method()
                    self.beanstalk.watch.delete(job)    
            except TableNotFound:
                logger.error("table not fouund ! %s" % self.link)
                self.beanstalk.watch.delete(job)    
            except DownloadError:
                logger.error("download error ! %s" % self.link)
                self.beanstalk.watch.bury(job)    
            except S3Error:
                logger.error("s3 error ! %s" % self.link)
                self.beanstalk.watch.bury(job)
            finally:
                await asyncio.sleep(2)
            
    
    async def __click_button(self, name):
        for btn in await self.page.query_selector_all('button'):
            if name in (await btn.inner_text()).lower():
                await btn.click()

    async def __check_range(self):
        try:
            await self.page.wait_for_selector('.bg-white.rounded-xl.p-4')
            await self.__click_button("tutup")     
        except: ...
        
        try:
            return await self.page.query_selector_all('div[class="flex flex-row gap-2 justify-between"] div[class="max-sm:hidden"] button.duration-75')
        except: ...
    
    async def __check_dropdown(self):
        try:
            await self.page.wait_for_selector('.bg-white.rounded-xl.p-4')
            await self.__click_button("tutup")     
        except: ...

        try:
            return await self.page.query_selector('//html/body/div[2]/div[2]/div[2]/div[1]/div[1]/div[3]/div[1]/div/div')
        except: ...

    async def __aexit__(self, *args: Any) -> None:
        await self.browser.close()
        return await super().__aexit__(*args)

if(__name__ == '__main__'):
    async def main():
        async with WorkerBps(): 
                ...
            
    asyncio.run(main())
