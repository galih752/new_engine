import asyncio
from loguru import logger
import os

class handleDownload():
    def __init__(self) -> None:
        self.page = None

    async def handle_range(self, ranges, pages):
        for i, range in enumerate(ranges):
            name_range = await range.text_content()

            if i: await pages.get_by_role('button', name=name_range).click()

            await pages.wait_for_selector('#data-table')
            for btn in await pages.query_selector_all('button'):
                if 'unduh' in (await btn.inner_text()).lower():
                    await btn.click()            

            await pages.wait_for_selector("*[role='menuitem']")
            await self._download(pages)

    async def  handle_dropdown(self, pages):
        select = await pages.query_selector('//html/body/div[2]/div[2]/div[2]/div[1]/div[1]/div[3]/div[1]/div/div[1]')
        await select.click()
        html_drop = await pages.query_selector('//html/body/div[2]/div[2]/div[2]/div[1]/div[1]/div[3]/div[1]/div/div[2]')
        all_div = await html_drop.query_selector_all('div')
        option = []
        for div in all_div:
            option.append(await div.get_attribute('id'))
        option.pop(0)
        
        for op in option:
            select = await pages.query_selector('input')
            await select.click()
            await pages.click(f'#{op}')
            await pages.wait_for_selector('#data-table')
            for btn in await pages.query_selector_all('button'):
                if 'unduh' in (await btn.inner_text()).lower():
                    await btn.click()

            await pages.wait_for_selector("*[role='menuitem']")
            await self._download(pages)

    async def handle_no_method(self, pages):
        await pages.wait_for_selector('#data-table')
        for btn in await pages.query_selector_all('button'):
            if 'unduh' in (await btn.inner_text()).lower():
                await btn.click()        

        await pages.wait_for_selector("*[role='menuitem']")
        await self._download(pages)
    
    async def _download(self, pages):
        async with pages.expect_download(timeout=360000) as download_info:
            for btn in await pages.query_selector_all("*[role='menuitem']"):
                if 'xlsx' in (await btn.inner_text()).lower():
                    await btn.click()
            download = await download_info.value

