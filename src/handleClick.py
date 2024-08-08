import os
import json

from time import time
from loguru import logger
from datetime import datetime
from src.exceptions import S3Error, DownloadError

class handleDownload():
    async def handle_range(self, ranges):
        for i, range in enumerate(ranges):
            name_range = await range.text_content()

            if i: await self.page.get_by_role('button', name=name_range).click()

            await self.page.wait_for_selector('#data-table')
            for btn in await self.page.query_selector_all('button'):
                if 'unduh' in (await btn.inner_text()).lower():
                    await btn.click()            

            await self.page.wait_for_selector("*[role='menuitem']")
            await self._download()

    async def  handle_dropdown(self):
        select = await self.page.query_selector('//html/body/div[2]/div[2]/div[2]/div[1]/div[1]/div[3]/div[1]/div/div[1]')
        await select.click()
        html_drop = await self.page.query_selector('//html/body/div[2]/div[2]/div[2]/div[1]/div[1]/div[3]/div[1]/div/div[2]')
        all_div = await html_drop.query_selector_all('div')
        option = []
        for div in all_div:
            option.append(await div.get_attribute('id'))
        option.pop(0)
        
        for op in option:
            select = await self.page.query_selector('input')
            await select.click()
            await self.page.click(f'#{op}')
            await self.page.wait_for_selector('#data-table')
            for btn in await self.page.query_selector_all('button'):
                if 'unduh' in (await btn.inner_text()).lower():
                    await btn.click()

            await self.page.wait_for_selector("*[role='menuitem']")
            await self._download()

    async def handle_no_method(self):
        await self.page.wait_for_selector('#data-table')
        for btn in await self.page.query_selector_all('button'):
            if 'unduh' in (await btn.inner_text()).lower():
                await btn.click()        

        await self.page.wait_for_selector("*[role='menuitem']")
        await self._download()
    
    async def _download(self):
        async with self.page.expect_download(timeout=360000) as download_info:
            for btn in await self.page.query_selector_all("*[role='menuitem']"):
                if 'xlsx' in (await btn.inner_text()).lower():
                    await btn.click()
            download = await download_info.value
            await download.save_as(local_file_path := 'temp.xlsx')

            if not os.path.exists(local_file_path): 
                logger.warning("Downloaded file does not exist.")

            name_level = self.link.split('/')[2].split('.')[0]
            sub_title_text = await (await self.page.query_selector("//html/body/div[2]/div[2]/div[2]/div[1]/div[1]/div[1]/h1")).inner_text()
            
            (level, name_level, category, sub_category, sub_title) = (
                self.clean_string(string) for string in
                (
                    "kabupaten_kota" if 'kab' in name_level or 'kota' in name_level else "provinsi", 
                    name_level,
                    self.data['category'],
                    self.data['sub_category'],
                    sub_title_text
                )
            )

            (s3_file_xlsx, s3_file_json) = (
                self.s3_path % (f'{level}/{name_level}/{category}/{sub_category}/{format}/{sub_title}.{format}') for format in ('xlsx', 'json')
            )

            if self.s3.exists(s3_file_xlsx) and self.s3.exists(s3_file_json):
                logger.warning(f"File already exists in S3")
                return

            try:
                with open(local_file_path, 'rb') as local_file:
                    with self.s3.open(s3_file_xlsx, 'wb') as s3_file:
                        s3_file.write(local_file.read())

                    logger.success(f"Uploaded file to S3: {s3_file_xlsx}")
                        
            except Exception as e:
                raise S3Error("failed push to s3 xlsx")

            os.remove(local_file_path)

            metadata = {
                "link": self.data["link"],
                "domain": "bps.go.id",
                "tag": [
                    "bps",
                    "bps.go.id",
                    "statistics table",
                ],
                "title": self.data['title'],
                "sub_title": sub_title_text,
                'update': (await updt.inner_text()).split(':')[-1].strip() if (updt := await self.page.query_selector("//html/body/div[2]/div[2]/div[2]/div[1]/div[1]/div[1]/p")) else None,
                'desc': (await desc.inner_text()).strip() if (desc := await self.page.query_selector('//html/body/div[2]/div[2]/div[2]/div[1]/div[3]/div')) else None,
                'category': self.data['category'],
                'sub_category': self.data['sub_category'],
                'path_data_raw': [
                    s3_file_xlsx,
                    s3_file_json
                ],
                'file_name': [
                    file.split("/")[-1] for file in (s3_file_xlsx, s3_file_json)
                ],
                'crawling_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'crawling_time_epoch': int(time())
            }

            try:
                with self.s3.open(s3_file_json, "w") as f:
                    json.dump(metadata, f)                                  
                logger.success(f"Wrote metadata to S3: {s3_file_json}")
            except Exception as e:
                raise S3Error("failed push to s3 json")