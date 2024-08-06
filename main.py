import json
import re

from loguru import logger
from playwright.async_api import async_playwright
import asyncio
import time
import os
import greenstalk
import s3fs
from datetime import datetime

client = greenstalk.Client(('192.168.150.21', 11300), watch='link_data_bps_pusat')

s3 = s3fs.S3FileSystem(
    key='GLZG2JTWDFFSCQVE7TSQ',
    secret='VjTXOpbhGvYjDJDAt2PNgbxPKjYA4p4B7Btmm4Tw',
    client_kwargs={'endpoint_url': 'http://10.12.1.149:8000'}
)

def clean_string(text):
    cleaned = re.sub(r'[^\w\s-]', '', text.lower())
    cleaned = re.sub(r'\s+', '_', cleaned)
    cleaned = cleaned.replace('/', '_')
    return cleaned.strip('_')

async def navigate_to_page(page, target_page):
    if target_page <= 5:
        await page.get_by_role("button", name=f"{target_page}").click()
    else:
        await page.get_by_role("button", name="5").click()
        for p in range(6, target_page + 1):
            await page.get_by_role("button", name=f"{p}").click()


async def process_job(data):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        await page.goto(data['link'], timeout=360000)

        try:
            await page.get_by_role("button", name="Tutup").click()
        except Exception as e:
            logger.error(f"Error closing modal: {e}")

        try:
            # Check for range buttons
            await page.wait_for_selector('div[class="flex flex-row gap-2 justify-between"] div[class="max-sm:hidden"] button.duration-75', timeout=10000)
            ranges = await page.query_selector_all('div[class="flex flex-row gap-2 justify-between"] div[class="max-sm:hidden"] button.duration-75')
        except Exception as e:
            logger.error(f"Error finding range buttons: {e}")
            ranges = []

        if ranges:
            for range in ranges[1:]:
                try:
                    await page.click('h1,text-main-primary')
                    await asyncio.sleep(0.3)
                    await range.click()
                    await asyncio.sleep(3)
                except Exception as e:
                    logger.error(f"Error clicking range button: {e}")

                sub_title = await page.query_selector('//html/body/div[2]/div[2]/div[2]/div[1]/div[1]/div[1]/h1')
                sub_title_text = await sub_title.inner_text()

                desc = await page.query_selector('//html/body/div[2]/div[2]/div[2]/div[1]/div[3]/div')
                description = ''
                if desc:
                    description = await desc.inner_text()

                await page.get_by_role("button", name="Unduh").click()

                # Wait for the download to start
                async with page.expect_download() as download_info:
                    await page.get_by_role("menuitem", name="XLSX").click(modifiers=["Alt"])
                download = await download_info.value

                await asyncio.sleep(3)

                # Save the downloaded file locally
                local_file_path = os.path.join(os.getcwd(), 'temp_download2023.xlsx')
                await download.save_as(local_file_path)

                if not os.path.exists(local_file_path):
                    logger.info("Downloaded file does not exist.")
                    await page.close()
                    await browser.close()
                    return False  # Indicate failure

                # Create the S3 file path
                file_name = f"{clean_string(sub_title_text)}.xlsx"
                s3_file_path = f"s3://ai-pipeline-raw-data/data/data_statistics/bps/pusat/crawl20240806/{data['category'].replace(' ','_').lower()}/{data['sub_category'].replace(' ','_').lower()}/xlsx/{file_name}"

                file_json = f"{clean_string(sub_title_text)}.json"
                path_json = f"s3://ai-pipeline-raw-data/data/data_statistics/bps/pusat/crawl20240806/{data['category'].replace(' ','_').lower()}/{data['sub_category'].replace(' ','_').lower()}/json/{file_json}"

                if s3.exists(s3_file_path) and s3.exists(path_json):
                    logger.warning(f"File already exists in S3: {s3_file_path}")
                    continue

                # Upload the file to S3
                try:
                    with open(local_file_path, 'rb') as local_file:
                        with s3.open(s3_file_path, 'wb') as s3_file:
                            s3_file.write(local_file.read())
                        logger.success(f"Uploaded file to S3: {s3_file_path}")
                except Exception as e:
                    logger.info(f"Error uploading file to S3: {e}")
                    await page.close()
                    await browser.close()
                    return False  # Indicate failure

                # Remove the local temporary file
                os.remove(local_file_path)

                metadata = {
                    "link": data["link"],
                    "domain": "bps.go.id",
                    "tag": [
                        "bps",
                        "bps.go.id",
                        "statistics table",
                    ],
                    "title": data['title'].replace('.xlsx',''),
                    "sub_title": sub_title_text,
                    'update': data['update'],
                    'desc': description,
                    'category': data['category'],
                    'sub_category': data['sub_category'],
                    'path_data_raw': [
                        s3_file_path,
                        path_json
                    ],
                    'file_name': [
                        file_name,
                        file_json  
                    ],
                    'crawling_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'crawling_time_epoch': int(time.time())
                }

                # Write metadata to S3
                try:
                    with s3.open(path_json, "w") as f:
                        json.dump(metadata, f)

                    logger.success(f"Written metadata to S3: {path_json}")
                except Exception as e:
                    logger.error(f"Error writing metadata to S3: {e}")

                logger.debug(json.dumps(metadata))

        else:
            try:
                optionnya = []
                await page.wait_for_selector('//html/body/div[2]/div[2]/div[2]/div[1]/div[1]/div[3]/div[1]/div')
                select = await page.query_selector('//html/body/div[2]/div[2]/div[2]/div[1]/div[1]/div[3]/div[1]/div')
                if select:
                    await select.click()
                    await asyncio.sleep(2)
                    logger.debug("Clicked on the dropdown.")
                    option = await page.query_selector('div.css-8aqfg3-menu')
                    if option:
                        option_text = await option.inner_text()
                        option_list = option_text.split('\n')
                        optionnya.append(option_list)
                        logger.debug(f"Dropdown options: {option_list}")
                    else:
                        logger.debug("Dropdown options menu not found.")
                        return False  # Indicate failure if dropdown options are not found
                else:
                    logger.warning("Dropdown selector not found.")
                    return False  # Indicate failure if dropdown selector is not found

                for option_bps in optionnya:
                    for option in option_bps:
                        option_bpsnya = ''
                        if '-' in option:
                            option_bpsnya = f"-{option.split('-')[1].strip()}"
                        else:
                            option_bpsnya = option
                        try:
                            await page.locator(".css-19bb58m").click()
                            await page.get_by_role("option", name=f"{option_bpsnya}").click()
                            logger.debug(f"Selected option: {option_bpsnya}")
                            await asyncio.sleep(5)

                            sub_title = await page.query_selector("//html/body/div[2]/div[2]/div[2]/div[1]/div[1]/div[1]/h1")
                            sub_title_text = await sub_title.inner_text()

                            desc = await page.query_selector('//html/body/div[2]/div[2]/div[2]/div[1]/div[3]/div')
                            description = ''
                            if desc:
                                description = await desc.inner_text()

                            # Proceed with the download logic regardless of the above attempts
                            await page.get_by_role("button", name="Unduh").click()

                            # Wait for the download to start
                            async with page.expect_download() as download_info:
                                await page.get_by_role("menuitem", name="XLSX").click(modifiers=["Alt"])
                            download = await download_info.value

                            await asyncio.sleep(3)

                            # Save the downloaded file locally
                            local_file_path = os.path.join(os.getcwd(), 'temp_download2023.xlsx')
                            await download.save_as(local_file_path)

                            if not os.path.exists(local_file_path):
                                logger.warning("Downloaded file does not exist.")
                                await page.close()
                                await browser.close()
                                return False  # Indicate failure

                            # Create the S3 file path
                            file_name = f"{clean_string(sub_title_text)}.xlsx"
                            s3_file_path = f"s3://ai-pipeline-raw-data/data/data_statistics/bps/pusat/crawl20240806/{data['category'].replace(' ','_').lower()}/{data['sub_category'].replace(' ','_').lower()}/xlsx/{file_name}"

                            file_json = f"{clean_string(sub_title_text)}.json"
                            path_json = f"s3://ai-pipeline-raw-data/data/data_statistics/bps/pusat/crawl20240806/{data['category'].replace(' ','_').lower()}/{data['sub_category'].replace(' ','_').lower()}/json/{file_json}"

                            if s3.exists(s3_file_path) and s3.exists(path_json):
                                logger.warning(f"File already exists in S3: {s3_file_path}")
                                continue

                            # Upload the file to S3
                            try:
                                with open(local_file_path, 'rb') as local_file:
                                    with s3.open(s3_file_path, 'wb') as s3_file:
                                        s3_file.write(local_file.read())

                                    logger.success(f"Uploaded file to S3: {s3_file_path}")
                                        
                            except Exception as e:
                                logger.error(f"Error uploading file to S3: {e}")
                                await page.close()
                                await browser.close()
                                return False  # Indicate failure

                            # Remove the local temporary file
                            os.remove(local_file_path)

                            metadata = {
                                "link": data["link"],
                                "domain": "bps.go.id",
                                "tag": [
                                    "bps",
                                    "bps.go.id",
                                    "statistics table",
                                ],
                                "title": data['title'].replace('.xlsx',''),
                                "sub_title": sub_title_text,
                                'update': data['update'],
                                'desc': description,
                                'category': data['category'],
                                'sub_category': data['sub_category'],
                                'path_data_raw': [
                                    s3_file_path,
                                    path_json
                                ],
                                'file_name': [
                                    file_name,
                                    file_json  
                                ],
                                'crawling_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                'crawling_time_epoch': int(time.time())
                            }

                            # Write metadata to S3
                            try:
                                with s3.open(path_json, "w") as f:
                                    json.dump(metadata, f)
                                logger.success(f"Wrote metadata to S3: {path_json}")
                            except Exception as e:
                                logger.error(f"Error writing metadata to S3: {e}")

                            logger.debug(json.dumps(metadata))

                        except Exception as e:
                            logger.error(f"Error during the download process: {e}")
            except Exception as e:
                logger.error(f"Error interacting with dropdown: {e}")

        sub_title = await page.query_selector("//html/body/div[2]/div[2]/div[2]/div[1]/div[1]/div[1]/h1")
        sub_title_text = await sub_title.inner_text()

        desc = await page.query_selector('//html/body/div[2]/div[2]/div[2]/div[1]/div[3]/div')
        description = ''
        if desc:
            description = await desc.inner_text()

        # Download file directly if no dropdown or range is available
        await page.get_by_role("button", name="Unduh").click()

        # Wait for the download to start
        async with page.expect_download() as download_info:
            await page.get_by_role("menuitem", name="XLSX").click(modifiers=["Alt"])
        download = await download_info.value

        await asyncio.sleep(3)

        # Save the downloaded file locally
        local_file_path = os.path.join(os.getcwd(), 'temp_download2023.xlsx')
        await download.save_as(local_file_path)

        if not os.path.exists(local_file_path):
            logger.warning("Downloaded file does not exist.")
            return False 
        
        file_name = f"{clean_string(sub_title_text)}.xlsx"
        s3_file_path = f"s3://ai-pipeline-raw-data/data/data_statistics/bps/pusat/crawl20240806/{data['category'].replace(' ','_').lower()}/{data['sub_category'].replace(' ','_').lower()}/xlsx/{file_name}"

        file_json = f"{clean_string(sub_title_text)}.json"
        path_json = f"s3://ai-pipeline-raw-data/data/data_statistics/bps/pusat/crawl20240806/{data['category'].replace(' ','_').lower()}/{data['sub_category'].replace(' ','_').lower()}/json/{file_json}"

        if s3.exists(s3_file_path) and s3.exists(path_json):
            logger.warning(f"File already exists in S3: {s3_file_path}")
            return True

        try:
            with open(local_file_path, 'rb') as local_file:
                with s3.open(s3_file_path, 'wb') as s3_file:
                    s3_file.write(local_file.read())

                logger.success(f"Uploaded file to S3: {s3_file_path}")

        except Exception as e:
            logger.error(f"Error uploading file to S3: {e}")
            return False 
        
        os.remove(local_file_path)

        metadata = {
            "link": data["link"],
            "domain": "bps.go.id",
            "tag": [
                "bps",
                "bps.go.id",
                "statistics table",
            ],
            "title": data['title'].replace('.xlsx',''),
            "sub_title": sub_title_text,
            'update': data['update'],
            'desc': data['desc'],
            'category': data['category'],
            'sub_category': data['sub_category'],
            'path_data_raw': [
                s3_file_path,
                path_json
            ],
            'file_name': [
                file_name,
                file_json  
            ],
            'crawling_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'crawling_time_epoch': int(time.time())
        }

        # Write metadata to S3
        try:
            with s3.open(path_json, "w") as f:
                json.dump(metadata, f)
            logger.success(f"Wrote metadata to S3: {path_json}")
        except Exception as e:
            logger.error(f"Error writing metadata to S3: {e}")
            return False  # Indicate failure

        logger.debug(json.dumps(metadata))
        await page.close()
        await browser.close()
        return True  # Indicate success


async def main():
    while True:
        job = None
        try:
            job = client.reserve(1)

            data = json.loads(job.body)
            success = await process_job(data)

            if success:
                client.delete(job)
                logger.success(f"Job {job.id} processed successfully and deleted.")
            else:
                client.delete(job)
                logger.error(f"Job {job.id} failed and deleted.")
        except greenstalk.NotFoundError:
            logger.error("Job already released")
        except greenstalk.TimedOutError:
            logger.debug("No job available, waiting...")
            await asyncio.sleep(3)
        except json.JSONDecodeError:
            logger.error("Error decoding JSON from job.")
            if job:
                client.delete(job)
            await asyncio.sleep(3)
        except Exception as e:
            logger.error(e)
            try:
                if job:
                    client.bury(job)
            except Exception:
                pass

if __name__ == "__main__":
    asyncio.run(main())
