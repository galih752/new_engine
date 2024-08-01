import json
import re
from playwright.async_api import async_playwright
import asyncio
import os
import greenstalk
import logging
import s3fs
import traceback
from datetime import datetime

# Setup logging
# logging.basicConfig(filename='log_error.log', level=print, 
#                     format='%(asctime)s %(levelname)s %(message)s')

# Setup Beanstalkd client
client = greenstalk.Client(('192.168.150.21', 11300), watch='link_statistic_table')

# Setup S3 filesystem
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
    try:
        if target_page <= 5:
            await page.get_by_role("button", name=f"{target_page}").click()
        else:
            await page.get_by_role("button", name="5").click()
            for p in range(6, target_page + 1):
                await page.get_by_role("button", name=f"{p}").click()
    except Exception as e:
        print(f"Error navigating to page {target_page}: {e}")

async def process_job(data):
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()

            await page.goto(data['url'], timeout=120000)
            print(f"Processing {data['url']}...")

            try:
                await page.get_by_role("button", name="Tutup").click()
            except Exception as e:
                print(f"Error closing modal: {e}")

            await page.wait_for_selector('//html/body/div[2]/div[2]/div[2]/div[2]/div[2]/div[2]/div[2]/div/div[2]/div[1]/div[1]/div/table/tbody/tr[1]/td[3]', timeout=60000)
            data_table = await page.query_selector('//html/body/div[2]/div[2]/div[2]/div[2]/div[2]/div[2]/div[2]/div/div[2]/div[1]/div[1]/div/table/tbody/tr[1]/td[2]/div')
            if data_table:
                for current_page in range(1, 12):
                    await navigate_to_page(page, current_page)

                    for n in range(1, 21):

                        await page.wait_for_selector(f"//html/body/div[2]/div[2]/div[2]/div[2]/div[2]/div[2]/div[2]/div/div[2]/div[1]/div[1]/div/table/tbody/tr[1]/td[3]", timeout=60000)
                            
                        title = await page.query_selector(f'//html/body/div[2]/div[2]/div[2]/div[2]/div[2]/div[2]/div[2]/div/div[2]/div[1]/div[1]/div/table/tbody/tr[{n}]/td[2]/div')
                        title_text = await title.inner_text() if title else ''

                        dates = await page.query_selector(f'//html/body/div[2]/div[2]/div[2]/div[2]/div[2]/div[2]/div[2]/div/div[2]/div[1]/div[1]/div/table/tbody/tr[{n}]/td[3]')
                        date = await dates.inner_text() if dates else ''

                        tanggal = await page.query_selector(f'//html/body/div[2]/div[2]/div[2]/div[2]/div[2]/div[2]/div[2]/div/div[2]/div[1]/div[1]/div/table/tbody/tr[{n}]/td[3]')
                        
                        if tanggal:
                            await tanggal.click()

                        try:
                            await page.wait_for_selector('//html/body/div[2]/div[2]/div[2]/div[1]/div[1]/div[1]/h1', timeout=60000)
                            sub_title = await page.query_selector('//html/body/div[2]/div[2]/div[2]/div[1]/div[1]/div[1]/h1')
                            sub_title_text = await sub_title.inner_text() if sub_title else ''
                            
                            url_data = page.url

                            desc = await page.query_selector('//html/body/div[2]/div[2]/div[2]/div[1]/div[3]/div')
                            description = await desc.inner_text() if desc else ''
                            
                            metadata = {
                                "link": url_data,
                                "domain": "bps.go.id",
                                "tag": ["bps", "bps.go.id", "statistics table"],
                                "title": title_text,
                                'update': await tanggal.inner_text() if tanggal else '',
                                'desc': description,
                                'category': data['category'],
                                'sub_category': data['subcategory'],
                            }

                            # Print metadata
                            print(json.dumps(metadata))

                            client = greenstalk.Client(('192.168.150.21', 11300), use='link_data_bps_pusat')
                            client.put(json.dumps(metadata, indent=2), ttr=3600)

                            # Optionally upload metadata to S3
                            # file_json = f"{clean_string(sub_title_text)}.json"
                            # path_json = f"s3://ai-pipeline-raw-data/data/data_statistics/bps/pusat/{data['category'].replace(' ', '_').lower()}/{data['subcategory'].replace(' ', '_').lower()}/json/{file_json}"
                            # with s3.open(path_json, "w") as f:
                            #     json.dump(metadata, f)

                            print("-------------------------------------------")
                            print(f'Bagian ke : {n}, Pada halaman ke : {current_page}')
                            print("-------------------------------------------")

                            await page.goto(data['url'])
                            await navigate_to_page(page, current_page)
                            await asyncio.sleep(1)
                        except Exception as e:
                            print(traceback.format_exc())
                            print(f"Error processing item {n} on page {current_page}: {e}")

            await page.close()
            await browser.close()
            return True
    except Exception as e:
        print(f"Error processing job: {e}")
        return False

async def main():
    while True:
        try:
            job = client.reserve(timeout=10)
            if job is None:
                logging.info("No more jobs in the queue.")
                break

            data = json.loads(job.body)
            success = await process_job(data)

            if success:
                client.delete(job)
                logging.info(f"Job {job.id} processed successfully and deleted.")
            else:
                client.release(job)
                logging.info(f"Job {job.id} failed and released back to the queue.")

        except greenstalk.TimedOutError:
            logging.info("No job available, waiting...")
            await asyncio.sleep(3)
        except json.JSONDecodeError:
            print("Error decoding JSON from job.")
            client.release(job)  # Releasing job in case of JSON error
            await asyncio.sleep(3)
        except Exception as e:
            print(f"Unexpected error: {e}")
            await asyncio.sleep(3)

if __name__ == "__main__":
    asyncio.run(main())