import json
import re
import time
import greenstalk
from loguru import logger
import traceback
import requests
import pyssdb

# Initialize Greenstalk and SSDB clients once
greenstalk_client = greenstalk.Client(('192.168.20.175', 11300), watch='sc_bps_daerah_list_new_fix')
ssdb_client = pyssdb.Client(
    host="192.168.150.21",
    port=8888,
    max_connections=500,
)

client = greenstalk.Client(('192.168.20.175', 11300), use='sc_bps_daerah_detail_new_fix')

cookies = {
    'f5avraaaaaaaaaaaaaaaa_session_': 'KLDJINDNACFMAOBCGKFLFPPCCCGBJBOLIPGPIOLOMJENGFGPPAIAACGFOKLAGBAFJPNDACLMDPICPOCMEJEAABELCEGENKAIOFFDBGJMHJEPNGJABIIOIEFCKFKOCEAO',
    '_ga_CLP8Q4CR7J': 'GS1.1.1721987499.2.1.1721988896.0.0.0',
    '_ga_MBEH1B2Q1F': 'GS1.1.1722074871.2.1.1722074881.0.0.0',
    '_ga_SLZ6H0R0CX': 'GS1.1.1722191457.5.0.1722191457.60.0.0',
    '_ga_T7YPSCVK8R': 'GS1.1.1722221423.2.0.1722221423.0.0.0',
    '_ga_7FD8073S86': 'GS1.1.1722228771.11.1.1722228937.0.0.0',
    '_ga_578WLYVT58': 'GS1.1.1722236209.6.1.1722236210.0.0.0',
    '_ga': 'GA1.1.984331042.1721381442',
    '49852857002cb53e882944e8ff39fb93': '77e1cc13d9d4433c9bd4dcc69d4b2583',
    'asw': '{"lang":"id"}',
    'bpscinfo': 'eyJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJCYWRhbiBQdXNhdCBTdGF0aXN0aWsiLCJqdGkiOiJmank1dmVFTEVvR1hUYzdKRU51MkIiLCJpYXQiOjE3MjMwMjcwNDQsImV4cCI6MTcyMzAzNDU0NH0.OqSUE1fRAoG7guIhn58hFumE6GogUgB4E3HTVcSRASE',
    'TS01395fde': '0167a1c86112f502c1297b01713fbc1eca7b2f7871ca065e8597d13d4af5b2a8252eea8fbea9f97dbd93ac153830ebddfd619d4c63a2e22e51ded4c88778008bdad0f73f84786fea5bc6709945d581653b88e85650109976e145e669e724c3ff26e5d03789',
}

def proses_job(data):
    params = {
        "subject": f"{data['params']}",
    }
    request_data = [{"locale": "id", "keyword": "$undefined", "subject": data["params"], "page": data["page"], "perpage": 100, "sortBy": "date", "sortOrder": "desc"}]
    request_data_json = json.dumps(request_data)

    beta_url_table = f"{data['link'].split('/')[2]}/id/statistics-table"

    headers = {
        "accept": "text/x-component",
        "accept-language": "en-US,en;q=0.9,id;q=0.8",
        "content-type": "text/plain;charset=UTF-8",
        "next-action": "7842359e1d2f27b80750062d8bbe8942fb30abc0",
        "next-router-state-tree": "%5B%22%22%2C%7B%22children%22%3A%5B%5B%22lang%22%2C%22id%22%2C%22d%22%5D%2C%7B%22children%22%3A%5B%22statistics-table%22%2C%7B%22children%22%3A%5B%22__PAGE__%3F%7B%5C%22subject%5C%22%3A%5C%22519%5C%22%7D%22%2C%7B%7D%2C%22%2Fid%2Fstatistics-table%3Fsubject%3D519%22%2C%22refresh%22%5D%7D%2Cnull%2Cnull%2Ctrue%5D%7D%2Cnull%2Cnull%2Ctrue%5D%7D%2Cnull%2Cnull%5D",
        "origin": data['link'].split('/')[2],
        "priority": "u=1, i",
        "referer": beta_url_table,
        "sec-ch-ua": '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    }
    
    try:
        response = requests.post(
            beta_url_table,
            params=params,
            cookies=cookies,
            headers=headers,
            data=request_data_json,
        )

        response_text = response.content.decode("utf-8")
        text = re.sub('\n','', response_text)
        text = re.sub('.*]]1:','', text)

        items = json.loads(text)

        for item in items["response"]["data"][1]:
            title = item["title"].lower().replace(" ", "-")
            title = re.sub("[^A-z0-9]", "-", title)
            link = f"{beta_url_table}/{item['tablesource']}/{item['id']}/{title}.html"
            
            metadata = {
                "link": link,
                "domain": data['link'].split("/")[2],
                "tag": ["bps", "bps.go.id", "statistics table"],
                "category": item["subject"],
                "sub_category": item["subcat"],
                "title": item["title"],
            }

            exist = ssdb_client.hexists("{}".format('sc_bps_daerah_links_new_fix'), "{}".format(item['id']))
            exist = exist.decode("utf-8")
            if exist == "0":
                hset = ssdb_client.hset(
                        'sc_bps_daerah_links_new_fix', 
                        item['id'], 
                        json.dumps(metadata)
                    )
                print(f"Successfully added {item['id']} to ssdb")
                
                if hset:
                    client.put(json.dumps(metadata, indent=2), ttr=3600)
            else:
                print(f"Already added {item['id']} to ssdb")

        return True
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        print(traceback.format_exc())
        return False

def main():
    while True:
        job = None
        try:
            job = greenstalk_client.reserve(1)
            data = json.loads(job.body)
            success = proses_job(data)

            if success:
                greenstalk_client.delete(job)
                logger.success(f"Job {job.id} processed successfully and deleted.")
            else:
                greenstalk_client.bury(job)
                logger.error(f"Job {job.id} failed and buried.")
        except greenstalk.NotFoundError:
            if job:
                logger.error(f"Job {job.id} not found.")
                greenstalk_client.bury(job)
        except greenstalk.TimedOutError:
            logger.error("Timed out while reserving a job.")
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            if job:
                greenstalk_client.bury(job)
        time.sleep(1)

if __name__ == "__main__":
    main()
