import json
import re
import time
import greenstalk
from loguru import logger
import traceback
import requests
import pyssdb

# Initialize Greenstalk and SSDB clients once
greenstalk_client = greenstalk.Client(
    ("192.168.20.175", 11300), watch="sc_bps_daerah_baselink_new_fix"
)
ssdb_client = pyssdb.Client(
    host="192.168.150.21",
    port=8888,
    max_connections=500,
)

client_detail = greenstalk.Client(
    ("192.168.20.175", 11300), use="sc_bps_daerah_detail_new_fix"
)

cookies = {
    "f5avraaaaaaaaaaaaaaaa_session_": "JJIBFCAPHIDKPICCBJHACEGODKPNBHFPMPGNJBFAJPIDGHOAFKILCJKJLIODBGCONFIDDDGKAGBMGBILNNNADHPGCMPFPFJNFDKOEPOOPGBFNPHBELHGEBPPKODMEFEB",
    "_ga_CLP8Q4CR7J": "GS1.1.1721987499.2.1.1721988896.0.0.0",
    "_ga_MBEH1B2Q1F": "GS1.1.1722074871.2.1.1722074881.0.0.0",
    "_ga_SLZ6H0R0CX": "GS1.1.1722191457.5.0.1722191457.60.0.0",
    "_ga_T7YPSCVK8R": "GS1.1.1722221423.2.0.1722221423.0.0.0",
    "_ga_7FD8073S86": "GS1.1.1722228771.11.1.1722228937.0.0.0",
    "_ga_578WLYVT58": "GS1.1.1722236209.6.1.1722236210.0.0.0",
    "_ga": "GA1.1.984331042.1721381442",
    "asw": '{"lang":"id"}',
    "e437b945144c4368eba347f6b8fbb958": "2446141807173d3ff0517b7bf5f0b03e",
    "f5avraaaaaaaaaaaaaaaa_session_": "CPEKBKBAJJDLPCFNOLNJEJEJJBPBJMOAJAPJNMFMAOICALNIJMFNDJOHJFNOGMBIMEEDKPDDBGKOADAHPECAMHNGCMMFBKPEFELCBAOBOHEOCBHNCKPGEONNDLLJINEM",
    "_ga_XXTTVXWHDB": "GS1.1.1723188762.70.1.1723189036.0.0.0",
    "bpscinfo": "eyJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJCYWRhbiBQdXNhdCBTdGF0aXN0aWsiLCJqdGkiOiJueXJaU3djVm9NVUx0NWRyMmZ5TF8iLCJpYXQiOjE3MjMxODg3MzcsImV4cCI6MTcyMzE5NjIzN30.1LKp_w_DU9aGd40ypkubS-bAoF8iig9P9fft6tgBGWE",
    "TS01395fde": "0167a1c861d50b763a3f97a32caa19c1d993c278471b73dc44ce943f5e8d87f8bb129ba7224b7a70b534ec687f129da158f83333003d8c1cbf092a08cdcc1a028258af497ff50a90732215204f4545ea6deeb38a6f42678aa43f820227527a2797d1f63059",
}


def proses_job(data):
    params = {
        "subject": data["subject"],
    }
    request_data = [
        {
            "locale": "id",
            "keyword": "$undefined",
            "subject": data["subject"],
            "page": data["page"],
            "perpage": 100,
            "sortBy": "date",
            "sortOrder": "desc",
        }
    ]
    request_data_json = json.dumps(request_data)
    beta_url_table = f"{data['link']}/id/statistics-table?subject={params['subject']}"

    headers = {
        "accept": "text/x-component",
        "accept-language": "en-US,en;q=0.9,id;q=0.8",
        "content-type": "text/plain;charset=UTF-8",
        "next-action": "7842359e1d2f27b80750062d8bbe8942fb30abc0",
        "next-router-state-tree": "%5B%22%22%2C%7B%22children%22%3A%5B%5B%22lang%22%2C%22id%22%2C%22d%22%5D%2C%7B%22children%22%3A%5B%22statistics-table%22%2C%7B%22children%22%3A%5B%22__PAGE__%3F%7B%5C%22subject%5C%22%3A%5C%22519%5C%22%7D%22%2C%7B%7D%2C%22%2Fid%2Fstatistics-table%3Fsubject%3D519%22%2C%22refresh%22%5D%7D%2Cnull%2Cnull%2Ctrue%5D%7D%2Cnull%2Cnull%2Ctrue%5D%7D%2Cnull%2Cnull%5D",
        "origin": f"http://{data['link'].split('/')[2]}",
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
        text = re.sub("\n", "", response_text)
        text = re.sub(".*]]1:", "", text)
        items = json.loads(text)
        index = 1



        if "data" in items["response"] and items["response"]["data"] is not None:
            if not items["response"]["data"]:
                raise Exception("posts empty")
            if len(items["response"]["data"]) == 1:
                index = 0
            for item in items["response"]["data"][index]:
                title = item["title"].lower().replace(" ", "-")
                title = re.sub("[^A-z0-9]", "-", title)
                link = (
                    f"{data['link']}/{item['tablesource']}/{item['id']}/{title}.html"
                )

                metadata = {
                    "link": link,
                    "domain": data["link"].split("/")[2],
                    "tag": ["bps", "bps.go.id", "statistics table"],
                    "category": item["subject"],
                    "sub_category": item["subcat"],
                    "title": item["title"],
                }
                exist = ssdb_client.hexists(
                    "{}".format("sc_bps_daerah_links_new_fix"), "{}".format("saljdj")
                )
                exist = exist.decode("utf-8")
                if exist == "0":
                    ssdb_client.hset(
                        "sc_bps_daerah_links_new_fix", item["id"], json.dumps(metadata)
                    )

                    client_detail.put(json.dumps(metadata, indent=2), ttr=3600)
                    print(f"Successfully added {item['id']} to ssdb & beanstalk")
                else:
                    print(f"Already added {item['id']} to ssdb")

            try:
                pages = items["response"]["data"][0]["pages"]

                for page in range(2, pages + 1):
                    metadata_page = {
                        "link": link,
                        "domain": data["link"].split("/")[2],
                        "tag": ["bps", data["link"].split("/")[2], "statistics table"],
                        "page": page,
                        "subject": data["subject"],
                    }
                    client_nextpage = greenstalk.Client(
                        ("192.168.20.175", 11300), use="sc_bps_daerah_list_new"
                    )
                    client_nextpage.put(json.dumps(metadata_page, indent=2), ttr=3600)

                    print(
                        "Successfully added {} to sc_bps_daerah_list page : {}".format(
                            data["subject"], page
                        )
                    )
            except Exception as e:
                print(e)
                print(traceback.format_exc())

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
