
import json
import re
import time
import greenstalk
from loguru import logger
import requests
import pyssdb

client = greenstalk.Client(('192.168.150.21', 11300), watch='sc_bps_daerah_list')


cookies = {
    'f5avraaaaaaaaaaaaaaaa_session_': 'MHPOMABOJKODPMPABABHBHDKAIMFCNFHFPGGJKGFAGNFKKBIDPLEOKKDGBCMPMIKCDDDMHBBKOMAILJIFFBAMHIFMENILOJMCPFJGJIIPDEKLONCMGFECIHKHGCPBLDM',
    '_ga_CLP8Q4CR7J': 'GS1.1.1721987499.2.1.1721988896.0.0.0',
    '_ga_MBEH1B2Q1F': 'GS1.1.1722074871.2.1.1722074881.0.0.0',
    '_ga_SLZ6H0R0CX': 'GS1.1.1722191457.5.0.1722191457.60.0.0',
    '_ga_T7YPSCVK8R': 'GS1.1.1722221423.2.0.1722221423.0.0.0',
    '_ga_7FD8073S86': 'GS1.1.1722228771.11.1.1722228937.0.0.0',
    '_ga_578WLYVT58': 'GS1.1.1722236209.6.1.1722236210.0.0.0',
    '_ga': 'GA1.1.984331042.1721381442',
    '49852857002cb53e882944e8ff39fb93': '77e1cc13d9d4433c9bd4dcc69d4b2583',
    'asw': '{"lang":"id"}',
    'f5avraaaaaaaaaaaaaaaa_session_': 'KLDJINDNACFMAOBCGKFLFPPCCCGBJBOLIPGPIOLOMJENGFGPPAIAACGFOKLAGBAFJPNDACLMDPICPOCMEJEAABELCEGENKAIOFFDBGJMHJEPNGJABIIOIEFCKFKOCEAO',
    '_ga_XXTTVXWHDB': 'GS1.1.1723021614.64.1.1723027340.0.0.0',
    'bpscinfo': 'eyJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJCYWRhbiBQdXNhdCBTdGF0aXN0aWsiLCJqdGkiOiJmank1dmVFTEVvR1hUYzdKRU51MkIiLCJpYXQiOjE3MjMwMjcwNDQsImV4cCI6MTcyMzAzNDU0NH0.OqSUE1fRAoG7guIhn58hFumE6GogUgB4E3HTVcSRASE',
    'TS01395fde': '0167a1c86112f502c1297b01713fbc1eca7b2f7871ca065e8597d13d4af5b2a8252eea8fbea9f97dbd93ac153830ebddfd619d4c63a2e22e51ded4c88778008bdad0f73f84786fea5bc6709945d581653b88e85650109976e145e669e724c3ff26e5d03789',
}

def proses_job(data):
    try:
 
        urls_beta = [
            "https://jakarta.beta.bps.go.id",
            "https://jabar.beta.bps.go.id",
            "https://jateng.beta.bps.go.id",
            "https://yogyakarta.beta.bps.go.id",
            "https://jatim.beta.bps.go.id",
            "https://banten.beta.bps.go.id",
            "https://jakselkota.beta.bps.go.id",
            "https://jaktimkota.beta.bps.go.id",
            "https://jakpuskota.beta.bps.go.id",
            "https://jakbarkota.beta.bps.go.id",
            "https://jakutkota.beta.bps.go.id",
            "https://bogorkab.beta.bps.go.id",
            "https://sukabumikab.beta.bps.go.id",
            "https://cianjurkab.beta.bps.go.id",
            "https://bandungkab.beta.bps.go.id",
            "https://garutkab.beta.bps.go.id",
            "https://tasikmalayakab.beta.bps.go.id",
            "https://ciamiskab.beta.bps.go.id",
            "https://kuningankab.beta.bps.go.id",
            "https://cirebonkab.beta.bps.go.id",
            "https://majalengkakab.beta.bps.go.id",
            "https://sumedangkab.beta.bps.go.id",
            "https://indramayukab.beta.bps.go.id",
            "https://subangkab.beta.bps.go.id",
            "https://purwakartakab.beta.bps.go.id",
            "https://karawangkab.beta.bps.go.id",
            "https://bekasikab.beta.bps.go.id",
            "https://bandungbaratkab.beta.bps.go.id",
            "https://pangandarankab.beta.bps.go.id",
            "https://bogorkota.beta.bps.go.id",
            "https://sukabumikota.beta.bps.go.id",
            "https://bandungkota.beta.bps.go.id",
            "https://cirebonkota.beta.bps.go.id",
            "https://bekasikota.beta.bps.go.id",
            "https://depokkota.beta.bps.go.id",
            "https://cimahikota.beta.bps.go.id",
            "https://tasikmalayakota.beta.bps.go.id",
            "https://banjarkota.beta.bps.go.id",
            "https://cilacapkab.beta.bps.go.id",
            "https://banyumaskab.beta.bps.go.id",
            "https://purbalinggakab.beta.bps.go.id",
            "https://banjarnegarakab.beta.bps.go.id",
            "https://kebumenkab.beta.bps.go.id",
            "https://purworejokab.beta.bps.go.id",
            "https://wonosobokab.beta.bps.go.id",
            "https://magelangkab.beta.bps.go.id",
            "https://boyolalikab.beta.bps.go.id",
            "https://klatenkab.beta.bps.go.id",
            "https://sukoharjokab.beta.bps.go.id",
            "https://wonogirikab.beta.bps.go.id",
            "https://karanganyarkab.beta.bps.go.id",
            "https://sragenkab.beta.bps.go.id",
            "https://grobogankab.beta.bps.go.id",
            "https://blorakab.beta.bps.go.id",
            "https://rembangkab.beta.bps.go.id",
            "https://patikab.beta.bps.go.id",
            "https://kuduskab.beta.bps.go.id",
            "https://jeparakab.beta.bps.go.id",
            "https://demakkab.beta.bps.go.id",
            "https://semarangkab.beta.bps.go.id",
            "https://temanggungkab.beta.bps.go.id",
            "https://kendalkab.beta.bps.go.id",
            "https://batangkab.beta.bps.go.id",
            "https://pekalongankab.beta.bps.go.id",
            "https://pemalangkab.beta.bps.go.id",
            "https://tegalkab.beta.bps.go.id",
            "https://brebeskab.beta.bps.go.id",
            "https://magelangkota.beta.bps.go.id",
            "https://surakartakota.beta.bps.go.id",
            "https://salatigakota.beta.bps.go.id",
            "https://semarangkota.beta.bps.go.id",
            "https://pekalongankota.beta.bps.go.id",
            "https://tegalkota.beta.bps.go.id",
            "https://kulonprogokab.beta.bps.go.id",
            "https://bantulkab.beta.bps.go.id",
            "https://gunungkidulkab.beta.bps.go.id",
            "https://slemankab.beta.bps.go.id",
            "https://jogjakota.beta.bps.go.id",
            "https://pacitankab.beta.bps.go.id",
            "https://ponorogokab.beta.bps.go.id",
            "https://trenggalekkab.beta.bps.go.id",
            "https://tulungagungkab.beta.bps.go.id",
            "https://blitarkab.beta.bps.go.id",
            "https://kedirikab.beta.bps.go.id",
            "https://malangkab.beta.bps.go.id",
            "https://lumajangkab.beta.bps.go.id",
            "https://jemberkab.beta.bps.go.id",
            "https://banyuwangikab.beta.bps.go.id",
            "https://bondowosokab.beta.bps.go.id",
            "https://situbondokab.beta.bps.go.id",
            "https://probolinggokab.beta.bps.go.id",
            "https://pasuruankab.beta.bps.go.id",
            "https://sidoarjokab.beta.bps.go.id",
            "https://mojokertokab.beta.bps.go.id",
            "https://jombangkab.beta.bps.go.id",
            "https://nganjukkab.beta.bps.go.id",
            "https://madiunkab.beta.bps.go.id",
            "https://magetankab.beta.bps.go.id",
            "https://ngawikab.beta.bps.go.id",
            "https://bojonegorokab.beta.bps.go.id",
            "https://tubankab.beta.bps.go.id",
            "https://lamongankab.beta.bps.go.id",
            "https://gresikkab.beta.bps.go.id",
            "https://bangkalankab.beta.bps.go.id",
            "https://sampangkab.beta.bps.go.id",
            "https://pamekasankab.beta.bps.go.id",
            "https://sumenepkab.beta.bps.go.id",
            "https://kedirikota.beta.bps.go.id",
            "https://blitarkota.beta.bps.go.id",
            "https://malangkota.beta.bps.go.id",
            "https://probolinggokota.beta.bps.go.id",
            "https://pasuruankota.beta.bps.go.id",
            "https://mojokertokota.beta.bps.go.id",
            "https://madiunkota.beta.bps.go.id",
            "https://surabayakota.beta.bps.go.id",
            "https://batukota.beta.bps.go.id",
            "https://pandeglangkab.beta.bps.go.id",
            "https://lebakkab.beta.bps.go.id",
            "https://tangerangkab.beta.bps.go.id",
            "https://serangkab.beta.bps.go.id",
            "https://tangerangkota.beta.bps.go.id",
            "https://cilegonkota.beta.bps.go.id",
            "https://serangkota.beta.bps.go.id",
            "https://tangselkota.beta.bps.go.id",
        ]
        for url in urls_beta:
            params = {
                "subject": f"{data['params']}",
            }
            data = [{"locale":"id","keyword":"$undefined","subject":data["params"],"page":data["page"],"perpage":100,"sortBy":"date","sortOrder":"desc"}]
            data = json.dumps(data)

            beta_url_table = f"{url}/id/statistics-table"

            headers = {
                "accept": "text/x-component",
                "accept-language": "en-US,en;q=0.9,id;q=0.8",
                "content-type": "text/plain;charset=UTF-8",
                # 'cookie': '_ga_SLZ6H0R0CX=GS1.1.1721980054.1.0.1721980060.54.0.0; _ga_7FD8073S86=GS1.1.1721981973.2.0.1721981973.0.0.0; _ga=GA1.1.298596981.1721976262; _ga_MBEH1B2Q1F=GS1.1.1721994815.4.0.1721994815.0.0.0; e3f04a0fdbb85d7f8cbdaf9cbaba3c10=4922ca05374e7c3143c26fcd64d16701; f5avraaaaaaaaaaaaaaaa_session_=BBJHLHPBFMNAEOJDNFECCEPKAMIOMPGPCIHLACGBMMLPGMOACFDNPBKEBLGENHDGINODFEELHEIDOBLEBLOABBBBBEEMCCCKMANMCDNOAJOGFMEFLNJKOEOPAFENNIBC; asw={"lang":"id"}; _ga_XXTTVXWHDB=GS1.1.1723024484.3.1.1723024505.0.0.0; bpscinfo=eyJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJCYWRhbiBQdXNhdCBTdGF0aXN0aWsiLCJqdGkiOiJwekNBM1Zsd1V2eFJfcmh6ajhFcDYiLCJpYXQiOjE3MjMwMjQyMDcsImV4cCI6MTcyMzAzMTcwN30.IGaGQxOECWg7IT4pynUtt9EglqF_WGNuolIYgD8WUzU; TS01395fde=0167a1c861b432a08e9bd0daab8fb6a5e441d28d715993d8c36f89b6f74a81c8091d753fa6d7abcfcc9445d516a61639113c9caecebd4d3770e0e5fbef8b5840bd36dbf1c2ff2dbe6ae8d4aafce4def9634457adc9683d3b4df1ba616d02596305db2ac6e2',
                "next-action": "7842359e1d2f27b80750062d8bbe8942fb30abc0",
                "next-router-state-tree": "%5B%22%22%2C%7B%22children%22%3A%5B%5B%22lang%22%2C%22id%22%2C%22d%22%5D%2C%7B%22children%22%3A%5B%22statistics-table%22%2C%7B%22children%22%3A%5B%22__PAGE__%3F%7B%5C%22subject%5C%22%3A%5C%22519%5C%22%7D%22%2C%7B%7D%2C%22%2Fid%2Fstatistics-table%3Fsubject%3D519%22%2C%22refresh%22%5D%7D%2Cnull%2Cnull%2Ctrue%5D%7D%2Cnull%2Cnull%2Ctrue%5D%7D%2Cnull%2Cnull%5D",
                "origin": url,
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
            response = requests.post(
                beta_url_table,
                params=params,
                cookies=cookies,
                headers=headers,
                data=data,
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
                    "domain": url.split("/")[2],
                    "tag": ["bps", "bps.go.id", "statistics table"],
                    "category": item["subject"],
                    "sub_category": item["subcat"],
                    "title": item["title"],
                }

                client = greenstalk.Client(('192.168.150.21', 11300), use='sc_bps_daerah_detail')
                client.put(json.dumps(metadata, indent=2), ttr=3600)

                print(json.dumps(metadata))

                ssdb = pyssdb.Client(
                    host="192.168.150.21",
                    port=8888,
                    max_connections=500,
                )

                
                hset = ssdb.hset(
                    "{}".format('sc_bps_daerah_links'), "{}".format(item['id']), "{}".format(json.dumps(metadata))
                )
                print("Successfully added {} to ssdb".format(item['id']))
    except Exception as e:
        print("An error occurred: {}".format(e))
        return False
    return True

def main():
    while True:
        job = None
        try:
            job = client.reserve(1)
            data = json.loads(job.body)
            success = proses_job(data)

            if success:
                client.delete(job)
                logger.success(f"Job {job.id} processed successfully and deleted.")
            else:
                client.delete(job)
                logger.error(f"Job {job.id} failed and buried.")
        except greenstalk.NotFoundError:
            if job:
                logger.error(f"Job {job.id} not found.")
                client.delete(job)
        except greenstalk.TimedOutError:
            logger.error("Timed out while reserving a job.")
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            if job:
                client.delete(job)
        time.sleep(1)

        
if __name__ == "__main__":
    main()