import multiprocessing
import re

import pandas as pd
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from tqdm import tqdm
import traceback

from custom_utils.scrap_utils import MAX_RETRIES, logger

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

session = requests.Session()

log = logger()


def get_all_links(page, retries=MAX_RETRIES):
    try:
        base_url = "https://ati.su/gw/atiwebroot/public/v1/api/passport/GetFirm/"
        headers = {
            'Content-Type': 'application/json',
            "Accept": 'application/json',
            "Content-Type": 'application/json',
            "User-Agent": "ati_integrator_9782442",
            "Accept-Encoding": "gzip, deflate, br"
        }
        res = session.get(page, headers=headers)
        json_data = res.json()

        return [f"{base_url}{firm['firm']['ati_id']}" for firm in json_data["firms"]]

    except (
        requests.exceptions.Timeout,
        requests.exceptions.HTTPError,
        requests.exceptions.ConnectionError,
        KeyError
    ) as e:
        if retries > 0:
            log.warning(f"ALL_LINKS Retrying {page} due to error: {e}")
            return get_all_links(page, retries=retries - 1)
        else:
            log.debug(f"ALL_LINKS Max retries reached with link {page}. Error: {e}")
            return []
    except Exception as e:
        print("LAST EXCEPTION:",traceback.format_exc())
        log.error(f"ALL_LINKS Exception occured in {page}, error: {e}")
        return []


def get_attribute(json_data, key):
    try:
        return json_data[key].strip()
    except:
        return ""


def parser_data(page, retries=MAX_RETRIES):

    contact_url = "https://ati.su/gw/atiwebroot/public/v1/api/passport/getContacts/{}"
    # email_url = "https://ati.su/api/email/getEmail/{}/{}" #TODO: RATE LIMITER 30 requests per day

    contact_email_headers = {
        "cookie": "_gcl_au=1.1.738173245.1698820354; tmr_lvid=8171fce759644a1d22aab21b802045ec; tmr_lvidTS=1698820366365; _ym_uid=1698820371505633408; _ym_d=1698820371; did=rvmNAeQXWZ416f5Ss90hyWGDsf8mJHkmCfYdPFeLvq8%3D; sid=8008576b4ffb4973b417a42f6d9c20e8; efid=%5E%5EVBQGB; _ymab_param=wTwnaHHskVQ1UgNYXt9gWjQpEamPS1RiaXLHQ5k8QlYAWZ5cQZR4smCO4GHouetAuFPOkPowWGGbB2OH88oBUjR5Mag; _ga_14VPSGD0HN=deleted; atisuReferrer=utm_campaign=new_header; uicult=en-US; uicult2=en; _ga=GA1.2.1187309523.1698820362; _ga_Z6YM1FRK5D=GS1.2.1698985173.11.1.1698986624.60.0.0; _ga_14VPSGD0HN=GS1.1.1698985172.12.1.1698987027.0.0.0"
    }

    try:
        index, p = page
        headers = {
            'authority': 'ati.su',
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9,ru-RU;q=0.8,ru;q=0.7',
            'cookie': 'uicult2=ru; _gcl_au=1.1.2119184884.1698926444; tmr_lvid=3e14f29eeb7a25532b6d5f6825aa2b59; tmr_lvidTS=1624637996182; _ymab_param=ube9aEMKpvi68iFEtwJtZ1xG5mjCB7Bmy1bT0AYI08qP9tFP068fLR0L9h3c4TIMq8h9oNR6_svi5iG_QekuJGRELuM; _ym_uid=1624637996362972713; _ym_d=1698926445; did=fC6aIwQN9xGaBBrF4MsIq9QRAPOYQthR10a7Wf%2FflCw%3D; efid=%5E%5EVBQGB; _ga=GA1.2.1678171963.1698926444; _ga_Z6YM1FRK5D=GS1.2.1698926445.1.1.1698926815.53.0.0; _ga_14VPSGD0HN=GS1.1.1698926444.1.1.1698927284.0.0.0; AtiGeo=0_0_0_26',
            'referer': 'https://ati.su/',
            'sec-ch-ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
        }
        # headers = {
        #     # 'Content-Type': 'application/json',
        #     # "Accept": 'application/json',
        #     # "Content-Type": 'application/json',
        #     # "User-Agent": "ati_integrator_9782442",
        #     # "Accept-Encoding": "gzip, deflate, br"
        # }


        d = {}
        link_res = session.get(p, headers=headers, verify=False)
        

        json_data = link_res.json()
        

        pattern = r"\+77\d{9}"
        ati_id = get_attribute(json_data, "atiId")

        
        contact_res = session.get(
            contact_url.format(ati_id), headers=contact_email_headers, verify=False
        )
        # ---------------------------------------------------------------
        contact_data = contact_res.json()


        # d["Position"] = index
        d["Url Link"] = f"https://ati.su/firms/{json_data['atiId']}/info"
        d["atiId"] = ati_id
        d["Firm Name"] = get_attribute(json_data, "firmName")
        d["Address"] = get_attribute(json_data, "address")
        d["INN"] = get_attribute(json_data, "inn")
        d["Firm Type"] = get_attribute(json_data, "firmType")
        d["City Name"] = get_attribute(json_data, "cityName")
        d["Country Name"] = get_attribute(json_data, "countryName")
        d["Ownership"] = json_data["ownership"]["name"].strip()

        phone_numbers = []
        for data in contact_data.values():
            phone_numbers.append(data["phoneInfo"]["phone"])
            phone_numbers.append(data["mobileInfo"]["phone"])
            phone_numbers.append(data["faxInfo"]["phone"])

        

        
        phone_numbers = list(set(filter(lambda x: x != "" and x != None, phone_numbers)))
        d["Phone Numbers"] = " ; ".join(phone_numbers)
        d["Email Adresses"] = "" 

        try:
            d["Additional Phone Number"] = re.search(pattern, str(json_data)).group()
        except:
            d["Additional Phone Number"] = ""

        email_pattern = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4}\b"

        d["Additional Email"] = ", ".join(re.findall(email_pattern, str(json_data)))

        return d
    except (
        requests.exceptions.Timeout,
        requests.exceptions.HTTPError,
        requests.exceptions.ConnectionError,
        KeyError
    ) as e:
        if retries > 0:
            log.warning(f"Index: {index} Retrying {p} due to error: {e}")
            return parser_data((index, p), retries=retries - 1)
        else:
            log.error(f"Index: {index} Max retries reached with link {p}. Error: {e}")
            return {}

    except Exception as e:
        print(traceback.format_exc())
        log.error(f"Index: {index} Exception occured in {p}, error: {e}")
        return {}


def main(pages, output_file,atis):
    # print("!!! The collection of links to companies !!!")

    # with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
    #     all_links = []
    #     for links in tqdm(pool.imap_unordered(get_all_links, pages), total=len(pages)):
    #         all_links.extend(links)
    # print("Collected links:", len(all_links))

    all_links = [(index + 1, url) for index, url in enumerate(atis)]

    print("!!! Parsing of links !!!")
    with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:

        results = list(
            tqdm(pool.imap_unordered(parser_data, all_links), total=len(all_links))
        )

    print(f"Количество собранных карточек: {len(results)}")
    print("Failed results: ", any(map(lambda x: x is None, results)))
    results = list(filter(lambda x: bool(x), results))

    print("Length of actual results:", len(results))
    # results = sorted(results, key=lambda x: x["Position"])

    print("!!! Saving data to files xlsx and csv !!!")
    df = pd.DataFrame(data=results)

    writer = pd.ExcelWriter(
        f"./{output_file}.xlsx",
        engine="xlsxwriter",
        engine_kwargs={"options": {"strings_to_urls": False}},
    )

    df.to_excel(writer, index=False, freeze_panes=(1, 1))
    writer.close()
    df.to_csv(f"./{output_file}.csv", index=False)
    print("!!! Parsing Completed !!!")


if __name__ == "__main__":
    import requests

    country_ids = {
        # 10:"kazakhstan",
        1:"russian"
        # 3:"belarus",
        # 2: "ukraine",
        # 14:"Uzbekistan",
        # 11:"Kyrgystan",
        # 7:"Armenia",
        # 6:"Lithuania",
        # 8:"Moldova",
        # 5:"Latvia"
    }

    take = 300
    urls = [
        # Перевозчики
        "https://ati.su/gw/rating-page-service/public/v1/rating?atiDocs=false&atiOrders=false&autopark=false&firmTypes=1&geoId={geo_id}&geoTypeId=0&reverse=false&skip={skip}&take={take}&verified=false",
        # Грузовладельцы
        # "https://ati.su/gw/rating-page-service/public/v1/rating?atiDocs=false&atiOrders=false&autopark=false&firmTypes=3&firmTypes=6&geoId={geo_id}&geoTypeId=0&reverse=false&skip={skip}&take={take}&verified=false",
        # Экспедиторы
        # "https://ati.su/gw/rating-page-service/public/v1/rating?atiDocs=false&atiOrders=false&autopark=false&firmTypes=2&firmTypes=4&firmTypes=5&geoId={geo_id}&geoTypeId=0&reverse=false&skip={skip}&take={take}&verified=false",
    ]

    links = []
    # for id, country_name in country_ids.items():
    #     for url in urls:
    #         res = requests.get(url.format(geo_id=id, skip=0, take=take))
    #         total = res.json()["total_firms_count"]
    #         links += [
    #             url.format(geo_id=id, skip=take * i, take=take)
    #             for i in range(total // take + 1)
    #         ]

    output_file = "./xlsx_files/ati_rus_the_rest_of_04.12.2023"

    rest_ati_ids = []
    with open("logger.log","r") as file:
        for line in file.readlines():
            rest_ati_ids.append(line[line.find('occured in')+11:line.find(', error:')])

    # print(rest_ati_ids)
    print("Len of Links:", len(rest_ati_ids))

    main(links, output_file,rest_ati_ids)

    # You can start from now Temirlan
