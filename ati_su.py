from custom_utils.scrap_utils import MAX_RETRIES, logger
import multiprocessing
import re


import fake_useragent
import pandas as pd
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from tqdm import tqdm

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

session = requests.Session()

log = logger()


def get_all_links(page, retries=MAX_RETRIES):
    try:
        base_url = "https://ati.su/gw/atiwebroot/public/v1/api/passport/GetFirm/"
        user = fake_useragent.UserAgent().random
        headers = {"User-Agent": user}
        res = session.get(page, headers=headers, verify=False)
        json_data = res.json()
        return [f"{base_url}{firm['firm']['ati_id']}" for firm in json_data["firms"]]

    except (
        requests.exceptions.Timeout,
        requests.exceptions.HTTPError,
        requests.exceptions.ConnectionError,
    ) as e:
        if retries > 0:
            log.warning(f"ALL_LINKS Retrying {page} due to error: {e}")
            return get_all_links(page, retries=retries - 1)
        else:
            log.debug(
                f"ALL_LINKS Max retries reached with link {page}. Error: {e}")
            return []
    except Exception as e:
        log.error(f"ALL_LINKS Exception occured in {page}, error: {e}")
        return []


def get_attribute(json_data, key):
    try:
        return json_data[key].strip()
    except:
        return ""


def parser_data(page, retries=MAX_RETRIES):
    # if len(page) > 2:
    #     print(page)

    try:
        index, p = page
        user = fake_useragent.UserAgent().random
        headers = {"User-Agent": user}

        d = {}
        link_res = session.get(p, headers=headers, verify=False)
        # ---------------------------------------------------------------

        json_data = link_res.json()

        pattern = r"\+77\d{9}"

        d["Position"] = index
        d["Url Link"] = f"https://ati.su/firms/{json_data['atiId']}/info"
        d["atiId"] = get_attribute(json_data, "atiId")
        d["Firm Name"] = get_attribute(json_data, "firmName")
        d["Address"] = get_attribute(json_data, "address")
        d["ИИН"] = get_attribute(json_data, "inn")
        d["Firm Type"] = get_attribute(json_data, "firmType")
        d["City Name"] = get_attribute(json_data, "cityName")
        d["Country Name"] = get_attribute(json_data, "countryName")
        d["Ownership"] = json_data["ownership"]["name"].strip()
        try:
            d["Phone Number"] = re.search(pattern, str(json_data)).group()
        except:
            d["Phone Number"] = ""

        email_pattern = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4}\b"

        d["Email"] = ", ".join(re.findall(email_pattern, str(json_data)))

        return d
    except (
        requests.exceptions.Timeout,
        requests.exceptions.HTTPError,
        requests.exceptions.ConnectionError,
    ) as e:
        if retries > 0:
            log.warning(f"Index: {index} Retrying {p} due to error: {e}")
            return parser_data((index, p), retries=retries - 1)
        else:
            log.error(
                f"Index: {index} Max retries reached with link {p}. Error: {e}")
            return {}

    except Exception as e:
        log.error(f"Index: {index} Exception occured in {p}, error: {e}")
        return {}


def main(pages, output_file):
    print("!!! The collection of links to comoanies !!!")

    with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
        all_links = []
        for links in tqdm(pool.imap_unordered(get_all_links, pages), total=len(pages)):
            all_links.extend(links)
    print("Collected links:", len(all_links))

    all_links = [(index + 1, url) for index, url in enumerate(all_links)]

    print("!!! Parsing of links !!!")
    with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:

        results = list(
            tqdm(pool.imap_unordered(parser_data, all_links), total=len(all_links))
        )

    print(f"Количество собранных карточек: {len(results)}")
    print("Failed results: ", any(map(lambda x: x is None, results)))
    results = list(filter(lambda x: bool(x), results))

    print("Length of actual results:", len(results))
    results = sorted(results, key=lambda x: x["Position"])

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

    # 10 Kazakhstan
    # 1 Russian
    geo_id = 1
    skip = 0
    take = 300
    urls = [
        # Перевозчики
        "https://ati.su/gw/rating-page-service/public/v1/rating?atiDocs=false&atiOrders=false&autopark=false&firmTypes=1&geoId={geo_id}&geoTypeId=0&reverse=false&skip={skip}&take={take}&verified=false",
        # Грузовладельцы
        "https://ati.su/gw/rating-page-service/public/v1/rating?atiDocs=false&atiOrders=false&autopark=false&firmTypes=3&firmTypes=6&geoId={geo_id}&geoTypeId=0&reverse=false&skip={skip}&take={take}&verified=false",
        # Экспедиторы
        "https://ati.su/gw/rating-page-service/public/v1/rating?atiDocs=false&atiOrders=false&autopark=false&firmTypes=2&firmTypes=4&firmTypes=5&geoId={geo_id}&geoTypeId=0&reverse=false&skip={skip}&take={take}&verified=false",
    ]

    links = []
    for url in urls:
        res = requests.get(url.format(geo_id=geo_id, skip=skip, take=take))
        total = res.json()["total_firms_count"]
        links += [
            url.format(geo_id=geo_id, skip=take * i, take=take)
            for i in range(total // take + 1)
        ]

    output_file = "./ati.su/files/ati_rus"

    print("Len of Links:", len(links))

    main(links, output_file)
