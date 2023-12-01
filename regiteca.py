import multiprocessing

import fake_useragent
import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from tqdm import tqdm
from custom_utils.scrap_utils import logger

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

session = requests.Session()


log = logger()


def parser_data(page, retries=3):
    try:
        index, p = page
        user = fake_useragent.UserAgent().random
        headers = {"User-Agent": user}

        d = {}
        link_res = session.get(p, headers=headers, verify=False)
        soup = BeautifulSoup(link_res.content, "html.parser")
        # ---------------------------------------------------------------

        d["id"] = index
        d["Title"] = soup.find("h2", {"class": "exhib-name"}).get_text().strip()
        d["Country-City"] = soup.find("span", {"id": "lbCity"}).get_text().strip()
        d["url"] = p
        exhib = soup.find_all("div", {"class": "exhib-web"})
        for e in exhib:
            if e.find("i", {"class": "fas fa-phone"}):
                d["Phone"] = e.find("a").get_text().strip()
            elif e.find("i", {"class": "fas fa-envelope-open"}):
                d["Email"] = e.find("a").get_text().strip()
            elif e.find("i", {"class": "fas fa-globe-americas"}):
                d["Website"] = e.find("a").get_text().strip()
            elif e.find("i", {"class": "fab fa-instagram"}):
                d["Instagram"] = e.get_text().replace("Instagram:", "").strip()
            elif e.find("i", {"class": "fab fa-facebook"}):
                d["Facebook"] = e.get_text().replace("Facebook:", "").strip()
            elif e.find("i", {"class": "fab fa-linkedin"}):
                d["Linkedin"] = e.get_text().replace("Linkedin:", "").strip()
            elif e.find("i", {"class": "fab fa-youtube"}):
                d["YouTube"] = e.get_text().replace("YouTube:", "").strip()
            elif e.find("i", {"class": "fab fa-telegram"}):
                d["Telegram"] = e.get_text().replace("Telegram:", "").strip()
            elif e.find("i", {"class": "fab fa-vk"}):
                d["Vkontakte"] = e.get_text().replace("Vkontakte:", "").strip()
            elif e.find("i", {"class": "fab fa-whatsapp"}):
                d["WhatsApp"] = e.get_text().replace("WhatsApp:", "").strip()
            elif e.find("i", {"class": "fab fa-viber"}):
                d["Viber"] = e.get_text().replace("Viber:", "").strip()
            elif e.find("i", {"class": "fab fa-twitter"}):
                d["Twitter"] = e.get_text().replace("Twitter:", "").strip()
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
            log.error(f"Index: {index} Max retries reached with link {p}. Error: {e}")
            return {}

    except Exception as e:
        log.error(f"Index: {index} Exception occured in {p}, error: {e}")
        return {}


def main(pages, output_file):
    print("!!! Parsing Started !!!")
    with multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
        results = list(tqdm(pool.imap_unordered(parser_data, pages), total=len(pages)))

    print(f"Collected links: {len(results)}")
    print("Failed Links: ", any(map(lambda x: x is None, results)))
    results = list(filter(lambda x: bool(x), results))
    print("Length of actual companies:", len(results))

    results = sorted(results, key=lambda x: x["id"])

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
    urls = [
        ("translogistica_01.12.2023", "TransKazakhstan"),
        ("kazbuilds_01.12.2023", "KazBuild"),
        ("kihe_01.12.2023", "KIHE"),
        ("securex_01.12.2023", "SECUREX%20Kazakhstan"),
        ("mining_metals_01.12.2023", "MMCA"),
    ]

    base_url = "https://reg.iteca.kz/list/s/en/auth_s.aspx?ExhCode={}%202023"

    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
    }
    for file, url in urls:
        res = requests.get(base_url.format(url), headers=headers)
        soup = BeautifulSoup(res.content, "html.parser")
        links = [
            (
                index + 1,
                f'https://reg.iteca.kz/list/en/detailsfull.aspx?link={a.find("a").get("href").split("?link=")[1]}',
            )
            for index, a in enumerate(soup.find_all("div", {"class": "exhib-name"}))
        ]
        output_file = f"./ati.su/xlsx_files/{file}"
        print("Len of Links:", len(links))

        main(links, output_file)
