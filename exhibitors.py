import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import pandas as pd
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

headers = {
    "user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
}

def parse_table(thead,tbody,output_file):
    data = []
    pbar = tqdm(total = len(tbody))
    for items in tbody:
        d = {}
        for head,item in zip(thead,items):
            d[head] = item.get_text()
        d['Contacts'] = ""
        d['Email'] = ""
        data.append(d)
        pbar.update()

    pbar.close()

    df = pd.DataFrame(data=data)
    writer = pd.ExcelWriter(
        f"{output_file}.xlsx",
        engine="xlsxwriter",
        engine_kwargs={"options": {"strings_to_urls": False}},
    )

    df.to_excel(writer, index=False, freeze_panes=(1, 1))
    writer.close()
    df.to_csv(f"{output_file}.csv", index=False)


if __name__ == "__main__":

    urls = [
        ("houseware","https://www.houseware.kz/Visitors/Exhibitor-List/?lang=en"),
        ("hometextilepro","https://www.hometextilexpo.kz/Exhibitors/Exhibitor-List/?lang=en")
    ]
    for file,url in urls:
        res = requests.get(url,headers = headers)
        soup = BeautifulSoup(res.content,"html.parser")
        table = soup.find("table",{"id":"example"})
        thead = [t.get_text() for t in table.find("thead").find("tr").find_all("th")]
        tbody =[tr.find_all("td") for tr in table.find("tbody").find_all("tr")]

        output_file = f"./ati.su/files/{file}"

        parse_table(thead, tbody,output_file)