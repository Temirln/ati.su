import requests
import pandas as pd
from tqdm import tqdm
import traceback
df = pd.read_excel("xlsx_files/ati_multicountry_04.12.2023.xlsx").to_dict("index")

ati_ids = list(map(lambda x: x['atiId'],df.values()))

contact_email_url = "https://api.ati.su/v1.0/firms/{}/contacts/summary"
contacts_emails_headers = {
    'Authorization': 'Bearer 448bd9478aab4218a1fb03f41e5df9a0',
    'Content-Type': 'application/json',
    "Accept": 'application/json',
    "Content-Type": 'application/json',
    "User-Agent": "ati_integrator_9782442",
    "Accept-Encoding": "gzip, deflate, br"
}
data = []

pbar = tqdm(total = len(ati_ids))

for id in ati_ids[:1]:
    d = {}

    phone_numbers = []
    email_addresses = []
    try:
        res = requests.get(contact_email_url.format(id),headers = contacts_emails_headers).json()
    except:
        print(traceback.format_exc())
        break

    for contact in res:
        phone_numbers.append(contact['mobile_phone'])
        phone_numbers.append(contact['fax'])
        phone_numbers.append(contact['phone'])

        email_addresses.append(contact['email'])


    phone_numbers = list(set(filter(lambda x: x != "" and x != None, phone_numbers)))
    email_addresses = list(set(filter(lambda x: x != "" and x != None, email_addresses)))
    

    d['atiId'] = id
    d['Phone Numbers'] = ";\n ".join(phone_numbers)
    d['Email Addresses'] = ";\n ".join(email_addresses)

    data.append(d)
    pbar.update()


pbar.close()
print(len(data))
# print(data)

df = pd.DataFrame(data=data)

writer = pd.ExcelWriter(
    f"xlsx_files/ati_multicountry_contacts_04.12.2023.xlsx",
    engine="xlsxwriter",
    engine_kwargs={"options": {"strings_to_urls": False}},
)
df.to_excel(writer, index=False, freeze_panes=(1, 1))
df.to_csv(f"xlsx_files/ati_multicountry_contacts_04.12.2023.csv", index=False)