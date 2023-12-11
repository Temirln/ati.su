import requests
import pandas as pd
from tqdm import tqdm
import traceback
import time
from custom_utils.scrap_utils import logger
from pprint import pprint
log = logger()
df = pd.read_excel("xlsx_files/ati_multicountry_04.12.2023.xlsx").to_dict("index")
exists_df = pd.read_excel("xlsx_files/ati_multicountry_emails_04.12.2023 (not complete).xlsx").to_dict("index")

ati_ids = list(map(lambda x: x['atiId'],df.values()))
exists_ati_ids = list(map(lambda x: x['atiId'],exists_df.values()))

ati = list(set(ati_ids)-set(exists_ati_ids))
print(len(ati))

contact_email_url = "https://api.ati.su/v1.0/firms/{}/contacts/summary"
contacts_emails_headers = {
    'Authorization': 'Bearer 8145fc3dcd8d4eec9c2332095fb27e97',
    'Content-Type': 'application/json',
    "Accept": 'application/json',
    "Content-Type": 'application/json',
    "User-Agent": "ati_integrator_9782442",
    "Accept-Encoding": "gzip, deflate, br"
}
data = []

pbar = tqdm(total = len(ati))

for index, id in enumerate(ati):
    try:
        if index % 6 == 0 and index != 0:
            log.info("Time Sleep: %d" % index)
            time.sleep(6)

        d = {}

        phone_numbers = []
        email_addresses = []
        try:
            res = requests.get(contact_email_url.format(id),headers = contacts_emails_headers).json()
        except:
            print()
            pprint(traceback.format_exc())
            print()
            break

        if "error" in res or "reason" in res:
            print()
            pprint(res)
            break

        for contact in res:
            phone_numbers.append(contact['mobile_phone'])
            phone_numbers.append(contact['fax'])
            phone_numbers.append(contact['phone'])

            email_addresses.append(contact['email'])


        phone_numbers = list(set(filter(lambda x: x != "" and x != None, phone_numbers)))
        email_addresses = list(set(filter(lambda x: x != "" and x != None, email_addresses)))
        

        d['atiId'] = id
        d['Firm Name'] = res[0]['firm_name'].strip()
        d['Firm Type'] = res[0]['firm_type'].strip()
        d['Ownership'] = res[0]['ownership'].strip()
        d['Phone Numbers'] = "; ".join(phone_numbers)
        d['Email Addresses'] = "; ".join(email_addresses)

        data.append(d)
        pbar.update()
    except IndexError as e:
        print(traceback.print_exc())
        continue
    
    except Exception as e:
        print()
        print(e)
        print()
        print(traceback.print_exc())
        print()
        break


pbar.close()
print(len(data))

df = pd.DataFrame(data=data)

writer = pd.ExcelWriter(
    f"./{'xlsx_files/ati_multicountry_email_additional_04.12.2023'}.xlsx",
    engine="xlsxwriter",
    engine_kwargs={"options": {"strings_to_urls": False}},
)

df.to_excel(writer, index=False, freeze_panes=(1, 1))
writer.close()
# df.to_csv(f"./xlsx_files/ati_multicountry_contacts_04.12.2023.csv", index=False)
