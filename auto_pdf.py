from pdfminer.high_level import extract_pages, extract_text
from pdfminer.layout import LTTextContainer, LTChar, LTTextBoxHorizontal
import re
import pandas as pd
from tqdm import tqdm

data = []
country_pattern = r"(\S+ \/ \S+)"
email_pattern = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4}\b"
# email_pattern = r"/^[\w-\.]+@([\w-]+\.)+[\w-]{2,4}$/"
# phone_number_pattern = r'(?:\+\s?\d{1,4}[-. ]?)?\(?\d{1,6}\)?[-. ]?\d{1,6}[-. ]?\d{1,6}'
# phone_number_pattern = r'(?:[+-]?\d{1,4}[-() ]?)?(?:\(\d{1,6}\)|\d{1,6})[-() ]?\d{1,6}[-() ]?\d{1,6}'
phone_number_pattern = (
    r"(?:[+-]?\d{1,4}[-() ]?;?)?(?:\(\d{1,6}\)|\d{1,6})[-() ]?;?\d{1,6}[-() ]?;?\d{1,6}"
)


# phone_number_pattern = r'(?:(?:\+\d{2,4}|0\d{1,4}-?) ?\d{2,7}[- ]\d{1,7}[- ]\d{1,7})'
# phone_number_pattern = r'\b(\+\d{1,4}[-. ]?)?\(?\d{3}\)?[-. ]?\d{3}[-. ]?\d{4}\b'

pbar = tqdm(total=len(list(enumerate(extract_pages("ati.su/Catalogue.pdf")))[10:]))
for i, page_level in list(enumerate(extract_pages("ati.su/Catalogue.pdf")))[10:]:
    # if i == 25:
    #     break
    for element in page_level:
        print(element)

    d = {}
    print()
    print()
    print()

    page = list(page_level)[1:]
    d["Title"] = page[0].get_text().strip()

    keys = page[1].get_text()
    j = 2

    if ":" in page[j].get_text():
        while ":" in page[j].get_text():
            keys = keys + page[j].get_text()
            values = page[j + 1].get_text()
            j += 1
    else:
        values = page[j].get_text()

    country = re.search(country_pattern, values)
    d["Country"] = country.group(1) if country else ""
    d["Phone Number"] = ", ".join(
        filter(lambda x: len(x) > 6, re.findall(phone_number_pattern, values))
    )
    d["Email"] = ", ".join(re.findall(email_pattern, values))

    # re.split(r'(?<!\s)\n(?!\s)', values)

    # for key,value in zip(keys.split("\n"),values.split("\n")):
    #     d[key] = value

    data.append(d)
    pbar.update()

    # # d['Title'] = page_level.
    # print(d)
    # print()
    # print()

pbar.close()
df = pd.DataFrame(data=data)

writer = pd.ExcelWriter(
    f"./AM_FR_2023_Exhibitor.xlsx",
    engine="xlsxwriter",
    engine_kwargs={"options": {"strings_to_urls": False}},
)
df.to_excel(writer, index=False, freeze_panes=(1, 1))
writer.close()
df.to_csv(f"./AM_FR_2023_Exhibitor.csv", index=False)
print("!!! Parsing Completed !!!")
