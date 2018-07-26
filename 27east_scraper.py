import re
import csv
import urllib2
import datetime

from lxml import etree

FIELDS = [
    'page title',
    'timestamp',
    'label',
    'email',
    'phone',
    'desc'
]

def get_email(txt):
    match_email = re.search(r'[\w\.-]+@[\w-]+\.[\w-]+', txt)
    return match_email.group(0) if match_email else ''

def get_phone(txt):
    match_phone = re.search(r'(\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]??\d{4}|\d{3}[-\.\s]??\d{4})', txt)
    return match_phone.group(0) if match_phone else ''

def main(urls):
    ts = datetime.datetime.now().strftime('%m/%d/%Y %H:%M:%S')
    filename = datetime.datetime.now().strftime('%Y-%m-%dT%H.%M.%S.csv')

    articles = []
    for url in urls:
        response = urllib2.urlopen(url)
        htmlparser = etree.HTMLParser()
        tree = etree.parse(response, htmlparser)

        # 1. get full text
        xpath = '//*[@id="yui-main"]/div/div/div[1]/div[4]/div[1]//text()'
        contents = [ii.strip() for ii in tree.xpath(xpath) if len(ii.strip()) > 1]
        xpath = '//*[@id="yui-main"]/div/div/div[1]/div[4]/div[2]//text()'
        contents += [ii.strip() for ii in tree.xpath(xpath) if len(ii.strip()) > 1]

        # with open('res2.txt', 'w') as f:
        #     for ii in contents:
        #         f.write(ii.encode('ascii', 'ignore')+'\n')

        # 2. separate articles
        desc = ''

        ii = 0

        # consider website too
        while ii < len(contents):
            desc += contents[ii] + '\n'
            email = get_email(contents[ii])
            phone = get_phone(contents[ii])

            if email or phone:
                if ii < len(contents)-1 and len(contents[ii+1]) < 50 and ((get_email(contents[ii+1]) and phone) or (get_phone(contents[ii+1]) and email)):
                    email = email if email else get_email(contents[ii+1])
                    phone = phone if phone else get_phone(contents[ii+1])
                    ii = ii + 1
                    desc += contents[ii] + '\n'
                elif ii < len(contents)-2 and len(contents[ii+1]+contents[ii+2]) < 50 and ((get_email(contents[ii+1]+contents[ii+2]) and phone) or (get_phone(contents[ii+1]+contents[ii+2]) and email)):
                    email = email if email else get_email(contents[ii+1]+contents[ii+2])
                    phone = phone if phone else get_phone(contents[ii+1]+contents[ii+2])
                    ii = ii + 2
                    desc += contents[ii-1] + '\n'
                    desc += contents[ii] + '\n'

                match_label = re.search(r'^[A-Z\s,/\*-]{2,}[\s,-]+', desc)
                articles.append({
                    'desc': desc.encode('ascii', 'ignore'),
                    'email': email.encode('ascii', 'ignore'),
                    'phone': phone.encode('ascii', 'ignore'),
                    'label': match_label.group(0).replace('\n', ' ').strip().strip('-').encode('ascii', 'ignore') if match_label else '',
                    'page title': url.split('/')[-1],
                    'timestamp': ts
                })
                desc = ''
            ii = ii + 1

    with open(filename, 'wb') as f:  # Just use 'w' mode in 3.x
        w = csv.DictWriter(f, FIELDS)
        w.writeheader()
        for ii in articles:
            w.writerow(ii)

if __name__ == "__main__":
    urls = [
        'http://www.27east.com/hamptons-classifieds/index.cfm/1220/Help-Wanted/Home-Health-Care',
        'http://www.27east.com/hamptons-classifieds/index.cfm/1235/Help-Wanted/Office-Professional',
        'http://www.27east.com/hamptons-classifieds/index.cfm/1205/Help-Wanted/Building-Trades',
        'http://www.27east.com/hamptons-classifieds/index.cfm/1210/Help-Wanted/Child-Care',
        'http://www.27east.com/hamptons-classifieds/index.cfm/1215/Help-Wanted/Domestic',
        'http://www.27east.com/hamptons-classifieds/index.cfm/1225/Help-Wanted/Food-Drink',
        'http://www.27east.com/hamptons-classifieds/index.cfm/1255/Help-Wanted/General',
        'http://www.27east.com/hamptons-classifieds/index.cfm/1220/Help-Wanted/Home-Health-Care',
        'http://www.27east.com/hamptons-classifieds/index.cfm/1230/Help-Wanted/Landscape-Garden',
        'http://www.27east.com/hamptons-classifieds/index.cfm/1235/Help-Wanted/Office-Professional',
        'http://www.27east.com/hamptons-classifieds/index.cfm/1245/Help-Wanted/Retail',
    ]

    main(urls)