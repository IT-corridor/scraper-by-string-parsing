import re
import csv
import urllib2
import datetime
import psycopg2
import config
import pdb

from lxml import etree
from mailchimp3 import MailChimp

FIELDS = [
    'page_title',
    'timestamp',
    'label',
    'email',
    'phone',
    'description'
]

client = MailChimp(mc_api=config.MC_API_KEY, mc_user=config.MC_USER_NAME)

def get_email(txt):
    match_email = re.search(r'[\w\.-]+@[\w-]+\.[\w-]+', txt)
    return match_email.group(0) if match_email else ''

def get_phone(txt):
    match_phone = re.search(r'(\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]??\d{4}|\d{3}[-\.\s]??\d{4})', txt)
    return match_phone.group(0) if match_phone else ''

def get_db_connection():
    connect_str = "dbname='east27' user='twodo_user' host='localhost' password='f2#2$2D@3'"
    conn = psycopg2.connect(connect_str)
    return conn

def main(urls):
    base_url = 'http://www.27east.com/hamptons-classifieds/index.cfm/'
    ts = datetime.datetime.now().strftime('%m/%d/%Y %H:%M:%S')
    filename = datetime.datetime.now().strftime('%Y-%m-%dT%H.%M.%S.csv')

    articles = []
    for url in urls:
        response = urllib2.urlopen(base_url+url)
        htmlparser = etree.HTMLParser()
        tree = etree.parse(response, htmlparser)

        # 1. get full text
        xpath = '//*[@id="yui-main"]/div/div/div[1]/div[4]/div[1]//text()'
        contents = [ii.strip() for ii in tree.xpath(xpath) if len(ii.strip()) > 1]
        xpath = '//*[@id="yui-main"]/div/div/div[1]/div[4]/div[2]//text()'
        contents += [ii.strip() for ii in tree.xpath(xpath) if len(ii.strip()) > 1]

        # 2. separate articles
        description = ''

        ii = 0

        # consider website too
        while ii < len(contents):
            description += contents[ii] + '\n'
            email = get_email(contents[ii])
            phone = get_phone(contents[ii])

            if email or phone:
                if ii < len(contents)-1 and len(contents[ii+1]) < 50 and ((get_email(contents[ii+1]) and phone) or (get_phone(contents[ii+1]) and email)):
                    email = email if email else get_email(contents[ii+1])
                    phone = phone if phone else get_phone(contents[ii+1])
                    ii = ii + 1
                    description += contents[ii] + '\n'
                elif ii < len(contents)-2 and len(contents[ii+1]+contents[ii+2]) < 50 and ((get_email(contents[ii+1]+contents[ii+2]) and phone) or (get_phone(contents[ii+1]+contents[ii+2]) and email)):
                    email = email if email else get_email(contents[ii+1]+contents[ii+2])
                    phone = phone if phone else get_phone(contents[ii+1]+contents[ii+2])
                    ii = ii + 2
                    description += contents[ii-1] + '\n'
                    description += contents[ii] + '\n'

                match_label = re.search(r'^[A-Z\s,/\*-]{2,}[\s,-]+', description)
                articles.append({
                    'description': description.encode('ascii', 'ignore'),
                    'email': email.encode('ascii', 'ignore'),
                    'phone': phone.encode('ascii', 'ignore'),
                    'label': match_label.group(0).replace('\n', ' ').strip().strip('-').encode('ascii', 'ignore') if match_label else '',
                    'page_title': url.split('/')[-1],
                    'timestamp': datetime.datetime.now()
                })
                description = ''
            ii = ii + 1

    # save csv file
    # with open(filename, 'wb') as f:  # Just use 'w' mode in 3.x
    #     w = csv.DictWriter(f, FIELDS)
    #     w.writeheader()
    #     for ii in articles:
    #         w.writerow(ii)

    # create mailchimp list and add members
    try:
        list_data = {
            'name': datetime.datetime.now().strftime('east27-%m-%d-%Y'),
            'contact': {
                'company': 'Hamptons Job Board',
                'address1': 'Main Street',
                'city': 'East Hampton',
                'state': 'New York',
                'zip': '11937',
                'country': 'USA'
            },
            'email_type_option': True,
            'permission_reminder': 'Hamptons Job Board: Start Listing For Free Today!',
            'campaign_defaults': {
                'from_name': 'Hamptons Job Board',
                'from_email': 'info@hamptonsjobboard.com',
                'subject': 'Hamptons Job Board',
                'language': 'English'
            }
        }

        llist = client.lists.create(data=list_data)

        # create merge fields for the list
        for ii in ['PAGETITLE', 'ADTITLE']:
            client.lists.merge_fields.create(list_id=llist['id'], data={
                'name': ii,
                'tag': ii,
                'type': 'text'
            })

        emails = []
        for ii in articles:
            if ii['email'] and ii['email'] not in emails:
                print (ii)
                emails.append(ii['email'])
                try:
                    client.lists.members.create(llist['id'], {
                        'email_address': ii['email'],
                        'status': 'subscribed',
                        'merge_fields': {
                            'PHONE': ii['phone'],
                            'ADTITLE': ii['label'],
                            'PAGETITLE': ii['page_title'] 
                        },
                    })
                except:
                    print ('------------------------------------')
    except Exception as e:
        pass

    # store in database
    conn = get_db_connection()
    # pdb.set_trace()
    with conn.cursor() as cursor:
        values = []
        columns = ', '.join(articles[0].keys())
        placeholders = ', '.join(['%s'] * len(articles[0]))

        sql = "INSERT INTO ad ( %s ) VALUES ( %s )" % (columns, placeholders)
        for ii in articles:
            values.append(tuple(ii.values()))

        cursor.executemany(sql, values)
        conn.commit()

if __name__ == "__main__":
    urls = [
        '/1220/Help-Wanted/Home-Health-Care',
        '/1235/Help-Wanted/Office-Professional',
        '/1205/Help-Wanted/Building-Trades',
        '/1210/Help-Wanted/Child-Care',
        '/1215/Help-Wanted/Domestic',
        '/1225/Help-Wanted/Food-Drink',
        '/1255/Help-Wanted/General',
        '/1220/Help-Wanted/Home-Health-Care',
        '/1230/Help-Wanted/Landscape-Garden',
        '/1235/Help-Wanted/Office-Professional',
        '/1245/Help-Wanted/Retail',
    ]

    main(urls)
