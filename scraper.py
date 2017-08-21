# This is a template for a Python scraper on morph.io (https://morph.io)
# including some code snippets below that you should find helpful

# You don't have to do things with the ScraperWiki and lxml libraries.
# You can use whatever libraries you want: https://morph.io/documentation/python
# All that matters is that your final data is written to an SQLite database
# called "data.sqlite" in the current working directory which has at least a table
# called "data".

from lxml import html
import urllib2, requests, csv, os, sqlite3
from datetime import date

def download_reg():
    if not os.path.exists(charity_reg_file):
        urllib2.urlopen('http://www.charitycommissionni.org.uk/charity-search/?q=&include=Removed&exportCSV=1', charity_register) ## download today's Charity Register

def get_charity_nums(charity_reg_file, charity_nums):
    f = open(charity_reg_file, 'rU' )
    for line in f:
        cells = line.split( "," )
        charity_nums.append((cells[0]))
    f.close()

    ## write each charity no. to the SqliteDB as PRIMARY KEYs
    for nic in charity_nums[1:]:
        try:
            c.execute("INSERT OR IGNORE INTO {tn} (id) VALUES ({nic})".\
                format(tn=table_name, nic=nic))
            conn.commit()
        except sqlite3.IntegrityError:
            print('ERROR: ID already exists in PRIMARY KEY column {}'.format(nic))

def get_fieldnames():
    # get fieldnames first from the FIELDS_xPATH file
    fieldnames = []
    xpaths = []
    with open('fields_xpath.txt', 'r') as fieldfile:
        reader = csv.DictReader(fieldfile)
        for row in reader:
            fieldnames.append(row['Field'])
            xpaths.append(row['xPath'])
            ## write each field heading to the SqliteDB so we can write later
            try:
                c.execute("ALTER TABLE {tn} ADD COLUMN '{cn}'"\
                    .format(tn=table_name, cn=row['Field']))
                conn.commit()
            except:
                pass

    pairings = list(zip(fieldnames, xpaths))
    return(pairings)

def scrape_write_data(charity_num, page, pairings):
    tree = html.fromstring(page.content)
    # go through each field in the xPath set and scrape data
    if page.status_code == requests.codes.ok:
        for field in pairings:
            fieldvalue = tree.xpath(field[1])
            newfieldvalue = []
            for listitem in fieldvalue:
                listitem.replace(' +',' ') # doesn't seem to be doing as expected
                newfieldvalue.append(listitem.strip().replace('\n','').replace('\r',''))
            item = '|'.join(newfieldvalue).strip().lstrip('|').rstrip('|')

            c.execute("UPDATE {tn} SET {cn}=(:what) WHERE id=:nic".format(tn=table_name, cn=field[0]), {'what': item, "nic": int(charity_num)})
            conn.commit()
        print('updated entry for', charity_num)
    else:
        page.raise_for_status()

# query each charity html page
def query_charities(charity_nums, pairings):
    for ccnino in charity_nums[1:]:
        query = {'regid':ccnino,'subid':'0'}
        try:
            page = requests.get(url, params=query)
            scrape_write_data(ccnino, page, pairings)
        except requests.exceptions.Timeout:
            print('connection timed out:', ccnino)
            errors.append(ccnino)
            continue
        except requests.exceptions.RequestException as e:
            print('Request Exception on',ccnino,':',e)
            errors.append(ccnino)
            continue

if __name__ == '__main__':
    url = "http://www.charitycommissionni.org.uk/charity-details"
    download_day = date.today().strftime('%Y%m%d')
    charity_reg_file = 'data/ccni' + download_day + '.csv'
    scraped_data = 'data/ccni_scraped' + download_day + '.csv'
    sqlite_file = 'data.sqlite' ## morph.io requirement
    table_name = 'data' ## morph.io requirement
    charity_nums = []
    errors = []
    pairings = get_fieldnames()

    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()
    c.execute('CREATE TABLE IF NOT EXISTS ' + table_name + '(id INTEGER PRIMARY KEY)')
    conn.commit()

    download_reg()
    get_charity_nums(charity_reg_file, charity_nums)
    get_fieldnames()
    query_charities(charity_nums, pairings)

    if len(errors) > 0:
        print('Data scraped from Charity Commission, except for these numbers:\n')
        for no in errors:
            print(no)
    else:
        print('Data scraped from Charity Commission for',len(charity_nums),'entries')

    conn.close()
