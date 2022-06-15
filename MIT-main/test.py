import os
import pandas as pd

States = {
            'AL': 'Alabama',
            'AK': 'Alaska',
            'AZ': 'Arizona',
            'AR': 'Arkansas',
            'CA': 'California',
            'CO': 'Colorado',
            'CT': 'Connecticut',
            'DE': 'Delaware',
            'FL': 'Florida',
            'GA': 'Georgia',
            'HI': 'Hawaii',
            'ID': 'Idaho',
            'IL': 'Illinois',
            'IN': 'Indiana',
            'IA': 'Iowa',
            'KS': 'Kansas',
            'KY': 'Kentucky',
            'LA': 'Louisiana',
            'ME': 'Maine',
            'MD': 'Maryland',
            'MA': 'Massachusetts',
            'MI': 'Michigan',
            'MN': 'Minnesota',
            'MS': 'Mississippi',
            'MO': 'Missouri',
            'MT': 'Montana',
            'NE': 'Nebraska',
            'NV': 'Nevada',
            'NH': 'New Hampshire',
            'NJ': 'New Jersey',
            'NM': 'New Mexico',
            'NY': 'New York',
            'NC': 'North Carolina',
            'ND': 'North Dakota',
            'OH': 'Ohio',
            'OK': 'Oklahoma',
            'OR': 'Oregon',
            'PA': 'Pennsylvania',
            'RI': 'Rhode Island',
            'SC': 'South Carolina',
            'SD': 'South Dakota',
            'TN': 'Tennessee',
            'TX': 'Texas',
            'UT': 'Utah',
            'VT': 'Vermont',
            'VA': 'Virginia',
            'WA': 'Washington',
            'WV': 'West Virginia',
            'WI': 'Wisconsin',
            'WY': 'Wyoming',
            'VI': 'Virgin Islands',
            'PR': 'Puerto Rico',
            'GU': 'Guam',
            'DC': 'Washington D.C'
        }
FILES = ['asy_rate1', 'confirmed1', 'death_case', 'map1', 'pie1', 'rate1']
BASE = '/Users/xuhongtao/PycharmProjects/Resource/new/'


def read(path, target):
    files = os.listdir(path)
    for file in files:
        if file == '.DS_Store':
            continue

        if file == target:
            item = os.listdir(path + file)
            for i in item:
                if i == '_SUCCESS':
                    continue

                return path + file + '/' + i

    return "Not found"


file = read(BASE, FILES[0])

count_dict = pd.read_csv(file).sort_values('case_month').to_dict('list')

count_list = []

for key, value in zip(count_dict['res_state'], count_dict['count']):
    if key in States:
        key = States[key]
    tem = [key, value]
    count_list.append(tem)


