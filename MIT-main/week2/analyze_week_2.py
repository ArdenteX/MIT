import datetime
import pandas as pd
import os
from multiprocessing import Pool

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


def _merge_and_plus(a, b):
    for key, value in zip(b.keys(), b.values()):
        # '2020/10/1'
        if '/' in key:
            tmp = key.split('/')
            if int(tmp[-2]) >= 10:
                tmp_1 = tmp[0] + '-' + tmp[1] + '-0' + tmp[2]
                b[tmp_1] = b.pop(key)
                key = tmp_1
                del tmp_1

            else:
                tmp_1 = key.replace('/', '-0')
                b[tmp_1] = b.pop(key)
                key = tmp_1
                del tmp_1

        if key in a.keys():
            a[key] = a[key] + b[key]

        else:
            a[key] = b[key]


def _data_standardization(count_dict):
    if isinstance(count_dict, dict) is False:
        raise TypeError

    max_data = max(count_dict.values())
    min_data = min(count_dict.values())

    count_list = []
    for key, value in zip(count_dict.keys(), count_dict.values()):
        tem = [key, value]
        count_list.append(tem)

    return max_data, min_data, count_list


'''
    Describe: Plot the America map to show the covid-19 case (map)
'''


class PlotMap:

    def __init__(self):
        self._States = {
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
        self.OUTPUT_PATH = '/Users/xuhongtao/PycharmProjects/Resource/new/map1/'

    def convert_data(self):
        file = read(BASE, FILES[3])

        count_dict = pd.read_csv(file).to_dict(orient='list')

        count_list = []

        '''
            The Map of the United States requires the complete states' names, 
            but the name in the dataset are all the abbreviations
            Create the reflect table named "States" to change the abbreviation into the full name
        '''

        for key, value in zip(count_dict['res_state'], count_dict['count']):
            if key in self._States:
                key = self._States[key]
            tem = [key, value]
            count_list.append(tem)

        # Obtain the max value and the lowest value
        max_data = max(count_dict['count'])
        min_data = min(count_dict['count'])
        return max_data, min_data, count_list


class PlotPie:

    def __init__(self):
        pass

    def convert_data(self):
        file = read(BASE, FILES[-2])

        count_dict = pd.read_csv(file).to_dict(orient='list')

        max_data = max(count_dict['count'])
        min_data = min(count_dict['count'])

        count_list = []
        for key, value in zip(count_dict['age_group'], count_dict['count']):
            tem = [key, value]
            count_list.append(tem)

        return max_data, min_data, count_list


class PlotBar:

    def __init__(self, OUTPUT_PATH):
        self._OUTPUT_PATH = OUTPUT_PATH

    def convert_date(self):
        files = os.listdir(self._OUTPUT_PATH)
        for i in range(len(files)):
            files[i] = self._OUTPUT_PATH + files[i]

        count_dict = {}
        for file in files:
            data = pd.read_csv(file, index_col=[0])
            tmp = data.to_dict()['case_month']
            _merge_and_plus(count_dict, tmp)

        return _data_standardization(count_dict)


class Goat:
    """
        @:param INPUT_PATH: The directory store the CSV file which will be processed
        @:param OUTPUT_PATH_AFTER: The directory store the CSV file which been processed
    """

    def __init__(self, INPUT_PATH, OUTPUT_PATH_AFTER):
        self._OUTPUT_PATH_AFTER = OUTPUT_PATH_AFTER
        self._INPUT_PATH = INPUT_PATH

        if os.path.exists(self._OUTPUT_PATH_AFTER) is False:
            print(datetime.datetime.now(), " Output Directory not exists, creating...")
            os.mkdir(self._OUTPUT_PATH_AFTER)
            print(datetime.datetime.now(), ' Create Successful')

        if os.path.exists(self._INPUT_PATH) is False:
            raise Exception("Directory is not exists!")

    """
        @:param file: The dataset
        @:param feature: Which features will be processed
        @:param suffix: Special suffix before '.csv'
    """

    def _counts(self, file, feature, suffix, date_start, date_end):
        filename = self._OUTPUT_PATH_AFTER + file.split("\\")[-1].split(".")[0] + '_{}.csv'.format(suffix)
        if os.path.exists(filename):
            print(datetime.datetime.now(), " File Exists")
        elif file != self._OUTPUT_PATH_AFTER:
            print(datetime.datetime.now(), " Received file: ", file)
            data = pd.read_csv(file, date_parser=[1])
            '''
                Update: Add the date limit
            '''
            if date_end != 'None' and date_start == 'None':
                data = data[(data['case_month'] <= date_end)]

            elif date_start != 'None' and date_end != 'None':
                data = data[(data['case_month'] >= date_start) & (data['case_month'] < date_end)]

            elif date_start != 'None' and date_end == 'None':
                data = data[(data['case_month'] >= date_start)]

            data = data[feature].value_counts()
            data.to_csv(filename)
            print(datetime.datetime.now(), " Write Successful")

    def _group_by(self, file, suffix, feature, judgement, judgement_2):
        filename = self._OUTPUT_PATH_AFTER + file.split("\\")[-1].split(".")[0] + '_{}.csv'.format(suffix)
        if os.path.exists(filename):
            print(datetime.datetime.now(), " File Exists")
        elif file != self._OUTPUT_PATH_AFTER:
            print(datetime.datetime.now(), " Received file: ", file)
            dataset = pd.read_csv(file).iloc[:, 1:]

            dataset['case_month'] = pd.to_datetime(dataset['case_month'])

            if judgement_2 == '':

                death = dataset[(dataset[feature] == judgement)]

                death = death.loc[:, ['case_month', feature]].groupby('case_month').count()
                death.to_csv(filename)
                print(datetime.datetime.now(), " Write Successful")

            else:
                death = dataset[(dataset[feature] == judgement) | (dataset[feature] == judgement_2)]
                death = death.loc[:, ['case_month', feature]].groupby('case_month').count()
                death.to_csv(filename)
                print(datetime.datetime.now(), " Write Successful")

    def _calculate(self, file, suffix, suffix_2):
        filename_1 = self._OUTPUT_PATH_AFTER + file.split("\\")[-1].split(".")[0] + '_{}.csv'.format(suffix)
        filename_2 = self._OUTPUT_PATH_AFTER + file.split("\\")[-1].split(".")[0] + '_{}.csv'.format(suffix_2)
        if os.path.exists(filename_1) or os.path.exists(filename_2):
            print(datetime.datetime.now(), " File Exists")
        elif file != self._OUTPUT_PATH_AFTER:
            print(datetime.datetime.now(), " Received file: ", file)
            df = pd.read_csv(file).iloc[:, 1:]
            df = df.loc[:, ['case_month', 'symptom_status']]

            if 'Asymptomatic' in df['symptom_status'].unique():
                df = df[(df['symptom_status'] == 'Symptomatic') | (df['symptom_status'] == 'Asymptomatic')]
                # Symptomatic = df[(df['symptom_status'] == 'Symptomatic')]
                asymptomatic = df[(df['symptom_status'] == 'Asymptomatic')]

                count_As = asymptomatic.groupby('case_month').count()
                count_Df = df.groupby('case_month').count()

                as_index = count_As.index
                df_index = count_Df.index

                for index in df_index:
                    if index not in as_index:
                        print(index)
                        count_As.loc[index, 'symptom_status'] = 0
                        continue

                count_As.to_csv(filename_1)
                count_Df.to_csv(filename_2)
                print(datetime.datetime.now(), " Write Successful")

    def multi_process(self, suffix, suffix_2='', feature='None',  date_start='None', date_end='None', methods='_count'
                      , judgement='Yes', judgement_2=''):
        if feature == 'None' and methods == '_count':
            raise Exception("_count() needs feature")

        if methods == '_calculate' and suffix_2 == '':
            raise Exception("_calculate needs suffix_2 param!")

        files = os.listdir(self._INPUT_PATH)
        for i in range(len(files)):
            files[i] = self._INPUT_PATH + files[i]

        pool = Pool(processes=4)
        if methods == '_count':
            for file in files:
                pool.apply_async(self._counts, (file, feature, suffix, date_start, date_end, ))

        if methods == '_groupBy':
            for file in files:
                pool.apply_async(self._group_by, (file, suffix, feature, judgement, judgement_2, ))

        if methods == '_calculate':
            for file in files:
                pool.apply_async(self._calculate, (file, suffix, suffix_2, ))

        start = datetime.datetime.now()

        print(start, " Start")
        pool.close()
        pool.join()

        end = datetime.datetime.now()
        print(end, " Finish, Spent time: ", end - start)


if __name__ == '__main__':
    input_path = 'D:\\广商网课\\resource\\Covid-19\\'
    out_path = 'D:\\广商网课\\resource\\Covid-19\\pie\\'
    out_path_2 = 'D:\\广商网课\\resource\\Covid-19\\after\\'
    out_path_3 = 'D:\\广商网课\\resource\\Covid-19\\bar\\'
    out_path_4 = 'D:\\广商网课\\resource\\Covid-19-result\\Asymptomatic\\'
    out_path_5 = 'D:\\广商网课\\resource\\Covid-19-result\\Confirmed\\'

    goat = Goat(input_path, out_path_5)
    # goat.multi_process('res_state', 'after')
    # goat.multi_process(methods='_calculate', suffix='Asymptomatic', suffix_2='Status')
    goat.multi_process(methods='_groupBy', suffix='confirmed', feature='death_yn', judgement='Yes', judgement_2='No')

# input_path = 'D:\\广商网课\\resource\\Covid-19\\covid-19-1.csv'
# out_path_5 = 'D:\\广商网课\\resource\\Covid-19-result\\Confirmed\\'
# goat = Goat(input_path, out_path_5)
# goat.group_by(input_path, suffix='confirmed', feature='death_yn', judgement='Yes', judgement_2='No')
#
# dataset = pd.read_csv(input_path)
# death = dataset[(dataset['death_yn'] == 'Yes') | (dataset['death_yn'] == 'No')]
# group = death.groupby('case_')

# input_path = 'D:\\广商网课\\resource\\Covid-19\\'
# path = 'D:\\广商网课\\resource\\Covid-19\\bar\\covid-19-20_bar.csv'
# df = pd.read_csv(path, index_col=[0])
# s = df.iloc[:, :].to_dict()
# pie = PlotPie(input_path, out_path).convert_data()
# m, n, bar = PlotBar('D:\\广商网课\\resource\\Covid-19\\bar\\').convert_date()

