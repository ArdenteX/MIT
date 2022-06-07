import os

import pandas as pd
from taohongxu.DataAnalysis.analyze_week_2 import _merge_and_plus


class AnalyzeDeath:

    def __init__(self, path):
        self.path = path

    def convert_data(self):
        files = os.listdir(self.path)
        for i in range(len(files)):
            files[i] = self.path + files[i]

        results = {}
        for file in files:
            data = pd.read_csv(file, index_col=[0])

            # death.html.index = death.html.index.astype(str)

            d_dict = data.to_dict(orient='index')

            result = {}

            for key, values in zip(d_dict.keys(), d_dict.values()):
                result[key] = values['death_yn']

            _merge_and_plus(results, result)

        n = max(results.values())
        count = 0
        m = []

        for i in range(5):
            count = count + (n / 5)
            m.append(int(count))

        return results, m


class ConfirmedAnalyze:

    def __init__(self, path):
        self.path = path

    def convert_data(self):
        files = os.listdir(self.path)
        for i in range(len(files)):
            files[i] = self.path + files[i]

        results = {}
        for file in files:
            data = pd.read_csv(file, index_col=[0])

            # death.html.index = death.html.index.astype(str)

            d_dict = data.to_dict(orient='index')

            result = {}

            for key, values in zip(d_dict.keys(), d_dict.values()):
                result[key] = values['death_yn']

            _merge_and_plus(results, result)

        return results


class deathRate:
    def __init__(self, path_confirmed, path_death):

        self._path_confirmed = path_confirmed
        self._path_death = path_death

    def convert_data(self):

        a = ConfirmedAnalyze(self._path_confirmed)
        c = AnalyzeDeath(self._path_death)
        b = a.convert_data()
        d, m = c.convert_data()

        confirmed = pd.Series(b, index=b.keys())
        death = pd.Series(d, index=d.keys()).sort_index()

        death_rate = (death / confirmed) * 100
        death_rate = death_rate.to_dict()
        return death_rate, confirmed.to_dict()


class AsymptomaticRate:

    def __init__(self, as_path):
        self._as_path = as_path

    def convert_data(self):
        asymptomatic = []
        status = []

        files = os.listdir(self._as_path)
        for i in range(len(files)):
            tmp = files[i].split('-')
            if 'Asymptomatic.csv' in tmp[-1]:
                asymptomatic.append(self._as_path + files[i])
            elif 'Status.csv' in tmp[-1]:
                status.append(self._as_path + files[i])

        as_results = {}
        st_results = {}
        for a, b in zip(asymptomatic, status):
            data = pd.read_csv(a, index_col=[0])
            data_st = pd.read_csv(b, index_col=[0])

            d_dict = data.to_dict(orient='index')
            d_dict_st = data_st.to_dict(orient='index')

            result = {}
            result_st = {}

            for key, values, key_st, values_st in zip(d_dict.keys(), d_dict.values(),
                                                      d_dict_st.keys(), d_dict_st.values()):
                result[key] = values['symptom_status']
                result_st[key_st] = values_st['symptom_status']

            _merge_and_plus(as_results, result)
            _merge_and_plus(st_results, result_st)
        rate = pd.Series(as_results) / pd.Series(st_results) * 100
        rate = rate.to_dict()

        return rate


# a = AsymptomaticRate('D:\\广商网课\\resource\\Covid-19-result\\Asymptomatic\\')
# r = a.convert_data()
# confirmed = 'D:\\广商网课\\resource\\Covid-19-result\\confirmed\\'
# death = 'D:\\广商网课\\resource\\Covid-19-result\\death\\'
#
# g = AnalyzeDeath(death)
# b, m = g.convert_data()
# a = deathRate(confirmed, death)
# c, d = a.convert_data()

#
#input_path = 'D:\\广商网课\\resource\\Covid-19\\covid-19-1.csv'
# df = pd.read_csv(input_path).iloc[:, 1:]
# df = df.loc[:, ['case_month', 'death_yn']]
# death = df[(df['death_yn'] == 'Yes')]
#
# death = death.groupby('case_month').count()
#
# if 'Asymptomatic' in df['symptom_status'].unique():
#     df = df[(df['symptom_status'] == 'Symptomatic') | (df['symptom_status'] == 'Asymptomatic')]
#     # Symptomatic = df[(df['symptom_status'] == 'Symptomatic')]
#     Asymptomatic = df[(df['symptom_status'] == 'Asymptomatic')]
#
#     count_As = Asymptomatic.groupby('case_month').count()
#     count_Df = df.groupby('case_month').count()
#
#     as_index = count_As.index
#     df_index = count_Df.index
#
#     for index in df_index:
#         if index not in as_index:
#             print(index)
#             count_As.loc[index, 'symptom_status'] = 0
#             continue
#
#     count_as_rate = (count_As.astype(int) / count_Df) * 100
#
#
# # It's hard to have a conclusion about the relationship between the two status
# # Maybe the Asymptomatic status rate can show the change efficiently
#
# plt.title('my lines example') #写上图题
# plt.xlabel('x') #为x轴命名为“x”
# plt.ylabel('y') #为y轴命名为“y”
# plt.tick_params(labelsize = 20) #设置刻度字号
# plt.plot(list(count_SY.to_dict()['symptom_status'].values())) #第一个data表示选取data为数据集，第二个是函数，data的平方
# plt.plot(list(count_As.to_dict()['symptom_status'].values())) #同上
# plt.legend(['Symptomatic', 'Asymptomatic']) #打出图例
# plt.show() #显
#
# dict_ = list(count_As.to_dict()['symptom_status'].values())



