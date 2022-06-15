import os

import pandas as pd
from analyze_week_2 import _merge_and_plus
from analyze_week_2 import read, BASE, FILES


class AnalyzeDeath:

    def __init__(self):
        pass

    def convert_data(self):
        file = read(BASE, FILES[2])

        count_dict = pd.read_csv(file).sort_values('case_month').to_dict('list')

        n = max(count_dict['count'])
        count = 0
        m = []

        for i in range(5):
            count = count + (n / 5)
            m.append(int(count))

        return count_dict, m


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
    def __init__(self):

        pass

    def convert_data(self):
        file = read(BASE, FILES[1])
        file_2 = read(BASE, FILES[-1])

        count_dict = pd.read_csv(file).sort_values('case_month').to_dict('list')
        count_dict_2 = pd.read_csv(file_2).sort_values('case_month').to_dict('list')
        return count_dict_2, count_dict


class AsymptomaticRate:

    def __init__(self):
        pass

    def convert_data(self):
        file = read(BASE, FILES[0])

        count_dict = pd.read_csv(file).sort_values('case_month').to_dict('list')

        return count_dict


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



