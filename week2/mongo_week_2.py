import datetime

import pandas as pd
import pymongo
import os
import multiprocessing

from pandas.io.parsers import TextFileReader

OUTPUT_PATH = 'D:\\广商网课\\resource\\Covid-19\\'
def connect():
    client = pymongo.MongoClient("mongodb://localhost:27017")
    db = client['MIT']
    collection = db['test']

    return collection


'''
    @:param df: Input the DataFrame which needs to split into multi smaller file
    @:param num: How many file
    @:param size: How many lines per file, default 1000000.
'''


def split_csv(df, num, size=1000000):
    if isinstance(df, TextFileReader):
        for i in range(num):
            print("{}/{}".format((i + 1), num))
            filename = OUTPUT_PATH + 'covid-19-{}.csv'.format(i + 1)
            chunk = df.get_chunk(size)
            chunk.to_csv(filename)
    else:
        raise Exception("'df' parameter needs TextFileReader type")


# split_csv(dataset, 20)


def file_paths():
    file_path = os.listdir(OUTPUT_PATH)
    for i in range(len(file_path)):
        file_path[i] = OUTPUT_PATH + file_path[i]

    return file_path


def insert_to_mongo(filepath):
    print("Receive file path: ", filepath)
    chunk = pd.read_csv(filepath)
    con = connect()
    print("Connected and Inserting.........")
    con.insert_many(chunk.to_dict(orient='records'))

    print("Successful!")


if __name__ == '__main__':

    # dataset = pd.read_csv("D:\\下载\\COVID-19_Case_Surveillance_Public_Use_Data_with_Geography.csv", iterator=True,
    #                       low_memory=False)
    # split_csv(dataset, 20, 1000000)

    pool = multiprocessing.Pool(processes=4)
    files = file_paths()[2:-2]
    for file in files:
        pool.apply_async(insert_to_mongo, (file, ))

    start = datetime.datetime.now()
    print("--------->Start<----------")
    pool.close()
    pool.join()

    end = datetime.datetime.now()
    print("--------->Finish<----------")
    print('------> Spent Time: {} <-----------'.format(end - start))
