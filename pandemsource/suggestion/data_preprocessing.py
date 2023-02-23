#!/usr/bin/env python
# -*-coding:utf-8 -*-
'''
@Author  :   Sampritha Manjunath 
@File    :   data_preprocessing.py
@Time    :   2022/06/09 17:02:30
@Desc    :   None
'''

import pandas as pd
import numpy as np
import re

from .config import Config

config = Config


class DataPreprocessing:
    def __init__(self) -> None:
        pass
    
    def clean_tweet(self, tweet):
        if type(tweet) == np.float:
            return ""
        temp = tweet.lower()
        temp = re.sub("[\'\"]", "", temp) # to avoid removing contractions in english
        temp = re.sub("@[A-Za-z0-9_]+","<user>", temp)
        temp = re.sub(r'http\S+', '<url>', temp)
        temp = temp.replace('\\n','')
        temp = re.sub('\n','', temp)
        temp = temp.split()
        temp = " ".join(word for word in temp)
        # print(temp)
        return temp

    def load_and_clean_data(self, input_file=config.input_file):
        print('-------------------------------------------------------')
        print('Reading input file')
        df = pd.read_csv(input_file)
        # print(df.head())
        # read tweet text column and sugg_label column
        df = df.iloc[: , 1:]
        df.columns = ['sentence','label']
        df['cleaned_sentence'] = df['sentence'].apply(lambda x: self.clean_tweet(x))
        print()
        print('Total number of tweets: ', len(df))
        # print('-------------------------------------------------------')
        print('Getting tweets that are marked as containing suggestion by SMA tool')
        print()
        sugg_df = df[df.label == 1]
        sugg_df = sugg_df.drop('label', axis=1)
        print('Total tweets that might contain suggestions: ', len(sugg_df))
        # print('-------------------------------------------------------')

        for i, text in enumerate(sugg_df['cleaned_sentence']):
            if any(trail in text for trail in config.trails):
                continue
            else:
                sugg_df['cleaned_sentence'][i] = None

        # keep only rows that contain suggestion words
        sugg_df = sugg_df.dropna()
        sugg_df = sugg_df.reset_index()

        print('Total tweets that contain "suggestion words" in it: ', len(sugg_df))
        # print(sugg_df.head())
        return sugg_df


if __name__ == '__main__':
    data_preprocessing = DataPreprocessing()

    print(data_preprocessing.load_and_clean_data())
