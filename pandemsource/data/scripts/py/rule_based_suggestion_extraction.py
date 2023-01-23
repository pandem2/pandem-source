#!/usr/bin/env python
# -*-coding:utf-8 -*-
'''
@Author  :   Sampritha Manjunath 
@File    :   extract_span
@Time    :   2022/06/13 12:37:13
@Desc    :   None
@ref     :   https://colab.research.google.com/drive/16hhNzOIczFUOnOfHXwAkNGzIvc5LBx06#scrollTo=WAUdAoqajdsq

'''

import pandas as pd
import numpy as np
import re
import typing
import csv
import io

class Config:
    trails = ("should", "could ", "would", "option", "ensure", "how about" "allow", "think", "wish",
              "please", "enable", "include", "need", " able", "needs", "better", "suggest", "propose", "provide", "add",
              "detect", "hope", "must")
    ends = ("or", "for example", "by", "unless", "", " :", "\(", "but", ",", "!")

    begining_words = ['you', 'your', 'would', 'please', 'great', 'this', 'tell', 'have', 'been',
                      'much', 'wish', 'also', 'is', 'like', 'to', 'be', 'that', 'if', 'ing', 'ity', 'ed', 'a', 'one']
    topic_clusters = """Cluster Terms,Centre Term
"health worker,chief medical officer,icu,healthcare worker,health care worker,doctor",healthcare worker
"covid vaccine mandate,vaccine mandate,vaccine rollout",vaccine mandate
"hospital,nursing home,care home",care home
"deaths per day,cause of death,risk of death,number of death,coronavirus death toll,death rate,death toll,covid death rate",death rate
"social distancing rule, social distancing, work from home, lockdown",social distancing rule
"positive for corona,positive covid test,positivity rate,positive for covid,positive case",positive for covid
"covid,covid infection,spread of covid,covid crisis,second wave,covid relief,covid death,long covid,delta variant,death from covid,wave of covid,covid as weapon,covid response,covid restriction",covid
"nhs,health minister,health official,world health organization,public health expert,public health,centers for disease control,mental health,ministry of health,venues of concern,department of health,health care,public health england,health,public health official, cdc, long term effect, side effect",public health
"coronavirus task force,virus,corona virus,new coronavirus case,new cases of coronavirus,coronavirus death,coronavirus outbreak,spread of coronavirus,coronavirus case,coronavirus,coronavirus infection,cases of coronavirus,tests positive for coronavirus,positive for coronavirus,coronavirus pandemic",coronavirus
"new cases of covid,cases per day,number of covid case,case,cases in india,new covid case,active case,number of case,covid case,cases of covid,confirmed cases of covid, covid patient",covid case
"vaccine,vaccination,flu shot,coronavirus vaccine,pfizer vaccine,vaccine side effect,proof of vaccination,covid vaccination,covid vaccine,flu vaccine,vaccinated person,vaccine passport",vaccine
"covid relief package,relief bill,american rescue plan,covid relief bill,relief fund",relief bill
"covid test,testing,pcr test,negative covid test,covid testing",covid test
"govt,supreme court,government",govt
"fight against corona,corona,corona update in india,corona case,spread of corona",corona
"pandemic,pandemic response,quarantine,global pandemic",pandemic
"face mask,face shield,mask,mask mandate, ppe",mask
"vaccine dose,doses of vaccine,dose of vaccine,second dose,covid vaccine dose",dose of vaccine
"blood clot, cough, flu",flu
"student, school, exam",school
"immune system, herd immunity",immune system
"job , business",business
"patient, people with disability, pregnant woman",patient
"""
config = Config

class DataPreprocessing:
    def __init__(self) -> None:
        pass
    
    def clean_tweet(self, tweet):
        if type(tweet) == float:
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

    def load_and_clean_data(self, df):
        # print('-------------------------------------------------------')
        # print(df.head())
        # read tweet text column and sugg_label column
        df = df.iloc[: , 1:]
        df.columns = ['sentence','label']
        df['cleaned_sentence'] = df['sentence'].apply(lambda x: self.clean_tweet(x))
        # print()
        # print('Total number of tweets: ', len(df))
        # print('-------------------------------------------------------')
        # print('Getting tweets that are marked as containing suggestion by SMA tool')
        # print()
        sugg_df = df[df.label == 1]
        sugg_df = sugg_df.drop('label', axis=1)
        # print('Total tweets that might contain suggestions: ', len(sugg_df))
        # print('-------------------------------------------------------')

        for i, text in enumerate(sugg_df['cleaned_sentence']):
            if any(trail in text for trail in config.trails):
                continue
            else:
                sugg_df['cleaned_sentence'][i] = None

        # keep only rows that contain suggestion words
        #sugg_df = sugg_df.dropna()
        #sugg_df = sugg_df.reset_index()

        # print('Total tweets that contain "suggestion words" in it: ', len(sugg_df))
        # print(sugg_df.head())
        return sugg_df





class ExtractSpan:
    def __init__(self):
        self.dataprep = DataPreprocessing()
        # print(self.dataprep.clean_tweet('Suggestions are:\n 1. You should wake up early \n 2. You should go to bed on time'))
    
    def include_terms(self):
        terms = set()
        csv_text = io.StringIO()
        csv_text.write(config.topic_clusters)
        csv_text.seek(0)
        df = pd.read_csv(csv_text)
        terms = set()

        for i in range(len(df)):
          terms.add(df['Centre Term'][i].lstrip())
          for x in df['Cluster Terms'][i].split(','):
            terms.add(x.lstrip())
        
        return terms


    def find_between(self, s, first, last):
        try:
            start = s.index(first)  # + len( first )
            # end = s.index( last, start )
            s = s[start:]
            a = re.search(r'\b' + last + r'\b', s)
            if a is None:
                end = len(s)
            else:
                end = a.start()
            return s[:end]
        except ValueError:
            return ""

    def find_between_r(self, s, first, last):
        try:
            start = s.rindex(first)  # + len( first )
            # end = s.rindex( last, start )
            s = s[start:]
            a = re.search(r'\b' + last + r'\b', s)
            if a is None:
                end = len(s)
            else:
                end = a.start()
            return s[:end]
        except ValueError:
            return ""

    def findShortest(self, lst, trails, terms):
        length = len(lst)
        short = len(lst[0])
        ret = lst[0]
        for x in range(1, length):
            if (len(lst[x]) < short) and (len(lst[x].split()) > 3) and any(
                    trail in lst[x].split()[0] for trail in trails) and any(
                        term in lst[x].split()[0] for term in terms):
                short = len(lst[x])
                ret = lst[x]

        if len(ret.split()) > 2:
            return ret
        else:
            return ''

    def remove_invalid_parentheses(self, s):
        removed = 0
        results = {s}
        count = {"(": 0, ")": 0}
        for i, c in enumerate(s):
            if c == ")" and count["("] == count[")"]:
                new_results = set()
                while results:
                    result = results.pop()
                    for j in range(i - removed + 1):
                        if result[j] == ")":
                            new_results.add(result[:j] + result[j + 1:])
                results = new_results
                removed += 1
            elif c in count:
                count[c] += 1
        count = {"(": 0, ")": 0}
        ll = len(s) - removed
        for i in reversed(range(ll)):
            c = s[i + removed]
            if c == "(" and count["("] == count[")"]:
                new_results = set()
                while results:
                    result = results.pop()
                    for j in range(i, ll):
                        if result[j] == "(":
                            new_results.add(result[:j] + result[j + 1:])
                results = new_results
                ll -= 1
            elif c in count:
                count[c] += 1
        return results.pop()
    
    def tagging_suggestion(self, final_result, corpus):
        tagging = []

        for i, res in enumerate(final_result):
            # print('\n****************************\n')
            # print(i)
            tag = []
            # if there is a suggestion in input sentence
            if res:
                # print('----------------------------------')
                res = ''.join(self.remove_invalid_parentheses(res))
                corpus[i] = ''.join(self.remove_invalid_parentheses(corpus[i]))

                try:
                    # print(i)
                    if re.search(res, corpus[i]) is None:
                        # print(i)
                        tag = ['O'] * len(corpus[i].split())
                        # print(tag)
                        tagging.append(tag)
                        continue
                except:
                    tag = ['O'] * len(corpus[i].split())
                    tagging.append(tag)
                    continue 
                
                tag = ['O'] * len(corpus[i].split())
                start_idx = re.search(res, corpus[i]).span()[0]
                end_idx = re.search(res, corpus[i]).span()[1]
                corp_idx = [(m.start(), m.end()-1) for m in re.finditer(r'\S+', corpus[i])]
                
                sugg_idx = []
                for j, cidx in enumerate(corp_idx):
                    if cidx[0] == start_idx:
                        sugg_idx.append(j)
                    if (cidx[1] == end_idx -2) or (cidx[1] == end_idx-1) or ((cidx[1]+1) == end_idx) or (cidx[1]==end_idx):
                        sugg_idx.append(j)
                if len(sugg_idx) < 2:
                    tag = ['O'] * len(corpus[i].split())
                    tagging.append(tag)
                    continue
                else:
                    for idx in range(sugg_idx[0], sugg_idx[1]+1):
                        if (idx == sugg_idx[0]):
                            tag[idx] = 'B'
                        else:
                            tag[idx] = 'I' 
                tagging.append(tag)

            # if no suggestion for input sentence, tag all as 'O'
            else:
                tag = ['O'] * len(corpus[i].split())
                tagging.append(tag)
    
        return tagging

    def get_span(self, sugg_df):
        result = []
        for i in range(len(sugg_df)):
            s = sugg_df['cleaned_sentence'][i]
            # s = 'i would like to be able to pull in bing news search results for a specified keyword'
            if s is None:
              result.append(None)
            else:
              s = s.lstrip()
              suggestions = []
              for j in config.trails:
                  for k in config.ends:
                      # get suggestions between suggestion word (left word priority) and end word (left word priority)
                      temp_sugg = self.find_between(s, j, k)
                      if len(temp_sugg) > 0:
                          sugg = temp_sugg
                          if (sugg not in suggestions) and (len(sugg.split()) >= 2):
                              # print(sugg)
                              suggestions.append(sugg)

                      # get suggestions between suggestion word (right word priority) and end word (right word priority)
                      temp_sugg = self.find_between_r(s, j, k)
                      if len(temp_sugg) > 0:
                          sugg = temp_sugg
                          if (sugg not in suggestions) and (len(sugg.split()) >= 2):
                              suggestions.append(sugg)
              result.append(suggestions)
        return result

    def filter_span(self, result):
        filtered_result = []
        for ele in result:
            sent = []
            if ele is None:
              filtered_result.append(None)
            else:
              for item in ele:
                  item = item.lstrip()
                  # clean the sentence by removing unwanted beginning words
                  while (item.split()[0] in config.begining_words) and (len(item.split()) > 1):
                      s = item.lstrip()
                      item = s.replace(s[:s.index(' ')], '', 1)
                      item = item.lstrip()
                  if item not in sent:
                      sent.append(item.lstrip())
              filtered_result.append(sent)
        return filtered_result

    def get_final_span(self, filtered_result):
        final_result = []
        for lst in filtered_result:
            if lst is not None and len(lst) > 0:
                s = self.findShortest(lst, config.trails, self.include_terms())
                s = self.remove_invalid_parentheses(s)
                final_result.append(s)
            else:
                final_result.append(lst)
        return final_result

    def get_span_from_text(self, text) -> typing.Optional[str]:
        s = text.lstrip()
        s = self.dataprep.clean_tweet(s)
        suggestions = []
        final_suggestion = None
        for j in config.trails:
            for k in config.ends:
                # get suggestions between suggestion word (left word priority) and end word (left word priority)
                temp_sugg = self.find_between(s, j, k)
                if len(temp_sugg) > 0:
                    sugg = temp_sugg
                    if (sugg not in suggestions) and (len(sugg.split()) >= 2):
                        # print(sugg)
                        suggestions.append(sugg)

                # get suggestions between suggestion word (right word priority) and end word (right word priority)
                temp_sugg = self.find_between_r(s, j, k)
                if len(temp_sugg) > 0:
                    sugg = temp_sugg
                    if (sugg not in suggestions) and (len(sugg.split()) >= 2):
                        suggestions.append(sugg)

            sent = []
            for item in suggestions:
                item = item.lstrip()
                # clean the sentence by removing unwanted beginning words
                while (item.split()[0] in config.begining_words) and (len(item.split()) > 1):
                    s = item.lstrip()
                    item = s.replace(s[:s.index(' ')], '', 1)
                    item = item.lstrip()
                if item not in sent:
                    sent.append(item.lstrip())

            if sent:
                s = self.findShortest(sent, config.trails, self.include_terms())
                s = self.remove_invalid_parentheses(s)
                final_suggestion = str(s)

        return final_suggestion

    def get_span_from_text_list(self, lst_text) -> typing.List[str]:
        return list(map(lambda x: self.get_span_from_text(x), lst_text))

    def get_span_from_df(self, df, with_tag=False):
        """
        input: CSV. If not provided will be picked from config.inputfile
        output: CSV file with suggestions included
        """
        
        df = self.dataprep.load_and_clean_data(df)

        # extract suggestions from the sentence
        result = self.get_span(df)

        # pick most suitable suggestion for a sentence
        filtered_span = self.filter_span(result)

        # clean the suggestion
        final_span = self.get_final_span(filtered_span)

        # if BIO annotaion is required as part of output
        if with_tag == True:
            corpus = df['cleaned_sentence']
            tag = self.tagging_suggestion(final_span, corpus)
            
            df = pd.DataFrame({
                   'Sentence':df['sentence'],
                   'Cleaned_stentence':df['cleaned_sentence'],
                   'Tag':tag,
                   'Suggestion':final_span
                 })

            # df = df[df.Suggestion != '[]']
            df.loc[df['Suggestion'] == '[]', 'Suggestion'] = None
            return df

        # default: if only suggestion is required as output
        else:
            # write result to csv
            df = pd.DataFrame({
                   'Sentence':df['sentence'],
                   'Cleaned_stentence':df['cleaned_sentence'],
                   'Suggestion':final_span
                 })

            #df = df[df.Suggestion != '[]']
            df.loc[df['Suggestion'] == '[]', 'Suggestion'] = None
            return df

def annotate(text, lang = None):
  extract = ExtractSpan()
  ix = [*range(0, len(text))]
  df = extract.get_span_from_df(pd.DataFrame({"index":ix, "text":text, "label":[1 for i in ix]}))
  return df["Suggestion"].to_list()
