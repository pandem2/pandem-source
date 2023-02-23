#!/usr/bin/env python
# -*-coding:utf-8 -*-
'''
@Author  :   Sampritha Manjunath 
@File    :   extract_span
@Time    :   2022/06/13 12:37:13
@Desc    :   None
@ref     :   https://colab.research.google.com/drive/16hhNzOIczFUOnOfHXwAkNGzIvc5LBx06#scrollTo=WAUdAoqajdsq

'''

import re
from .config import Config
import typing
from .data_preprocessing import DataPreprocessing
import csv
import pandas as pd

config = Config


class ExtractSpan:
    def __init__(self):
        self.dataprep = DataPreprocessing()
        # print(self.dataprep.clean_tweet('Suggestions are:\n 1. You should wake up early \n 2. You should go to bed on time'))
    
    def include_terms(self):
        terms = set()

        df = pd.read_csv(config.topic_clusters)
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
            if len(lst) > 0:
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

    def get_span_from_df(self, csv_file=config.input_file, with_tag=False):
        """
        input: CSV. If not provided will be picked from config.inputfile
        output: CSV file with suggestions included
        """
        
        df = self.dataprep.load_and_clean_data(csv_file)

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
            
            # write result to csv
            with open('output/Suggestion_dataset_with_tag.csv', 'w') as f:
                writer = csv.writer(f)
                writer.writerow(['Sentence','Cleaned_stentence','Tag','Suggestion'])
                writer.writerows(zip(df['sentence'], df['cleaned_sentence'], tag, final_span))
            f.close()

            df = pd.read_csv('output/Suggestion_dataset_with_tag.csv')
            df = df[df.Suggestion != '[]']
            # df.loc[df['Suggestion'] == '[]', 'Suggestion'] = None
            df.to_csv('output/Suggestion_dataset_with_tag.csv', index=False)

        # default: if only suggestion is required as output
        else:
            # write result to csv
            with open('output/Suggestion_dataset.csv', 'w') as f:
                writer = csv.writer(f)
                writer.writerow(['Sentence','Cleaned_stentence','Suggestion'])
                writer.writerows(zip(df['sentence'], df['cleaned_sentence'], final_span))
            f.close()

            df = pd.read_csv('output/Suggestion_dataset.csv')
            df = df[df.Suggestion != '[]']
            # df.loc[df['Suggestion'] == '[]', 'Suggestion'] = None
            df.to_csv('output/Suggestion_dataset.csv', index=False)
        print('***********************  DONE  ************************')


# sample usage
if __name__ == '__main__':
    e_span = ExtractSpan()
    eg_list = [
        'It is suggested that everyone should be vaccinated',
        'Suggestions are:\n1. You should wake up early \n2. You should go to bed on time',
        'It is suggested to wash your face before you sleep',
        'Yeah, nah!'
    ]
    # print(e_span.get_span_from_text(eg_list[0]))
    # expected result: 'should be vaccinated'
    print(e_span.get_span_from_text_list(eg_list))
    # expected result: ['should be vaccinated', 'should go to bed on time',
    #                   'suggested to wash your face before you sleep', None]

    # for the use in PANDEM-2
    """
    input: is passed from the SMA component. Output of SMA component is the input here
    output: CSV output with suggestions and BIO tags (optional)
    """
    e_span.get_span_from_df('data/test.csv')
    # expected result: csv
    # df['suggestion] = ["provide covid-19 relief to struggling americans, mitch mcconnell and republicans chose to jam throu", 
    #                       "visit: <url>       #medicalisolationtransformerinindia #eitransformermanufacturerinindia #transformermanufacturerinindia
                            #cableharness <url>", 
    #                       "could help stop the spread of coronavirus <url> via <user>",
    #                       "provide the suitable personal protective e",
    #                       "need federal aid for people, not billion dollar"]

    # takes input from config.input_file --> placed in the data folder
    
    e_span.get_span_from_df('data/test.csv', with_tag=True)
