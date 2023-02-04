#!/usr/bin/env python
# -*-coding:utf-8 -*-
'''
@Author  :   Sampritha Manjunath 
@File    :   config.py
@Time    :   2022/06/09 17:02:42
@Desc    :   None
'''
import pkg_resources

# import os
#
# if not os.path.exists('./output'):
#     os.makedirs('./output')


class Config:
    topic_clusters = pkg_resources.resource_filename("pandemsource", "data/suggestion/topic_clusters.csv")
    input_file = ""
    trails = ("should", "could ", "would", "option", "ensure", "how about" "allow", "think", "wish",
              "please", "enable", "include", "need", " able", "needs", "better", "suggest", "propose", "provide", "add",
              "detect", "hope")
    ends = ("or", "for example", "by", "unless", "", " :", "\(")

    begining_words = ['you', 'your', 'would', 'please', 'great', 'this', 'tell', 'have', 'been',
                      'much', 'wish', 'also', 'is', 'like', 'to', 'be', 'that', 'if', 'ing', 'ity', 'ed', 'a', 'one']
