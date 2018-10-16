import nltk
import os
import time
import re
import pandas as pd
from nltk.corpus import stopwords
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from email.parser import Parser
from nltk.stem.porter import *
from stemming.porter2 import stem

f_name=""
check='True'
a=-1
for route,directories,files in os.walk('/media/deepak/temp/'):
   if os.path.dirname(os.path.dirname(route))=='/media/deepak/temp/':
      counter_words=0
      counter_emails=0      
   for file in files:
      main_path=os.path.dirname(os.path.join(route,file))
      for check in 'True':
        if os.path.dirname(main_path)=='/media/deepak/temp/':
           break
        main_path=os.path.dirname(main_path)
      f_name=main_path.split('/')
      main_path=main_path+".txt"
      openFile=open(main_path,'a+')
      with open(os.path.join(route,file)) as fl:
         for lines in fl:
             counter_words=counter_words+lines.__len__()
             lines=re.sub('Subject:','',lines)
             lines=re.sub('(http(s)?://)?www\..*|[^a-zA-Z0-9 ]|[\w\-\W]+:.*|[?|$|-|.]|.*Forwarded by.*|The following section of this message contains a file attachment.*|.*Internet MIME message format.*|.*MIME-compliant system.*|.*save it or view it from within your mailer.*|.*please ask your system administrator.*|\-+ [\w\d\s]+ \-+|\=+ [\w\d\s]+ \=+|\-\s*[\d\-\w\W]+.\w+','',lines)
             stopping_words_removed=" ".join([word for word in lines.split() if word not in set(stopwords.words('english'))])
             print stopping_words_removed                    
             #stemmed_line =" ".join([stem(word) for word in stop_line.split()])
             #print stemmed_line
             openFile.write(stopping_words_removed)
      openFile.close()
      counter_emails+=1
      f_name=f_name[-1]
   if os.path.dirname(route)=='/media/deepak/temp/':
      a+=1
      if counter_words>0 and a>0:
         print("email sent count is %d,name is %s,Average length of an email is %d\n" % (counter_emails,f_name,counter_words/counter_emails))