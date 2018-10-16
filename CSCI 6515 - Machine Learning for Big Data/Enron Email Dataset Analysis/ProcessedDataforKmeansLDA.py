import re
import os
import sys
from collections import OrderedDict
from itertools import islice

for route,directories,files in os.walk('/media/deepak/file_folder'):

    for file in files:
         f_name=os.path.join(route,file).split('/')      
         openfile=open(os.path.join(route,file), "r+")
         data_file=openfile.read()
         openfile.close()
         data_file=re.sub(r'\W+', ' ',data_file)
         data_file=data_file.lower()
         all_words=data_file.split(' ')
         d={}
         file_created="/media/deepak/"+f_name[-1]+"data_words.csv"   
         file_w=open(file_created, 'a+')
         for eachword in all_words:
            if eachword.__len__()>4:
                if eachword in d:
                  d[eachword]+=1
                else:
                  d[eachword]=1
         total_word_count=0
         k=0
         d=OrderedDict(sorted(d.items(),key=lambda x: x[1], reverse=True))
         for keys,values in d.items():
                total_word_count=values+total_word_count
                print(keys)
                print(values)
                k=k+1
                if k==300:
                   break
         k=0      
         for keys,values in d.items():
             prob_of_word_occuring=float(float(values)/total_word_count)
             print "%d%s%d,%f\n" % (len(keys),',',values,prob_of_word_occuring)
             file_w.write("%d%s%d,%f\n" % (len(keys),',',values,prob_of_word_occuring))
             k=k+1
             if k==300:
                break
         file_w.close()