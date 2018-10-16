#This has been taken and edited as per task from http://spark.apache.org/docs/latest/mllib-clustering.html#latent-dirichlet-allocation-lda

import os
import sys
from pyspark.mllib.clustering import LDA,LDAModel
from pyspark.mllib.linalg import Vectors
from pyspark import SparkContext

sc =SparkContext()
for route,directories,files in os.walk('/media/deepak/data_words.csv'):
    for file in files:
        f_path=os.path.join(route,file)
        f_name=os.path.join(route,file).split('/')
        # Load and parse the data
        data = sc.textFile(f_path)
        dataParsed = data.map(lambda line: Vectors.dense([float(x) for x in line.strip().split(',')]))
        # Index documents with unique IDs
        corpus = dataParsed.zipWithIndex().map(lambda x: [x[1], x[0]]).cache()
        # Cluster the documents into three topics using LDA
        ldaModel = LDA.train(corpus, k=3)
        # Output topics. Each is a distribution over words (matching word count vectors)
        print(f_name[-1])
        print("Learned topics (as distributions over vocab of " + str(ldaModel.vocabSize()) + " words):")
        topics = ldaModel.topicsMatrix()
        for topic in range(3):
            print("Topic " + str(topic) + ":")
            for word in range(0, ldaModel.vocabSize()):
                print(" " + str(topics[word][topic]))
        # Save and load model
        ldaModel.save(sc, "/media/deepak/lda_output/"+f_name[-1])
        sameModel = LDAModel.load(sc, "/media/deepak/lda_output/"+f_name[-1])