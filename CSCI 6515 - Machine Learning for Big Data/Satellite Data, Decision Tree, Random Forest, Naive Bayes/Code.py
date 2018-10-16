from sklearn import tree,ensemble,naive_bayes
from sklearn.model_selection import train_test_split,cross_val_score
from sklearn.metrics import confusion_matrix, accuracy_score
import numpy as np
from scipy import stats

f = open('satellite.csv')
d = np.loadtxt(f,delimiter=',')
data = d[:,:-1]
target = d[:,-1]

train_x,test_x,train_y,test_y = train_test_split(data,target,test_size=0.3)

dtc = tree.DecisionTreeClassifier()
dtc.fit(train_x,train_y)
predict_train_y = dtc.predict(train_x)
predict_test_y = dtc.predict(test_x)
print 'Decision tree train accuracy : %.2f'%(accuracy_score(train_y,predict_train_y))
print 'Decision tree test accuracy : %.2f'%(accuracy_score(test_y,predict_test_y))
print 'confusion matrix for decision tree : '
print confusion_matrix(test_y,predict_test_y)
with open('tree.dot','w') as dotfile:
     tree.export_graphviz(dtc,dotfile)

#converting the dot file into png format
from subprocess import check_call
check_call(['dot','-Tpng','tree.dot','-o','DT.png'])

rfc = ensemble.RandomForestClassifier()
rfc_scores = cross_val_score(rfc,data,target,cv=10)
print 'Random forest mean accuracy : %.2f'%(rfc_scores.mean())
print 'Random forest std : %.2f'%(rfc_scores.std())

nbc = naive_bayes.GaussianNB()
nbc_scores = cross_val_score(nbc,data,target,cv=10)
print 'Naive bayes mean accuracy : %.2f'%(nbc_scores.mean())
print 'Naive bayes std : %.2f'%(nbc_scores.std())

print stats.ttest_ind(rfc_scores,nbc_scores)

