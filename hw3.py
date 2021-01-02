import csv
import mapreduce as mr
from mrjob.job import MRJob
import sys

inFile = sys.argv[1]
def mapper(_,row):
    yield ([row['Product'],row['Date received'][:4],row['Company']], 1)
def reducer(word,count):
    yield(word,sum(count))
    
def mapper1(line,count):
    yield([line[0],line[1]],count)
def reducer1(pro_date,total):
    yield([pro_date[0],pro_date[1]],[max(total),len(total),sum(total)])

def mapper2(pro_date,list1):
    yield([pro_date[0],pro_date[1],list1[2],list1[1],int(float(list1[0]) / float(list1[2])* 100)])

with open(inFile, "r") as f:
    lines = enumerate(csv.DictReader(f))
    a = list(mr.run(lines,mapper,reducer))
    b = list(mr.run(a,mapper1,reducer1))
    c = list(mr.run(b,mapper2))

for i in c:
    for j in range(len(i)):
        i[j] = str(i[j])
for i in c:
    i[0] = "\"" + i[0] + "\""
for i in c:
    print(', '.join(i))
