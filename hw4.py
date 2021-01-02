from pyspark import SparkContext
from pyspark.sql.session import SparkSession
import csv
import sys
inFile = sys.argv[1]
sc = SparkContext()
def help1(file):
    import csv
    with open(file, 'r') as fi:
        reader = csv.DictReader(fi)
        for row in reader:
             yield([row['Product'],row['Date received'],row['Company']])
rdd = sc.parallelize(help1(inFile))

rdd2 = rdd.map(lambda x: (x[0] + " " + x[1]+ " " + x[2], 1))
total_companies_year = rdd.map(lambda x: (x[0] + " " + x[1][:4],set([x[2]])))\
                        .reduceByKey(lambda x,y: x | y)\
                        .map(lambda x: (x[0], len(x[1])))
                        
total_complaint_year = rdd.map(lambda x: (x[0] + " " + x[1][:4], 1))\
                          .groupByKey()\
                          .mapValues(lambda x: len(x))


max_com = rdd.map(lambda x: (x[0] + "+++" + x[1][:4]+ "---" + x[2], 1))\
                        .groupByKey()\
                        .map(lambda x: (x[0], len(x[1])))\
                        
max_com1 = max_com.map(lambda x: (x[0].split("---")[0],x[1]))\
            .groupByKey()\
            .map(lambda x: (x[0], max(x[1])))

max1 = sorted(max_com1.collect())
total_companies = sorted(total_companies_year.collect())
total_complaints = sorted(total_complaint_year.collect())
final = []
for i in range(len(max1)):
    final.append([max1[i][0].split("+++")[0],max1[i][0].split("+++")[1],total_complaints[i][1],total_companies[i][1]
    ,int(float(max1[i][1]) / float(total_complaints[i][1]) * 100)])
with open('output.csv', mode='w',newline='') as csv_file:
    fieldnames = ['Product', 'Date', 'Total complaints','Total companies','Highest rate']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()
    for item in final:
        writer.writerow({'Product':item[0],'Date':item[1],'Total complaints':item[2],'Total companies':item[3],'Highest rate':item[4]})   
