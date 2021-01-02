
import csv
import sys
inFile = sys.argv[1]
outFile = sys.argv[2]
def sort(var)
:
sort_ID = []
for ID in var.keys()
:
sort_ID.insert(0,ID)
for i in range(len(sort_ID))
:
for j in range(len(sort_ID) - 1)
:
if sort_ID[j] > sort_ID[j + 1]
:
sort_ID[j], sort_ID[j + 1] = sort_ID[j + 1], sort_ID[j]
for i in sort_ID:
yield i
sales = list(map(dict,csv.DictReader(open(inFile, 'r'
))))
size = len(sales)
proID_count = {}
proID_sum = {}
for t in sales:
cus, pro, item = t[
'Customer ID'
],t[
'Product ID'
],t[
'Item Cost'
]
if pro not in proID_sum.keys()
:
proID_sum[pro] = round(float(item),2)
else:
proID_sum[pro] = round(proID_sum[pro] + float(item),2)
if pro not in proID_count.keys()
:
proID_count[pro] = [cus,1]
elif proID_count[pro][0] != cus:
proID_count[pro][1] += 1
proID_count[pro][0] = cus
sort_ID = sort(proID_count)
with open(outFile, mode='w'
,newline='
'
) as csv_file:
fieldnames = [
'Product ID'
, 'Costumer Count'
, 'Total Revenue'
]
writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
writer.writeheader()
for ID in sort_ID:
writer.writerow({'Product ID'
: ID, 'Costumer Count'
: proID_count[ID][1],
'Total Revenue'
: proID_sum[ID]})
with open(outFile) as csv_file:
csv_reader = csv.reader(csv_file, delimiter='
,
'
)
for row in csv_reader:
print(f'\t{row[0]}
{row[1]}
{row[2]}.
'
)
