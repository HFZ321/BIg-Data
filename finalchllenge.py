from pyspark import SparkContext
from pyspark.sql.session import SparkSession
import re
import sys
sc = SparkContext()
def trun_match1(county):
    if county in {"MAN","MH","MN","NEWY","NEW Y","NY"}:
        return '1'
    elif county in {"BRONX","BX"}:
        return '2'
    elif county in {"BK","K","KING","KINGS"}:
        return '3'
    elif county in {"Q","QN","QNS","QU","QUEEN"}:
        return '4'
    elif county in {"R","RICHMOND"}:
        return '5'
    else:
        return 0
def check_num(house_num):
    if house_num == '':
        return False
    if '-' in house_num:
        try:
            n1,n2 = house_num.split('-')
            float(n1)
            float(n2)
        except ValueError:
            return False
        return True
    try:
        float(house_num)
    except ValueError:
        return False
    return True
def map1(CV):
    if '/' in CV:
        return CV[len(CV[0]) - 5:]
    return ''
def map2(CV):
    if CV == '':
        return False
    try:
        num = int(CV)
    except ValueError:
        return False
    if num > 2019 or num < 2015:
        return False
    return True
def map_f(line):
    SCL = line[0]
    List = list(line)
    ID = SCL[0]
    try:
        s, oL = SCL[1].split('-')
        s, oR = SCL[2].split('-')
        s, eL = SCL[3].split('-')
        s, eR = SCL[4].split('-')
        oL,oR,eL,eR = int(oL),int(oR),int(eL),int(eR)
        cv,num = List[1][1].split('-')
        num = int(num)
    except ValueError:
        return ''
    if cv != s:
        return ''
    if num % 2 == 0:
        if num >= eL and num <= eR:
            return(ID,List[1][0])
    if num % 2 != 0:
        if num >= oL and num <= oR:
            return(ID,List[1][0])
         return ''
def mapf(line):
    SCL = line[0]
    List = line
    ID,oL,oR,eL,eR = SCL[0],SCL[1],SCL[2],SCL[3],SCL[4]
    if ('-' in oL )or( '-' in oR )or( '-' in eL )or( '-' in eR):
        return map_f(line)
    num = List[1][1]
    if '-' in num:
        return ''
    try:
        num,oL,oR,eL,eR = int(num),int(oL),int(oR),int(eL),int(eR)
    except ValueError:
        return ''
    if num % 2 == 0:
        if num >= eL and num <= eR:
            return(ID,List[1][0])
    return ''
def mapf2(line):
    n15,n16,n17,n18,n19 = 0,0,0,0,0
    List = line
    my_dict = {i:List.count(i) for i in List}
    n15 = my_dict.get('2015')
    n16 = my_dict.get('2016')
    n17 = my_dict.get('2017')
    n18 = my_dict.get('2018')
    n19 = my_dict.get('2019')
    if n15 == None: n15 = 0
    if n16 == None: n16 = 0
    if n17 == None: n17 = 0
    if n18 == None: n18 = 0
    if n19 == None: n19 = 0
    return [List[0],n15,n16,n17,n18,n19]

rdd = sc.textFile("/data/share/bdm/nyc_parking_violation")
rdd1 = sc.textFile("/data/share/bdm/nyc_cscl.csv")

rdd_PerLine = rdd.map(lambda line: line.split(","))
VCheader = rdd_PerLine.take(1)
rddCV = rdd_PerLine.filter(lambda line: line != VCheader[0])\
.map(lambda line: map1(line[4]) + "~" + line[23] + "~" + line[21] + "~" + line[24])\
.map(lambda line: line.split("~")).filter(lambda line: map2(line[0]) and check_num(line[1]))\
.map(lambda line: [line[0],line[1],trun_match1(line[2]),line[3]])\
.map(lambda line: [(line[2],line[3]),(line[0],line[1])])

p = ''',(?=(?:[^'"]|'[^']*'|"[^"]*")*$)'''
rddSC = rdd1.map(lambda line: re.split(p, line)).filter(lambda line: len(line) >= 31)\
.map(lambda line: line[0] + "~" + line[2] + "~" + line[3] + "~" + line[4] + "~"  + line[5] + "~"
 + line[13]  + "~"+ line[28] + "~" + line[29])\
.map(lambda line: line.split("~")).filter(lambda line: line[0] != '')

rddSC1 = rddSC.map(lambda line: [(line[5],line[6]),(line[0],line[1],line[2],line[3],line[4])])
rddSC2 = rddSC.map(lambda line: [(line[5],line[7]),(line[0],line[1],line[2],line[3],line[4])])
rddSC3 = rddSC1.join(rddCV)
rddSC4 = rddSC2.join(rddCV)
rddnew = rddSC3.union(rddSC4)


rddnew = rddnew.map(lambda line: line[1]).map(lambda line: mapf(line))\
.filter(lambda line: line != '').reduceByKey(lambda x,y: x + ',' + y)\
.map(lambda x: (x[0],x[1].split(','))).map(lambda x: [x[0]] + x[1]).map(lambda line: mapf2(line))
#use to check the result
#number = rddnew.flatMap(lambda line: line).filter(lambda line: isinstance(line,int)).map(lambda x:('total',\
#x)).groupByKey().mapValues(lambda x: sum(x))
rddnew.saveAsTextFile(sys.argv[1])









