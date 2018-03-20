from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    person = line.split(',')
    age = int(person[2])
    friends = int(person[3])
    return (age,friends)
    
lines = sc.textFile("/user/[USER_DIR]/fakefriends.csv")
rdd = lines.map(parseLine)
totalsByAge = rdd.mapValues(lambda x:(x,1)).reduceByKey(lambda x,y: (x[0] + y[0],x[1] + y[1]))
averageByAge =  totalsByAge.mapValues(lambda x: int(x[0] / x[1]))
results = averageByAge.collect()
for result in results:
    print (result)
