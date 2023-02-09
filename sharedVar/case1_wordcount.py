from pyspark import SparkContext, SparkConf,StorageLevel
import re
if __name__ == '__main__':
    conf = SparkConf().setAppName('case2').setMaster('local[*]')
    sc = SparkContext(conf=conf)
    abnormal_char= [',','.','!','#','$','%']
    bd  = sc.broadcast(abnormal_char)
    rdd = sc.textFile('../data/accumulator_broadcast_data.txt',2)
    normal_count = sc.accumulator(0)
    abnormal_count = sc.accumulator(0)
    #rdd1 = rdd.filter(lambda x:x.strip())  #去除空白行
    #rdd2 = rdd1.map(lambda x:x.strip())    #去除每行首尾空格
   # rdd3 = rdd2.map(lambda x:re.split("\s+",x))  #正则表达式，多个空格切分
    rdd3 = rdd.map(lambda x:x.split())
    print(rdd3.collect())
    def counts(data):
        global normal_count,abnormal_count
        if len(data)==0:
            return
        if data in bd.value:
            abnormal_count += 1
        else:
            normal_count += 1

    rdd4 = rdd3.flatMap(lambda x:x)
    #解嵌套：先遍历每个子集合，对每个集合元素执行函数，若子集合为空就会报空类型错误
    #lambda x:x 则不会报错？
    #空集合解嵌套后会消失
    rdd4.map(counts).collect()
    print(abnormal_count,normal_count)

    ''''''