#utf-8
import  jieba
def process(data):
    rs = jieba.cut_for_search(data)
    return list(rs)


def userprocess(data):
    dt = []
    ls = process(data[1])
    for i in range(0,len(ls)):
        print(i)
        tmp = str(data[0])+'_'+ls[i]
        dt.append(tmp)
    return dt

def timeprocess(date):
    #23:00:00
    hour  = str(date[:2])
    if hour == '00':
        return ('23-00',1)
    time1 = ''
    time2 = ''
    if int(hour)+1 ==24:
        time1 = '00'
    else:
        time1 = str(int(hour)+1)
    time2 = str(int(hour)-1)
    if len(time1)==1:
        time1 = '0'+time1
    if len(time2)==1:
        time2 = '0'+time2
    time1 = hour+'_'+time1
    time2 = time2+'_'+hour
    return [time1,time2]


