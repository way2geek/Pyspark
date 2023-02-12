# coding: utf8
import  jieba
if __name__ == '__main__':
    content  = '肥妮子有个大头'
    content2 = '猪妮子是个大肥猪头娃子'
    result = jieba.cut(content,True)
    result2 = jieba.cut(content2,True)
    result3 = jieba.cut_for_search(content2)
    print(list(result))
    print(list(result2))
    print(list(result3))