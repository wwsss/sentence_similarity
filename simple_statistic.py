import pandas as pd
import jieba
import jieba.analyse
import time

costum_content = pd.read_csv('/dev/shm/costum.csv')
def get_tfidf_theme():
    time1 = time.time()
    text = '.'.join(list(costum_content['content']))
    tags = jieba.analyse.extract_tags(text, topK=50, allowPOS=('n', 'vn', 'v'))
    print(time.time() - time1)
    print(list(tags))

    return tags

def get_textRank_theme():
    time1 = time.time()
    text = '.'.join(list(costum_content['content']))
    # pprint.pprint(text)
    tags = jieba.analyse.textrank(text, topK=50, allowPOS=('n', 'vn', 'v'))
    print(time.time() - time1)
    print(list(tags))

    return tags

def get_main_sentence():
    tags_list = ['保单', '代发', '客服', '菜单', '专属', '车险', '保险', '电话', '优惠',
                 '支付', '微信', '油卡', '交强险', '报价', '车主', '联系', '地址', '保养',
                 '付款', '保费', '信用卡', '投保', '发票', '短信', '信息', '商业']
    count = 0
    main_sentence = {'sentence':[]}
    for sentence in list(costum_content['content']):
        for tag in tags_list:
            if tag in sentence:
                main_sentence['sentence'].append(sentence)
                count += 1
                break
    print(count)
    main_sentence_df = pd.DataFrame(main_sentence)
    main_sentence_df.to_csv('costum_main_sentence.csv')

if __name__ == '__main__':
    get_main_sentence()

