import pandas as pd
import jieba
import jieba.analyse
import hashlib
import time
import multiprocessing

costum_content = pd.read_csv('/dev/shm/helper.csv')
allowPOS = ('an','e','i','j','l','Ng','n','vg','v','vd','vn','y','z')
now = time.strftime("%m%d%H%M_", time.localtime())
tail = '_helper'

def get_high_frequence_sentence():
    time1 = time.time()
    hash_content_dict = {}
    hash_count_dict = {}
    hash_values = []
    content_count_dict = {'content': [], 'count': []}
    for i in range(costum_content.ix[:,0].size):
        content = costum_content.ix[i]['content']
        hash_value = hashlib.md5((content).encode('utf8')).hexdigest()
        #hash_value = hash((content).encode('utf8'))
        if hash_value not in hash_values:
            if hash_value not in hash_content_dict.keys():
                hash_content_dict[hash_value] = content
                hash_count_dict[hash_value] = 1
            else:
                hash_count_dict[hash_value] += 1
                '''
                if hash_count_dict[hash_value] > 5:
                    hash_values.append(hash_value)
                    content_count_dict['content'].append(hash_content_dict[hash_value])
                    content_count_dict['count'].append(hash_count_dict[hash_value])
                    hash_content_dict.pop(hash_value)
                '''
    for key in hash_count_dict.keys():
        if hash_count_dict[key] > 10:
            content_count_dict['content'].append(hash_content_dict[key])
            content_count_dict['count'].append(hash_count_dict[key])

    #df = pd.DataFrame(content_count_dict)
    #df.to_csv('high_frequence_sentence.csv',index=False)
    #print(df.ix[:,0].size)
    print(time.time()-time1)

def get_high_frequence_sentence2():
    time1 = time.time()
    content_count_dict = {}
    filtered_content_count_dict = {'content': [], 'count': []}
    for i in range(costum_content.ix[:,0].size):
        content = costum_content.ix[i]['content']
        if content not in content_count_dict.keys():
            content_count_dict[content] = 1
        else:
            content_count_dict[content] += 1


    for key in content_count_dict.keys():
        if content_count_dict[key] > 10:
            filtered_content_count_dict['content'].append(key)
            filtered_content_count_dict['count'].append(content_count_dict[key])

    df = pd.DataFrame(filtered_content_count_dict)
    df.to_csv(now+'high_frequence_sentence'+tail+'.csv',index=False)
    print(df.ix[:,0].size)
    print(time.time()-time1)

def get_tfidf_theme(content,return_dict):
    time1 = time.time()
    text = '.'.join(content)
    tags = jieba.analyse.extract_tags(text, topK=100, allowPOS=allowPOS)
    print(time.time() - time1)
    return_dict['result'] = list(tags)

def get_textRank_theme(content,return_dict):
    time1 = time.time()
    text = '.'.join(content)
    #pprint.pprint(text)
    tags = jieba.analyse.textrank(text, topK=100, allowPOS=allowPOS)
    print(time.time() - time1)

    return_dict['result'] = list(tags)

def get_keyword(df):
    #content = [(df.ix[i, 'content'] + '.') * df.ix[i, 'count'] for i in range(df.ix[:, 0].size)]
    content = list(df['content'])
    manager = multiprocessing.Manager()
    return_dict1 = manager.dict()
    return_dict2 = manager.dict()
    p1 = multiprocessing.Process(target=get_textRank_theme,args=(content,return_dict1))
    p2 = multiprocessing.Process(target=get_tfidf_theme,args=(content,return_dict2))
    p1.start()
    p2.start()
    p1.join()
    p2.join()
    key_words1 = return_dict1['result']
    key_words2 = return_dict2['result']
    with open(now+'keywords'+tail+'.txt','w') as f:
        for word in key_words1:
            if word in key_words2:
                f.writelines(word)
                f.writelines('\n')

def get_main_sentence(df,keywords):
    '''
    content = []
    for i in range(df.ix[:, 0].size):
        for j in range(df.ix[i, 'count']):
            content.append(df.ix[i, 'content'])
    '''
    content = list(df['content'])
    tags_list = keywords
    count = 0
    main_sentence = {'sentence':[]}
    for sentence in content:
        for tag in tags_list:
            if tag in sentence:
                main_sentence['sentence'].append(sentence)
                count += 1
                break
    print(count)
    main_sentence_df = pd.DataFrame(main_sentence)
    main_sentence_df.to_csv(now+'main_sentence'+tail+'.csv')

if __name__ == '__main__':
    get_high_frequence_sentence2()
    df = pd.read_csv(now+'high_frequence_sentence'+tail+'.csv')
    get_keyword(df)
    with open(now+'keywords'+tail+'.txt','r') as f:
        keywords = []
        for line in f.readlines():
            keywords.append(line.strip('\n'))
    get_main_sentence(df,keywords)