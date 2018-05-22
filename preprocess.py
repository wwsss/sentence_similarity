#-*-coding:utf-8-*-
import pandas as pd
import csv

with open(r"坐席与客户.csv", 'r',encoding='utf-8') as f:
    reader = csv.reader(f)
    linenumber = 1
    pre_linenumber = 1
    num = 0
    dialogue_content = {'id':[],'content':[]}
    while True:
        try:
            for row in reader:
                if row[10][0] < '0' or row[10][0] > 'z' and row[-2] == '文本':
                    dialogue_content['id'].append(row[8])
                    dialogue_content['content'].append(row[10])
                    num += 1
                linenumber += 1
            break
        except:
            print(linenumber)
            linenumber += 1

print(linenumber)
print(num)

dialogue_content_df = pd.DataFrame(dialogue_content)
dialogue_content_df.to_csv('/dev/shm/dialogue.csv',index=False)
costum_content = dialogue_content_df[dialogue_content_df['id']=='客户']
costum_content.to_csv('/dev/shm/costum.csv',index=False)
helper_content = dialogue_content_df[dialogue_content_df['id']=='坐席']
helper_content.to_csv('/dev/shm/helper.csv',index=False)

