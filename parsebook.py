import nltk
from nltk.tokenize import sent_tokenize, word_tokenize
import os
import sqlite3

# create connection to ngrams.db
conn = sqlite3.connect('ngrams.db')
c = conn.cursor()

sql = '''
create table documents (id integer primary key, title text);

create table words (id integer primary key, word text);

create table sentences (id integer primary key, document_id integer, sentence_number integer);
create index idx_sentences_document_id on sentences(document_id);

create table word_positions (id integer primary key, sentence_id integer, word_id integer, position integer);
create index idx_word_positions_sentence_id on word_positions(sentence_id);
create index idx_word_positions_word_id on word_positions(word_id);
create index idx_word_positions_position on word_positions(position);
'''

for s in sql.split(';'):
    c.execute(s)
conn.commit()


# word to int
wordkey = 1
words = {}

# dictionary from filename to sentences
docs = {}
dockey = 1

# sentences
sentkey = 1
sentences = {}

# word positions
wordpos = []

folder = 'data'

# list all .txt files in data folder
files = os.listdir(folder)
for file in files:
    if file.endswith('.txt'):
        doc = file[:-4]
        docs[doc] = dockey
        dockey += 1
        c.execute('insert into documents values (?, ?)', (docs[doc], doc))
        with open(f'{folder}/{file}') as f:
            text = f.read()
            sentences = sent_tokenize(text)
            sentnum = 1
            for sentence in sentences:
                #sentences[sentkey] = (sentkey, docs[doc], sentnum)
                c.execute('insert into sentences values (?, ?, ?)', (sentkey, docs[doc], sentnum))                
                sentnum += 1
                
                tokens = map(lambda x : x.lower(), word_tokenize(sentence))
                pos = 1
                for word in tokens:
                    if word not in words:
                        words[word] = wordkey
                        wordkey += 1
                        c.execute('insert into words values (?, ?)', (words[word], word))
                    c.execute('insert into word_positions (sentence_id, word_id, position) values (?, ?, ?)', (sentkey, words[word], pos))
                    pos += 1
                # only increment sentkey at the end of the sentence
                sentkey += 1

conn.commit()
