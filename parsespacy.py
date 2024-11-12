import spacy
import re
from spacy.tokenizer import Tokenizer
from spacy.util import compile_infix_regex
import os
import sqlite3

database = 'easy.db'
folder = 'data3'

stopwords = ["i","me","my","myself","we","our","ours","ourselves","you","your",
            "yours","yourself","yourselves","he","him","his","himself","she","her",
            "hers","herself","it","its","itself","they","them","their","theirs",
            "themselves","what","which","who","whom","this","that","these","those",
            "am","is","are","was","were","be","been","being","have","has","had","having",
            "do","does","did","doing","a","an","the","and","but","if","or","because",
            "as","until","while","of","at","by","for","with","about","against","between",
            "into","through","during","before","after","above","below","to","from","up",
            "down","in","out","on","off","over","under","again","further","then","once",
            "here","there","when","where","why","how","all","any","both","each","few",
            "more","most","other","some","such","no","nor","not","only","own","same",
            "so","than","too","very","s","t","can","will","just","don","should","now",
            "don't","it's"]

# Create a connection to the ngrams.db SQLite database
conn = sqlite3.connect(database)
c = conn.cursor()

# Define SQL for creating tables and indices
sql = '''
CREATE TABLE documents (id INTEGER PRIMARY KEY, title TEXT);

CREATE TABLE words (id INTEGER PRIMARY KEY, word TEXT, stopword BOOLEAN DEFAULT 0);

CREATE TABLE sentences (id INTEGER PRIMARY KEY, document_id INTEGER, sentence_number INTEGER);
CREATE INDEX idx_sentences_document_id ON sentences(document_id);

CREATE TABLE word_positions (id INTEGER PRIMARY KEY, sentence_id INTEGER, word_id INTEGER, position INTEGER);
CREATE INDEX idx_word_positions_sentence_id ON word_positions(sentence_id);
CREATE INDEX idx_word_positions_word_id ON word_positions(word_id);
CREATE INDEX idx_word_positions_sentence_word ON word_positions(word_id, sentence_id);
CREATE INDEX idx_word_positions_position ON word_positions(position);
'''

# Execute the SQL statements to set up the database
for s in sql.split(';'):
    if s.strip():
        c.execute(s)
conn.commit()


# Load the spaCy model for English
nlp = spacy.load("en_core_web_sm")
nlp.max_length = 2000000

infixes = nlp.Defaults.infixes + [
    r"(?<=\w)([:;,\.\!\?\"])(?=\s|$|[\"'])",      # Separate trailing punctuation (including quotes) at the end of words
    r"(?<=\s)([\"'])",                     # Separate leading quotes at the start of words
    r"(?<=^)([\"'])",                     # Separate leading quotes at the start of words
    r"(?<=\w)(--)(?=\s|$)",                # Separate double dashes between words
    r"(?<=\w)(—)(?=\s|$)",                 # Separate em dashes at the end of words
    r"(?<=\s)([_])(?=\w)",               # Separate underscores at the start of words
    r"(?<=\w)([_])(?=\s|$)",               # Separate underscores at the end of words
    r"(?<=\")(\w+)(?=\")",                 # Separate a single word enclosed in double quotes
    r"(?<=\w)(,--)(?=\s|$)",
    r"(^|(?<=\s))([\"'])",
]


# Compile the new infix pattern
infix_re = compile_infix_regex(infixes)

# Define a custom tokenizer with the modified infix pattern
nlp.tokenizer = Tokenizer(nlp.vocab, infix_finditer=infix_re.finditer)

# Initialize variables
wordkey = 1
words = {}  # Map words to unique IDs
docs = {}   # Map document names to unique IDs
dockey = 1
sentkey = 1

# List all .txt files in the data folder
files = os.listdir(folder)
for file in files:
    if file.endswith('.txt'):
        doc_name = file[:-4]
        docs[doc_name] = dockey
        dockey += 1
        c.execute('INSERT INTO documents VALUES (?, ?)', (docs[doc_name], doc_name))

        with open(f'{folder}/{file}') as f:
            text = f.read()
            
            
            # standardize the single and double quotes
            text = text.replace('“', '"').replace('”', '"').replace('‟', '"')
            text = text.replace('‘', "'").replace('’', "'").replace('‚', "'").replace('‛', "'")
            text = text.replace('"', ' " ')
            # standardize other punctuation
            text = text.replace('–', '--')
            text = text.replace('—', '--')
            text = text.replace('…', '...')
            text = text.replace('_', '')
            text = re.sub(r'-{3,}', '--', text)
            text = text.replace('.,', '. ,')
            text = text.replace(",'", ", '")
            text = text.replace("',", "' ,")
            text = text.replace(".'", ". '")
            text = text.replace("'.", "' .")
            text = text.replace('--', ' -- ')
            text = text.replace('(', ' ( ')
            text = text.replace(')', ' ) ')
            text = text.replace('!', ' ! ')
            text = text.replace('?', ' ? ')
            text = re.sub(r'\.{4,}', '...', text)


            # normalize whitespace
            text = ' '.join(text.split())
            
            # Use spaCy to process the text
            doc = nlp(text)

            sentnum = 1
            for sentence in doc.sents:
                # Insert each sentence with document ID and sentence number
                c.execute('INSERT INTO sentences VALUES (?, ?, ?)', (sentkey, docs[doc_name], sentnum))
                sentnum += 1

                pos = 1
                for token in sentence:
                    word = token.text.strip().lower()
                    if not word:
                        continue
                    if word not in words:
                        words[word] = wordkey
                        wordkey += 1
                        stopword = word in stopwords
                        c.execute('INSERT INTO words VALUES (?, ?, ?)', (words[word], word, stopword))

                    # Insert word positions for each token in the sentence
                    c.execute('INSERT INTO word_positions (sentence_id, word_id, position) VALUES (?, ?, ?)', (sentkey, words[word], pos))
                    pos += 1
                
                # Increment sentkey after processing the sentence
                sentkey += 1

# Commit all changes to the database
conn.commit()

print('\n'.join(words.keys()))