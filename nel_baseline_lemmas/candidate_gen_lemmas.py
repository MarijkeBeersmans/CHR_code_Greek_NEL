import pandas as pd
from argparse import ArgumentParser
import os
import sys
import getpass
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s',
    handlers=[logging.StreamHandler()]
)

parser = ArgumentParser()
parser.add_argument('mention_file')
parser.add_argument('--glaux_id_col', default='glaux_id')
parser.add_argument('--re_id_col', default='RE_id')
parser.add_argument('--topk', default=64)


args = parser.parse_args()


sys.path.insert(1, "../../glaux_database_connector")
import DatabaseConnector

hostname = 'forbidden knowledge'
username = 'forbidden knowledge'
password = getpass.getpass()

# from the GLAUx id of the mention, return the lemma id
logging.info("Loading mention file: %s", args.mention_file)

if args.mention_file.endswith('jsonl'):
    df = pd.read_json(args.mention_file, lines=True)
elif args.mention_file.endswith('csv'):
    df = pd.read_csv(args.mention_file)
else:
    print('file not jsonl or csv, not implemented')


logging.info("Loaded %d mentions.", len(df))

id_str = ','.join(map(str, df[args.glaux_id_col].values.tolist()))

query = f'SELECT glaux_id, lemma_id FROM wordorder JOIN written_word_strings ON written_word_id = ID WHERE glaux_id in ({id_str})'


connector = DatabaseConnector.DatabaseConnector(hostname, username, password)
conn = connector.initiate_connection()

with conn.cursor() as cursor:
    cursor.execute(query)
    rows = cursor.fetchall()


glaux_id2lemma_id = {str(row['glaux_id']):row['lemma_id'] for row in rows}

# convert lemma id to nam id 

logging.info("Loading lemma mapping file.")
df_nam_id_to_glaux_lemma = pd.read_csv('../data/silver_data/mappings/glaux_lemma_TM_Nam_nieuw.csv', header=None)
logging.info("Loaded %d lemma mappings.", len(df_nam_id_to_glaux_lemma))

df_nam_id_to_glaux_lemma.rename(columns = {0: 'glaux_id',
                     1: 'text_1',
                    2: 'text_2',
                    3: 'text_3',
                    4: 'tm_id',
                    5: 'tm_id_2',
                    6: 'tm_id_3'}, inplace=True)

df_nam_id_to_glaux_lemma.drop(columns=['text_1', 'text_2', 'text_3'], inplace=True)
records = df_nam_id_to_glaux_lemma.to_dict(orient='records')

glaux_lemma_to_tm = {}

for record in records:
    if record['glaux_id'] not in glaux_lemma_to_tm:
        record_nam_ids = []
        for item in [record['tm_id'], record['tm_id_2'], record['tm_id_3']]:
            if pd.notnull(item) and item != 0:
                record_nam_ids.append(item)
        glaux_lemma_to_tm[record['glaux_id']] = record_nam_ids
    else:
        logging.info(f'glaux_id {record["glaux_id"]} already in dictionary.')
#

# check which REAL entries have a corresponding nam id
import numpy as np

logging.info("Loading re_to_nam_id mapping file.")
df2 = pd.read_csv('../data/silver_data/mappings/re_id_to_nam_id.tab', sep='\t', header=None, dtype={0: 'Int64'})
logging.info("Loaded %d re_to_nam_id mappings.", len(df2))

df2.dropna(subset=0, inplace=True)
df2 = df2[df2!= 'missing']

#make a dictionary from the nam_ids to the corresponding re_ids
nam_id_2_re_ids = {}

def add_nam_id_to_dict(nam_id, row_val, nam_id_2_re_ids):
    """Helper function to add nam_id to the dictionary."""
    if nam_id in nam_id_2_re_ids:
        nam_id_2_re_ids[nam_id].append(row_val)
    else:
        nam_id_2_re_ids[nam_id] = [row_val]

# Iterate over each row in the dataframe df2
for index, row in df2.iterrows():
    # Check if the second column (row[1]) is a string
    if isinstance(row[1], str):
        # Handle multiple IDs separated by colon
        if ':' in row[1]:
            nam_ids = row[1].split(':')[1].split(' / ')
            for nam_id in nam_ids:
                add_nam_id_to_dict(int(nam_id.strip()), row[0], nam_id_2_re_ids)
        # Handle single ID
        else:
            add_nam_id_to_dict(int(row[1]), row[0], nam_id_2_re_ids)
    
    # If the value is a valid number, handle it
    elif not np.isnan(row[1]):
        add_nam_id_to_dict(int(row[1]), row[0], nam_id_2_re_ids)


def reverse_dict(original_dict):
    reversed_dict = {}

    for key, values in original_dict.items():
        for value in values:
            if value not in reversed_dict:
                reversed_dict[value] = []
            reversed_dict[value].append(key)

    return reversed_dict

re_id_2_nam_ids = reverse_dict(nam_id_2_re_ids)

logging.info("Built glaux_id2lemma_id dictionary with %d entries.", len(glaux_id2lemma_id))
logging.info("Built glaux_lemma_to_tm dictionary with %d entries.", len(glaux_lemma_to_tm))
logging.info("Built nam_id_2_re_ids dictionary with %d entries.", len(nam_id_2_re_ids))
logging.info("Built re_id_2_nam_ids dictionary with %d entries.", len(re_id_2_nam_ids))


# return all candidates


def get_possible_re_ids(glaux_id):
    glaux_ids = glaux_id.split(',')
    all_nam_id_sets = []
    lemma_id = None

    for glaux_id in glaux_ids:
        try:
            lemma_id = glaux_id2lemma_id[glaux_id]
        except KeyError:
            logging.warning(f'glaux_id {glaux_id} not found in glaux_id2lemma_id.')
            continue
        if lemma_id:
            try:
                nam_ids = glaux_lemma_to_tm[lemma_id]
                all_nam_id_sets.extend(nam_ids)
            except KeyError:
                logging.warning(f'lemma_id {lemma_id} not found in glaux_lemma_to_tm.')
                continue

    if not all_nam_id_sets:
       return set()

    # Find common nam_ids across all glaux_ids
    #


    possible_re_ids = set()
    for nam_id in all_nam_id_sets:
        print(nam_id)
        try:
            possible_re_ids.update(nam_id_2_re_ids[nam_id])
        except KeyError:
            logging.warning(f'nam_id {nam_id} not found in nam_id_2_re_ids.')
        #filter on those that contain more than one


    return possible_re_ids


# def get_possible_re_ids(glaux_id):
#     lemma_id = glaux_id2lemma_id[glaux_id]
#     try:
#         nam_ids = glaux_lemma_to_tm[lemma_id]
#     except KeyError:
#         nam_ids = 'no_id'
#         logging.warning(f'lemma {lemma_id} could not be found.')
#         return set()
#     try:
#         possible_re_ids = []
#         for nam_id in nam_ids:
#             possible_re_ids += nam_id_2_re_ids[nam_id]
#         return set(possible_re_ids)
#     except KeyError:
#         logging.warning(f'Nam {nam_id} could not be found.')
#         return set()
    # return a set to make DAMN SURE there are no duplicates in the possible re_id list
# def get_possible_re_ids(glaux_id):
#     lemma_ids = [glaux_id2lemma_id[gid] for gid in glaux_id]
#     try:
        
#     nam_ids = [
#         nam_id
#         for lemma_id in lemma_ids
#         if lemma_id in glaux_lemma_to_tm
#         for nam_id in glaux_lemma_to_tm[lemma_id]
#     ]

#     except KeyError:
#         nam_ids = 'no_id'
#         logging.warning(f'lemma {lemma_id} could not be found.')
#         return set()
#     try:
#         possible_re_ids = []
#         for nam_id in nam_ids:
#             possible_re_ids += nam_id_2_re_ids[nam_id]
#         return set(possible_re_ids)
#     except KeyError:
#         logging.warning(f'Nam {nam_id} could not be found.')
#         return set()
import random
random.seed(42)

def rank_random(re_ids, topk=64):
    """
    Randomly shuffle the list of re_ids and return the top k.
    """
    lst_ids = list(re_ids)
    random.shuffle(lst_ids)
    return lst_ids[:topk]
    

df['matching_lemma'] = df.glaux_id.map(glaux_id2lemma_id)

df['matching_nam_id'] = df.matching_lemma.map(glaux_lemma_to_tm)

df['possible_re_ids'] = df.glaux_id.map(get_possible_re_ids)

#doublecheck whether 
df['dubbel'] = df.apply(lambda x:  x[args.re_id_col] in x.possible_re_ids, axis=1)
logging.info(df.dubbel.value_counts())

df['candgen_new'] = df['possible_re_ids'].map(lambda x: rank_random(x, topk=args.topk))

df['in_topk'] = df.apply(lambda x: x[args.re_id_col] in x.candgen_new, axis=1)

df.to_csv('test.csv')

logging.info(f"recall@{args.topk} {df['in_topk'].value_counts(normalize=True).mul(100).astype(str)+'%'}")



