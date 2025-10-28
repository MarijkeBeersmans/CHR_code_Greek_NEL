# %%
import pandas as pd
from argparse import ArgumentParser

parser = ArgumentParser()

parser.add_argument('mention_file')
parser.add_argument('output_file')
parser.add_argument('version')
parser.add_argument('--context_len', default=50)
parser.add_argument('--output_dir', default='C:/Users/u0161477/Documents/NIKAW-Prep/NIKAW-Prep/Gitlab/CHR_NEL_code/data/blink_data')
parser.add_argument('--real_df', default='../../../Paulys_kb/finish_real_version/20250522_real_entity_dict_w_wikilink_v5.csv')
parser.add_argument('--debug', default=False)


args = parser.parse_args()

df_to_add_mention = pd.read_csv(args.mention_file)

from tqdm import tqdm
import sys
sys.path.insert(1, "../../../../../glaux_database_connector")

sys.path.insert(1, "../../../../glaux_database_connector")
import os

output_dir = os.path.join(os.path.abspath(args.output_dir), args.version, 'synthetic')

if os.path.exists(output_dir) == False:
    os.makedirs(output_dir, exist_ok=True)



# import GlauxData
import DatabaseConnector
import pandas as pd
import time
from tqdm import tqdm
import getpass

hostname = 'forbidden knowledge' 
username = 'Marijke'
password = getpass.getpass()

import logging

logging.basicConfig(filename=f'../../../../logs/silver_pipeline_{args.version}.log', level=logging.INFO)
logger = logging.getLogger(__name__)

connector = DatabaseConnector.DatabaseConnector(hostname, username, password)
conn = connector.initiate_connection()



def get_context_window(connector, word_ids, allow_empty_tokens=False, context_window={'left': 20, 'right':20}, debug=False):
    # Initialize lists 
    left_contexts = []
    right_contexts = []
    mentions = []
    kwic_word_ids = []
    if debug:
        word_ids = word_ids[:20]
    
    # Initiate a connection to the database
    conn = connector.initiate_connection()
    
    try:
        with conn.cursor() as cursor:
            query = ''
            
            # Build SQL query based on on row['glaux_id']
            query = f'SELECT DISTINCT glaux_id, place_in_unit, unit_id, sentence_id FROM `wordorder` WHERE glaux_id in ({",".join(word_ids)})'
            # logging.info(query)
            
            # Execute the query to fetch glaux_id, place_in_unit and unit_id
            cursor.execute(query)

            rows = cursor.fetchall()
            # assert len(rows) == 1
            left_context_len = context_window['left']
            right_context_len = context_window['right']
            for row in tqdm(rows):
                place_in_unit_start = max(1, row['place_in_unit'] - left_context_len)

                place_in_unit_end = row['place_in_unit'] + right_context_len

                place_in_unit_str = ",".join([str(i) for i in range(place_in_unit_start, place_in_unit_end)])
                # logging.info(place_in_unit_end)
                unit_id = row['unit_id']
                # Build another SQL query to fetch words based on unit IDs and place in units and allow_empty_tokens flag
                if allow_empty_tokens:
                    query = f'SELECT sentence_id, glaux_id, word_string_NFC, place_in_unit FROM `wordorder` JOIN written_word_strings ON written_word_id = ID WHERE place_in_unit in ({place_in_unit_str}) and unit_id = {unit_id}'
                else:
                    query = f'SELECT sentence_id, glaux_id, word_string_NFC, place_in_unit FROM `wordorder` JOIN written_word_strings ON written_word_id = ID WHERE word != \'\' and word != \'E\' and place_in_unit in ({place_in_unit_str}) and unit_id = {unit_id}'

                # Execute the query to fetch words
                cursor.execute(query)
                # logging.info(query)
                rows_context = cursor.fetchall()
            
                # Retrieve the left context, right context and the mention
                if debug:
                    tokens = [row['word_string_NFC'] for row in rows_context]
                    wids = [row['glaux_id'] for row in rows_context]
                    place_in_units = [row['place_in_unit'] for row in rows_context]
                    logging.info(tokens)
                    logging.info(wids)
                    logging.info(place_in_units)
                else:
                    tokens = [row['word_string_NFC'] for row in rows_context]
                    wids = [row['glaux_id'] for row in rows_context]
                left_context = ' '.join(tokens[:wids.index(row['glaux_id'])])
                right_context = ' '.join(tokens[wids.index(row['glaux_id'])+1:])
                if debug:
                    logging.info(f'right_context = {right_context}' )
                mention = tokens[wids.index(row['glaux_id'])]
                left_contexts.append(left_context)
                right_contexts.append(right_context)
                if debug:
                    logging.info(f'right_contexts = {right_contexts}')
                mentions.append(mention)
                kwic_word_ids.append(row['glaux_id'])

    finally:
        # Ensure the connection is closed
        conn.close()
        
    return left_contexts, right_contexts, mentions, kwic_word_ids

logging.info(f'the length of the to find stuff is {len(df_to_add_mention.glaux_id.astype(str).values.tolist())}')
logging.info(f'the length of the set to find stuff is {len(set(df_to_add_mention.glaux_id.astype(str).values.tolist()))}')


left_contexts, right_contexts, mentions, kwic_word_ids = get_context_window(connector, df_to_add_mention.glaux_id.astype(str).values.tolist(), context_window={'left': int(args.context_len), 'right': int(args.context_len)}, debug=args.debug)

logging.info(f'the length of left_contexts is {len(left_contexts)}')
logging.info(f'the length of mentions is {len(mentions)}')
logging.info(f'the length of rights is {len(right_contexts)}')
logging.info(f'the length of kwic_word_ids  is {len(kwic_word_ids)}')
logging.info(f'left_contexts = {left_contexts[:5]}')
logging.info(f'right_contexts = {right_contexts[:5]}')
logging.info(f'mentions = {mentions[:5]}')



return_df = pd.DataFrame(
    {'glaux_id': kwic_word_ids,
     'mention': mentions,
     'left_context': left_contexts,
     'right_context' : right_contexts
    })

logging.info(len(return_df))


final_df = pd.merge(df_to_add_mention, return_df, on='glaux_id', how='left')

logging.info(f'loading real_df from {args.real_df}')
real = pd.read_csv(args.real_df)
logging.info(f'merging Volltext and Kurztext')
final_df = pd.merge(final_df, real[['RE_id', 'Artikel', 'Volltext', 'Kurztext', 'label_id']], on='RE_id')
assert final_df['RE_artikel'].values.tolist() == final_df['Artikel'].values.tolist()


final_df.to_csv(os.path.join(output_dir, f'{args.output_file}'))

