#--------------------
# Configuration
#--------------------
import sys
import logging
sys.path.insert(1, "../../../../../glaux_database_connector")

import GlauxData
import DatabaseConnector
import pandas as pd
import re
from tqdm import tqdm
import getpass


hostname = 'no sorry'
username = 'Marijke'
password = getpass.getpass()

# username = 'Marijke'


from argparse import ArgumentParser

parser = ArgumentParser()

parser.add_argument('version', type=str)

args = parser.parse_args()

logging.basicConfig(filename=f'../../../../logs/silver_pipeline_{args.version}.log', level=logging.INFO)
logger = logging.getLogger(__name__)

# Maak een handler voor het loggen naar de console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

# Voeg de console handler toe aan de root logger
logging.getLogger().addHandler(console_handler)

def main():
    TM_nam_lemma = pd.read_csv(f'../outputs/{args.version}/TM_Glaux_text_links_nam_and_lemma_{args.version}.csv')

    #about half do not contain passage information at all
    logging.info(f'Number of entities that contain paragraph information {len(TM_nam_lemma[TM_nam_lemma.start_passage != "missing"])}')
    TM_nam_lemma['start_end_passage'] = TM_nam_lemma.apply(lambda x: list(zip(x['start_passage'].split(','), x['end_passage'].split(','))), axis=1)
    logging.info(f"sanity check: \n{TM_nam_lemma['start_end_passage'].head()}")
    logging.info(f'length before exploding {len(TM_nam_lemma)}')
    # we explode, so that if we have multiple possible passages, they all have their own row
    TM_nam_lemma_exploded = TM_nam_lemma.explode('start_end_passage')
    TM_nam_lemma_exploded['start_passage_good'] = TM_nam_lemma_exploded.start_end_passage.apply(lambda x: x[0])
    TM_nam_lemma_exploded['end_passage_good'] = TM_nam_lemma_exploded.start_end_passage.apply(lambda x: x[1])
    logging.info(f'length after exploding{len(TM_nam_lemma_exploded)}')
    logging.info(f'sanity check:{TM_nam_lemma_exploded.head()}')


    def clean_struct(passage):
        #remove the final dot 
        pattern = r'\.(?=[a-zA-Z])'
        replacement = ""
        passage = re.sub(pattern, replacement, passage)
        #add final part to final thing if it's a alphabetical char
        parts = passage.strip('.').replace('..', '.').split('.')
        if parts[-1].isalpha():
            last_part = ''.join(parts[-2:])
            passage = '.'.join(parts[:-2] + [last_part])
        else:
            passage = '.'.join(parts)
        return passage

    
    logging.info(f"fixing the passages: \n {TM_nam_lemma_exploded[['start_passage_good', 'end_passage_good']].sample(n=10, random_state=25)}")
    TM_nam_lemma_exploded['start_passage_good'] = TM_nam_lemma_exploded.start_passage_good.apply(lambda x: clean_struct(x))
    TM_nam_lemma_exploded['end_passage_good'] = TM_nam_lemma_exploded.end_passage_good.apply(lambda x: clean_struct(x))
    logging.info(f"fixed \n {TM_nam_lemma_exploded[['start_passage_good', 'end_passage_good', 'STRUCT_C', 'STRUCT_NC']].sample(n=10, random_state=25)}")

    connector = DatabaseConnector.DatabaseConnector(hostname, username, password)
    conn = connector.initiate_connection()

    text_components_df = pd.DataFrame()       

    for name, group in TM_nam_lemma_exploded.groupby('GLAUX_TEXT_ID'):
        components = group['start_passage_good'].values.tolist() + group['end_passage_good'].values.tolist()
        components = [component for component in components if component != 'missing']
        if len(components) != 0:
            components = set(components)
            components = "','".join(list(components))
            query = f'''SELECT unit_id, startpos, endpos, component_type, text_components.name 
                    FROM text_components 
                    JOIN text_metadata ON unit_id = text_metadata.ID 
                    WHERE 
                    unit_id = {name} AND 
                    text_components.name in ('{components}');'''
            # print(query)

        try:
            with conn.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()
                #make a mapping from 
                rows_df = pd.DataFrame(rows)
                text_components_df = pd.concat([text_components_df, rows_df])


        except Exception as e:
            logging.info(f'querying or saving failed with exception {e}')

            # conn = connector.initiate_connection()       

    #drop the duplicates
    text_components_df.drop_duplicates(keep='first', inplace=True)
    text_components_df.reset_index(drop=True, inplace=True)
    text_components_df.rename(columns={'name': 'name_component_level'}, inplace=True)


    glaux_ids = text_components_df['startpos'].values.tolist() + text_components_df['endpos'].values.tolist()

    query = f'''SELECT glaux_id, place_in_unit
                FROM wordorder 
                WHERE 
                glaux_id in ({','.join(map(str,glaux_ids))});'''
        # print(query)

    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            #make a mapping from 
            glaux_id2_place_in_unit = {row['glaux_id']: row['place_in_unit'] for row in rows}
            text_components_df['startpos_place_in_unit'] = text_components_df['startpos'].map(glaux_id2_place_in_unit)
            text_components_df['endpos_place_in_unit'] = text_components_df['endpos'].map(glaux_id2_place_in_unit)


    except Exception as e:
        logging.info(f'querying failed with exception {e}')

    logging.info(f'sanity check text_components \n {text_components_df.head()}')
    logging.info(f'found {len(text_components_df)} relevant text components')


    lemma_ids = ','.join(TM_nam_lemma_exploded['lemma_id'].apply(lambda x: x.replace('[','').replace(']', '').replace(' ', '')).values.tolist())
    lemma_ids = set(lemma_ids.split(','))
    lemma_ids = list(lemma_ids)
    print(lemma_ids[:5])
    lemma_ids = ','.join(lemma_ids)
    unit_ids = ','.join(map(str, TM_nam_lemma_exploded['GLAUX_TEXT_ID'].unique()))

    query = f'''SELECT glaux_id, lemma_id, sentence_id, unit_id, place_in_unit, word_string_NFC 
        FROM wordorder 
        JOIN written_word_strings ON written_word_id = ID 
        WHERE 
        lemma_id in ({lemma_ids}) AND
        unit_id in ({unit_ids});'''
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            #make a mapping from 
            possible_words = pd.DataFrame(rows)

    except Exception as e:
        print(f'querying failed with exception {e}')
    
    logging.info(f'sanity check possible words {possible_words.head()}')
    possible_words = possible_words.rename(columns={'lemma_id': 'mention_lemma_id'})

    final_mentions = []

    import copy
    from tqdm import tqdm

    test = 0

    for item in tqdm(TM_nam_lemma_exploded.to_dict(orient='records')):
        start_passage = item['start_passage_good']
        end_passage = item['end_passage_good']
        GLAUx_text_id = item['GLAUX_TEXT_ID']
        lemma_id = item['lemma_id'].replace('[', '').replace(']', '').replace(' ', '')
        lemma_ids = lemma_id.split(',')
        #find the possible components in the text_components_df
        if start_passage == end_passage:
            # we find when the possible component name is equal to the passage and when the
            # the unit id = the glaux text id (when this component is in the same text)
            # we exclude "books" because they are too large generally
            possible_components = text_components_df.loc[(text_components_df['name_component_level'] == start_passage)&(text_components_df['unit_id'] == GLAUx_text_id)&(text_components_df['component_type'] != 'book')]
            if len(possible_components) != 0:
                for component in possible_components.itertuples():
                    # check stuff
                    component_start_pos = component.startpos_place_in_unit
                    component_end_pos = component.endpos_place_in_unit
                    potential_mentions = possible_words.loc[
                        (possible_words['unit_id'] == GLAUx_text_id)&
                        (possible_words['place_in_unit'] > component_start_pos)&
                        (possible_words['place_in_unit'] < component_end_pos)&
                        (possible_words['mention_lemma_id'].astype(str).isin(lemma_ids))
                        ].to_dict(orient='records')
                    for potential_mention in potential_mentions:
                        component_dict = component._asdict()
                        mention_full = copy.copy(item)
                        mention_full.update(potential_mention)
                        mention_full.update(component_dict)
                        final_mentions.append(mention_full)
        else:
            possible_components_start = text_components_df.loc[(text_components_df['name_component_level'] == start_passage)&(text_components_df['unit_id'] == GLAUx_text_id)]
            possible_components_end = text_components_df.loc[(text_components_df['name_component_level'] == end_passage)&(text_components_df['unit_id'] == GLAUx_text_id)]
            possible_components_start = possible_components_start.rename(columns={column: column + '_start' for column in possible_components_start.columns})
            possible_components_end = possible_components_end.rename(columns={column: column + '_end' for column in possible_components_end.columns})
            
            if len(possible_components_start) == len(possible_components_end) and len(possible_components_start) != 0:
                possible_components_double = pd.concat([possible_components_start, possible_components_end], axis=1)
                for component in possible_components_double.itertuples():
                    component_start_pos = component.startpos_place_in_unit_start
                    component_end_pos = component.endpos_place_in_unit_end
                    potential_mentions = possible_words.loc[
                        (possible_words['unit_id'] == GLAUx_text_id)&
                        (possible_words['place_in_unit'] > component_start_pos)&
                        (possible_words['place_in_unit'] < component_end_pos)&
                        (possible_words['mention_lemma_id'].astype(str).isin(lemma_ids))
                        ].to_dict(orient='records')
                    for potential_mention in potential_mentions:
                        component_dict = component._asdict()
                        mention_full = copy.copy(item)
                        mention_full.update(potential_mention)
                        mention_full.update(component_dict.pop('Index'))
                        final_mentions.append(mention_full)

    
    test = pd.DataFrame(final_mentions)
    logging.info(f'dropping duplicates, length {len(test)}')
    test.drop_duplicates(subset='glaux_id', keep=False, inplace=True)
    logging.info(f'dropped duplicates, length {len(test)}')
    test.to_csv(f'../outputs/{args.version}/final_mentions_without_books_{args.version}.csv')
    logging.info(f'number of mentions: {len(test)}')
    logging.info(f'sanity check mentions: \n {len(test)}')

if __name__ == '__main__':
    main()