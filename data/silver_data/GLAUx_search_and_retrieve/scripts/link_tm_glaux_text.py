#--------------------
# preparation to connect to GLAUx
#--------------------
import sys
import logging
import lxml
import os
import lxml.etree


from argparse import ArgumentParser

parser = ArgumentParser()

parser.add_argument('version', type=str)

args = parser.parse_args()

logging.basicConfig(filename=f'../../../../logs/silver_pipeline_{args.version}.log', level=logging.INFO)
logger = logging.getLogger(__name__)
sys.path.insert(1, "../../../../../glaux_database_connector")
import GlauxData
import DatabaseConnector
import pandas as pd
import re

hostname = 'no_sorry' 
username = 'Marijke'
password = 'no sorry'

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)

# Voeg de console handler toe aan de root logger
logging.getLogger().addHandler(console_handler)


import re

def replace_dollars(text):
    # This pattern matches two or more consecutive $ symbols
    pattern = r'\${2,}'
    # Replace each match with the same number of dots
    result = re.sub(pattern, lambda m: '.' * len(m.group()), text)
    return result


def contains_non_zero_numbers(text):
    # This pattern matches any digit from 1 to 9
    pattern = r'[1-9]'
    # Search for the pattern in the text
    match = re.search(pattern, text)
    # Return True if a match is found, otherwise False
    return match is not None


def split_at_middle_dollar(text):
    # Find the middle index of the string
    middle_index = len(text) // 2

    # Initialize variables to track the closest $ position
    closest_dollar_index = -1
    min_distance = len(text)

    # Iterate through the string to find the closest $ to the middle
    for i, char in enumerate(text):
        if char == '$':
            distance = abs(i - middle_index)
            if distance < min_distance:
                closest_dollar_index = i
                min_distance = distance

    # If no $ is found, return the original string
    if closest_dollar_index == -1:
        return text

    # Split the string at the closest $ position
    part1 = text[:closest_dollar_index]
    part2 = text[closest_dollar_index + 1:]

    return part1, part2




def main():

    #--------------------
    # Combine author and non-author results from Trismegistos expansion
    #--------------------

    df_tmexpanded_authors = pd.read_csv('../../trismegistos_expansion/output/tm_linked_sources_authors.csv', index_col=0)
    df_tmexpanded_authors['subset'] = 'author'
    df_tmexpanded_non_authors = pd.read_csv('../../trismegistos_expansion/output/tm_linked_sources_non_authors.csv', index_col=0)
    df_tmexpanded_non_authors['subset'] = "non_author"

    df_tmexpanded = pd.concat([df_tmexpanded_authors, df_tmexpanded_non_authors])
    df_tmexpanded.reset_index(drop=True, inplace=True)

    #--------------------
    # Clean Trismegistos expansion
    #--------------------

    #get all cells that have matched using the algorithm (script get_tm_authorwork_ids)
    df_match_found = df_tmexpanded.loc[df_tmexpanded['original_response'].str.startswith("b'\\r\\n\\r\\n\\r\\n\\r\\nAUTHORMatch_CONDITIONALAuthorwork") == True, :].copy()
    df_match_found.drop(columns=[f'start_passage_{n}' for n in range(8)], inplace=True)
    df_match_found.drop(columns=[f'end_passege_{n}' for n in range(8)], inplace=True)
    df_match_found.reset_index(drop=True, inplace=True)


    # Iterate through the DataFrame rows
    for row in df_match_found.itertuples():
        orig_response = getattr(row, 'original_response')
        start, tm_author_work, human_readable_title, machine_readable = orig_response.split('@')

        if not contains_non_zero_numbers(machine_readable):
            row_start_passage = 'missing'
            row_end_passage = 'missing'
        else:
            possible_start_end_passages = machine_readable.split('|')
            row_start_passage = []
            row_end_passage = []

            for possible_start_end_passage in possible_start_end_passages:
                if contains_non_zero_numbers(possible_start_end_passage):
                    split_index = possible_start_end_passage.find('$')
                    machine_readable_passage_info = possible_start_end_passage[:split_index].strip()
                    machine_readable_numbers = possible_start_end_passage[split_index + 1:][:-3].strip()
                    passage1, passage2 = split_at_middle_dollar(machine_readable_numbers)

                    try:
                        if machine_readable_passage_info.split(' - ')[0] == machine_readable_passage_info.split(' - ')[1] and not passage1 == passage2:
                            print(f'some sort of mistake is happening in {machine_readable_passage_info} with {machine_readable_numbers}')
                        else:
                            if '..' not in passage1.replace('$', '.'):
                                row_start_passage.append(passage1.replace('$', '.')) 
                            if '..' not in passage2.replace('$', '.'):
                                row_end_passage.append(passage2.replace('$', '.')) 
                    except IndexError:
                        logging.info(f"error in {row.Index}: - split not possible for {machine_readable}")
                        continue
            if len(row_start_passage) == 0 or len(row_end_passage) == 0:
                row_start_passage = 'missing_or_incomplete'
                row_end_passage = 'missing_or_incomplete'
            else:
                row_start_passage = ','.join(row_start_passage)
                row_end_passage = ','.join(row_end_passage)

        # Add the results to the DataFrame
        df_match_found.at[row.Index, 'start_passage'] = row_start_passage
        df_match_found.at[row.Index, 'end_passage'] = row_end_passage





    logging.info(f"""Number of sources extracted from the RE entries: {len(df_tmexpanded)},
                 Number of those sources the TM algorithm could link to a text:  {len(df_match_found)},
                {len(df_match_found[df_match_found['subset'] == 'author'])} of those are authors, 
                {len(df_match_found[df_match_found['subset'] == 'non_author'])} are non authors""")
    


    logging.info(f"""Number of sources extracted from the RE entries: {len(df_tmexpanded)},
                 Number of those sources the TM algorithm could link to a text:  {len(df_match_found)},
                {len(df_match_found[df_match_found['subset'] == 'author'])} of those are authors, 
                {len(df_match_found[df_match_found['subset'] == 'non_author'])} are non authors""")

    df_match_found.reset_index(drop=True, inplace=True)

    #--------------------
    # Access GLAUx for a mapping from the GLAUx text id to the TM Authorwork id and map
    #--------------------


    # get the correct authorworks
    authorwork_ids = [str(authorwork_id).replace('https://www.trismegistos.org/text/', '') for authorwork_id in df_match_found.tm_authorwork_id.values if authorwork_id != 'missing']
    authorwork_ids = set(authorwork_ids)

    query = f"SELECT ID, TM_authorWork FROM text_metadata WHERE TM_authorWork in  ({','.join(authorwork_ids)});"

    dct = {}

    connector = DatabaseConnector.DatabaseConnector(hostname, username, password)

    conn = connector.initiate_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
    finally:
        conn.close()
    for row in rows:
        dct[row['TM_authorWork'].replace('\r', '')] = str(row['ID'])

    # there are no duplicates so this is fine
    new_dct = {}

    for key, value in dct.items():
        new_dct.update({item:value for item in key.split(' / ')})

    #map the tm_authorwork_ids to the glaux_ids 
    df_match_found['GLAUX_TEXT_ID'] = df_match_found['tm_authorwork_id'].map(new_dct, na_action='ignore')


    #manually added a very frequent text that wasn't linked to glaux but was in glaux after all
    df_match_found.loc[df_match_found.tm_authorwork_id == '93', 'GLAUX_TEXT_ID'] = '1700'

    #check how many references are in texts that are actually in glaux
    len(df_match_found) - df_match_found.GLAUX_TEXT_ID.isnull().sum()

    df_match_found.dropna(subset='GLAUX_TEXT_ID', inplace=True)
    df_match_found.reset_index(drop=True, inplace=True)
    
    logging.info(f"""Number of sources linked to texts that are in GLAUx: {len(df_match_found)}""")

    #--------------------
    # Retrieve GLAUx metadata
    #--------------------

    metadata = os.path.normpath("Glaux/1.0/metadata.xlsx")

    df_glaux_metadata = pd.read_excel(metadata, dtype={'GLAUX_TEXT_ID': 'object'})

    def adjust_STRUCT_C(row):
        ## Check for "line" attributes in the texts themselves
        glaux_file = row.TLG
        STRUCT_C = row.STRUCT_C
        try:
            glaux_map = os.path.normpath('Glaux/1.0/final_xml')
            # Use iterparse to incrementally parse the XML file
            xml_file_path = os.path.join(glaux_map, glaux_file + '.xml')
            context = lxml.etree.iterparse(xml_file_path, events=("start",))
        
            for event, elem in context:
                # Check if the element is a "word"
                if elem.tag == "word":
                    # Check if it has a "line" attribute
                    if "line" in elem.attrib:
                        if not isinstance(STRUCT_C, str):
                            return 'line'
                        else:
                            return ','.join(STRUCT_C.split(',') + ['line'])
                    else:
                        return STRUCT_C
            return STRUCT_C
        except FileNotFoundError:
            return STRUCT_C

    df_glaux_metadata['STRUCT_C'] = df_glaux_metadata.apply(adjust_STRUCT_C, axis=1)

    # put both in same datatype to merge
    df_glaux_metadata.GLAUX_TEXT_ID = df_glaux_metadata.GLAUX_TEXT_ID.astype(str)
    df_match_found.GLAUX_TEXT_ID = df_match_found.GLAUX_TEXT_ID.astype(str)
    in_glaux_entities = df_match_found.merge(df_glaux_metadata[['GLAUX_TEXT_ID', 'STRUCT_NC', 'STRUCT_C', 'TOKENS']], how='inner', on='GLAUX_TEXT_ID')
    logging.info(f'{len(in_glaux_entities)} linked sources \w GLAUx metadata written to data/silver_dfata/outputs/{args.version}/TM_Glaux_text_links.csv')

    output_dir = f'../outputs/{args.version}'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    in_glaux_entities.to_csv(f'../outputs/{args.version}/TM_Glaux_text_links_{args.version}.csv')

if __name__ == '__main__':
    main()
