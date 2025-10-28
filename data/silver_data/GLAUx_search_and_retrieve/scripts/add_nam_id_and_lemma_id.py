#--------------------
# Configuration
#--------------------
import sys
import logging

sys.path.insert(1, "../../../../../glaux_database_connector")
import lxml
import os
import lxml.etree
import GlauxData
import DatabaseConnector
import pandas as pd
import getpass

hostname = 'ghum-s-atrmg1.luna.kuleuven.be' 
username = 'Marijke'
password = getpass.getpass()

from argparse import ArgumentParser

parser = ArgumentParser()

parser.add_argument('version', type=str)

args = parser.parse_args()

logging.basicConfig(filename=f'../../../../logs/silver_pipeline_{args.version}.log', level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    #--------------------
    # Read in the 
    #--------------------
    links_w_glaux_texts = pd.read_csv(f'../outputs/{args.version}/TM_Glaux_text_links_{args.version}.csv')

    #--------------------
    # add nam_id
    #--------------------

    import numpy as np

    df_re_to_nam = pd.read_csv('../../mappings/re_id_to_nam_id.tab', sep='\t', header=None, dtype={0: 'Int64'})
    df_re_to_nam.dropna(inplace=True)
    df_re_to_nam = df_re_to_nam[df_re_to_nam != 'missing']
    df_re_to_nam.rename(columns={0: 'RE_id', 1: 'nam_id'}, inplace=True)

    re_to_nam = {}

    for RE_id in df_re_to_nam.RE_id.unique().tolist():
        possible_nam_ids = df_re_to_nam[df_re_to_nam.RE_id == RE_id].nam_id.values.tolist()
        final_nams = []
        for possible_nam_id in possible_nam_ids:
            if not possible_nam_id == '0':
                if ':' in str(possible_nam_id):
                    final_nams.extend(map(int, possible_nam_id.split(': ')[1].split(' / ')))
                else:
                    if str(possible_nam_id).isnumeric():
                        possible_nam_id = int(possible_nam_id)
                        final_nams.append(possible_nam_id)

        re_to_nam[RE_id] = final_nams

    links_w_glaux_texts['nam_id'] = links_w_glaux_texts.RE_id.map(re_to_nam, na_action='ignore')
    logging.info(f'nam_ids added to {len(links_w_glaux_texts["nam_id"]) - links_w_glaux_texts["nam_id"].isnull().sum()} of {len(links_w_glaux_texts)} linked sources')
    logging.info(f'dropping those without nam_ids:')
    logging.info(f"{links_w_glaux_texts[links_w_glaux_texts['nam_id'].isnull() == True].RE_id.value_counts()}")

    links_w_glaux_texts.dropna(subset='nam_id', inplace=True)
    links_w_glaux_texts.reset_index(drop=True, inplace=True)


    #--------------------
    # add lemma_id
    #--------------------
    ## read and link mark's  that links tm nam_id's to glaux lemma's
    df_nam_id_to_glaux_lemma = pd.read_csv('C:/Users/u0161477/Documents/NIKAW-Prep/NIKAW-Prep/Gitlab/CHR_NEL_code/data/silver_data/mappings/glaux_lemma_TM_Nam_nieuw.csv', header=None)

    df_nam_id_to_glaux_lemma.rename(columns = {0: 'glaux_id',
                        4: 'tm_id',
                        5: 'tm_id_2',
                        6: 'tm_id_3'}, inplace=True)


    tm_to_glaux_lemma = {}
    for row in df_nam_id_to_glaux_lemma.itertuples():
        for tm_id in ['tm_id', 'tm_id_2', 'tm_id_3']:
            current_tm_id = getattr(row, tm_id)
            if current_tm_id != '' and current_tm_id not in tm_to_glaux_lemma.keys():
                tm_to_glaux_lemma[current_tm_id] = [getattr(row, 'glaux_id')]
            else:
                tm_to_glaux_lemma[current_tm_id].append(getattr(row, 'glaux_id'))



    def map_tm_to_glaux_lemma(tm):
        lemma_ids = []
        for x in tm:
            if x in tm_to_glaux_lemma:
                if isinstance(tm_to_glaux_lemma[x], str):
                    lemma_ids.append(tm_to_glaux_lemma[x])
                else:
                    assert isinstance(tm_to_glaux_lemma[x], list)
                    lemma_ids.extend(tm_to_glaux_lemma[x]) 
        return lemma_ids

    links_w_glaux_texts['lemma_id'] = links_w_glaux_texts.nam_id.map(map_tm_to_glaux_lemma)
    logging.info(f"lemma_ids added to {len(links_w_glaux_texts[links_w_glaux_texts['lemma_id'].map(len) != 0])} of {len(links_w_glaux_texts)} linked sources")
    final_df = links_w_glaux_texts[links_w_glaux_texts['lemma_id'].map(len) != 0]
    final_df.reset_index(drop=True, inplace=True)
    print(final_df['lemma_id'].head(10))
    final_df.to_csv(f'../outputs/{args.version}/TM_Glaux_text_links_nam_and_lemma_{args.version}.csv')
    

if __name__ == '__main__':
    main()