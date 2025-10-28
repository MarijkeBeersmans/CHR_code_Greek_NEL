#!/usr/bin/env python
# coding: utf-8

# In[44]:
import argparse

parser = argparse.ArgumentParser(description='Extract mentions (both single- and multi-token) from one of Evelien\'s exported annotations\nrequires fields: declined_form, label_id, serial (glaux_id), sec_use, tert_use, serial_central for ALL individual words in the text, serial and declined form for ALL individual words in the text in order.\n Tertiary use was included in anticipation, but no records are annotated as such, so we don\'t use it here.')
parser.add_argument('input_files', type=str, help='Path to the input csv file, separated by commas')
parser.add_argument('version')
parser.add_argument('--output_dir', default='C:/Users/u0161477/Documents/NIKAW-Prep/NIKAW-Prep/Gitlab/CHR_NEL_code/data/blink_data')
parser.add_argument('--train_test_split', type=float, default=0.8, help='Percentage of the data to use for training')
parser.add_argument('--split_batches', action='store_true', help='Split the data into batches')
parser.add_argument('--batch_size', type=int, default=32, help='Size of the batches')
parser.add_argument('--unseen_only', action="store_true")
parser.add_argument('--re_id_col', default='RE_id')
parser.add_argument('--max_n_ents', default=50)
parser.add_argument('--manual_folder', default='C:/Users/u0161477/Documents/NIKAW-Prep/NIKAW-Prep/Gitlab/CHR_NEL_code/data/blink_data/v6/manual/kurz',help="doesn't matter if it's kurz or voll because we only care about the RE_ids")

args = parser.parse_args()

import pandas as pd
import os

output_dir = os.path.join(os.path.abspath(args.output_dir), args.version, 'synthetic')

if os.path.exists(output_dir) == False:
    os.makedirs(output_dir, exist_ok=True)

import logging

logging.basicConfig(filename=f'../../../../logs/silver_pipeline_{args.version}.log', level=logging.INFO)
logger = logging.getLogger(__name__)

#-------------------------
# utility to get blink_jsonl output
#-------------------------

def to_jsonl_df(df, output_file, real_candidates='voll', language='greek'):
    dataset = []
    for row in df.itertuples():
        example = {}
        if 'left_context' in df.columns:
            example["context_left"] = row.left_context
        else:
            example["context_left"] = row.context_left
        if 'right_context' in df.columns:
            example["context_right"] = row.right_context
        else:
            example["context_right"] = row.context_right
        example["mention"] = row.mention
        example["data_status"] = 'silver'
        if isinstance(row.Volltext, str) and real_candidates == 'voll':
            example["text"] = row.Volltext
        #some items have Volltexts but not Kurztexts, in that case, we use the Voll
        elif isinstance(row.Volltext, str) and pd.notna(row.Kurztext) == False:
            example["text"] = row.Volltext
        else:
            example["text"] = row.Kurztext
        example["label_id"] = row.label_id
        example["RE_id"] = row.RE_id
        example["label_title"] = row.RE_artikel
        if language == 'greek':
            example["glaux_id"] = row.glaux_id
        else:
            example["serial"] = row.glaux_id
            example['world'] = row.subset

        dataset.append(example)

    import json

    with open(output_file, 'w', encoding='UTF-8') as f:
        for entry in dataset:
            json.dump(entry, f, ensure_ascii=False)
            f.write('\n')

#-------------------------
# Load the data
#-------------------------
total_data = pd.DataFrame()

for file in args.input_files.split(','):
    df = pd.read_csv(file, dtype=str)
    total_data = pd.concat([total_data, df])

#-------------------------
# Split the data into train, dev and test sets, allowing for no overlap between entity sets
#-------------------------

import os

if os.path.exists(output_dir) == False:
    os.makedirs(output_dir)

from sklearn.model_selection import train_test_split, GroupShuffleSplit


def shuffle_and_split_dataset(df, split_percentage=0.8, random_state=None):
    """
    Shuffles and splits a dataset into two DataFrames based on the given percentage.

    Parameters:
    - df (pd.DataFrame): The DataFrame to shuffle and split.
    - split_percentage (float): The percentage of the data to put in the first split (default is 0.8).
    - random_state (int, optional): Random state seed for reproducibility.

    Returns:
    - train (pd.DataFrame): The first split of the dataset.
    - df_test (pd.DataFrame): The second split of the dataset.
    """

    # Shuffle and split the DataFrame
    train, df_test = train_test_split(df, test_size=(1 - split_percentage), random_state=random_state, shuffle=True)

    return train, df_test


def shuffle_and_split_by_group(df, group_column, split_percentage=0.8, random_state=None):
    """
    Shuffles and splits a dataset into two DataFrames based on groups with no group overlap.

    Parameters:
    - df (pd.DataFrame): The DataFrame to shuffle and split.
    - group_column (str): The column name that identifies groups.
    - split_percentage (float): The percentage of groups to put in the first split (default is 0.8).
    - random_state (int, optional): Random state seed for reproducibility.

    Returns:
    - train (pd.DataFrame): The first split of the dataset (with a subset of groups).
    - df_test (pd.DataFrame): The second split of the dataset (with the remaining groups).
    """

    # Extract unique group identifiers and shuffle them
    unique_groups = df[group_column].drop_duplicates()
    groups_train, groups_test = train_test_split(
        unique_groups,
        test_size=(1 - split_percentage),
        random_state=random_state,
        shuffle=True
    )

    # Filter the DataFrame based on the split groups
    train = df[df[group_column].isin(groups_train)]
    df_test = df[df[group_column].isin(groups_test)]

    return train, df_test

def groupshufflesplit_by_manual(total_data, re_id_col, train_test_split, manual_folder, random_state=45):
    test_manual = pd.read_json(os.path.join(manual_folder, 'test.jsonl'), dtype=str, lines=True)
    train_manual = pd.read_json(os.path.join(manual_folder, 'train.jsonl'),dtype=str, lines=True)
    valid_manual =pd.read_json(os.path.join(manual_folder, 'valid.jsonl'),dtype=str, lines=True)

    re_ids_in_test = test_manual[re_id_col].values.tolist()
    print(len(re_ids_in_test))
    re_ids_in_train = train_manual[re_id_col].values.tolist()
    print(len(re_ids_in_train))
    re_ids_in_valid = valid_manual[re_id_col].values.tolist()
    print(len(re_ids_in_valid))
    re_ids_in_manual = re_ids_in_test + re_ids_in_train + re_ids_in_valid 

    # split the total data already on these
    test_start_total = total_data[total_data[re_id_col].isin(set(re_ids_in_test))].reset_index(drop=True)
    train_start_total = total_data[total_data[re_id_col].isin(set(re_ids_in_train))].reset_index(drop=True)
    valid_start_total = total_data[total_data[re_id_col].isin(set(re_ids_in_valid))].reset_index(drop=True)

    unique_synthetic = total_data[total_data[re_id_col].isin(set(re_ids_in_manual))==False].reset_index(drop=True)

    train_rest, test_rest = shuffle_and_split_by_group(unique_synthetic, re_id_col, train_test_split, random_state)
    # print(train_rest[['context_left', 'mention', 'kurztext']].head())
    print(len(train_rest), len(test_rest))
 
    test_rest, valid_rest = shuffle_and_split_by_group(test_rest, re_id_col, 0.5, random_state)

    if len(train_start_total) > 0:
        train_final = pd.concat([train_start_total, train_rest])
    else:
        train_final = train_rest
            # print(train_final[['context_left', 'mention', 'kurztext']].head())
    if len(test_start_total) > 0:  
        test_final = pd.concat([test_start_total, test_rest])
    else:
        test_final = test_rest
    # print(test_final[['context_left', 'mention', 'kurztext']].head())
    if len(valid_start_total) > 0:  
        valid_final = pd.concat([valid_start_total, valid_rest])
    else:
        valid_final = valid_rest

    #finally, remove any glaux_id that is also in the gold
    glaux_ids_in_test = test_manual['glaux_id'].values.tolist()
    glaux_ids_in_train = train_manual['glaux_id'].values.tolist()
    glaux_ids_in_valid = valid_manual['glaux_id'].values.tolist()

    glaux_ids_in_manual = glaux_ids_in_test + glaux_ids_in_train + glaux_ids_in_valid 

    print(f'len before removing GLAUx_ids that are in the gold: train {len(train_final)}, valid {len(valid_final)}, test {len(test_final)}')

    indexes = train_final.loc[train_final['glaux_id'].isin(glaux_ids_in_manual)].index
    train_final.drop(indexes, inplace=True)
    train_final.reset_index(drop=True, inplace=True)

    indexes = valid_final.loc[valid_final['glaux_id'].isin(glaux_ids_in_manual)].index
    valid_final.drop(indexes, inplace=True)
    valid_final.reset_index(drop=True, inplace=True)

    indexes = test_final.loc[test_final['glaux_id'].isin(glaux_ids_in_manual)].index
    test_final.drop(indexes, inplace=True)
    test_final.reset_index(drop=True, inplace=True)

    print(f'len after removing GLAUx_ids that are in the gold: train {len(train_final)}, valid {len(valid_final)}, test {len(test_final)}')


    return train_final, test_final, valid_final


if args.unseen_only:
    train, test, dev = groupshufflesplit_by_manual(total_data, args.re_id_col, args.train_test_split, args.manual_folder)
else:
    train, test = shuffle_and_split_dataset(total_data, args.train_test_split, random_state=45)

    test, dev = shuffle_and_split_dataset(test, 0.5, random_state=45)

import pandas as pd
from tqdm import tqdm


def split_batches(df, batch_size, id_name):
    #maybe upsample instead of delete?
    """
    Splits a DataFrame into batches of a specified size while ensuring that each batch 
    does not contain duplicate values for a given ID key.

    Parameters:
        df (pd.DataFrame): The input DataFrame.
        batch_size (int): The desired batch size.
        id_name (str): The column name used to prevent duplicate IDs within batches.

    Returns:
        list of pd.DataFrame: A list of DataFrames, each containing rows of batch_size (if possible).
     """

    # Initialize list to store batches and their unique ID sets
    batches = []
    batch_id_sets = []

     # Create a new batch
    def new_batch():
        batches.append(pd.DataFrame(columns=df.columns))
        batch_id_sets.append(set())

    new_batch() # Start with the first batch

     # Iterate through the DataFrame efficiently
    for index, row in tqdm(df.iterrows(), total=len(df)):
        # Try to place row in an existing batch
        placed = False
        for i in range(len(batches)):
            if row[id_name] not in batch_id_sets[i] and len(batches[i]) < batch_size:
               batches[i] = batches[i]._append(row)
               batch_id_sets[i].add(row[id_name])
               placed = True
               break # Stop searching after placement
        # If the row couldn't fit into an existing batch, create a new one
        if not placed:
            new_batch()
            batches[-1] = batches[-1]._append(row)
            batch_id_sets[-1].add(row[id_name])


    # Remove incomplete batches if necessary
    final_batches = [batch for batch in batches if len(batch) == batch_size]

    # Combine all batches into a single DataFrame
    combined_df = pd.concat(final_batches, ignore_index=True)

    return combined_df


if not os.path.exists(os.path.join(output_dir, 'voll')):
    os.mkdir(os.path.join(output_dir, 'voll'))

if not os.path.exists(os.path.join(output_dir, 'kurz')):
    os.mkdir(os.path.join(output_dir, 'kurz'))


def undersample_df(df, re_id_col, max_n=50):
    # Apply sampling to each group and reset the index
    undersampled_df = (
        df.groupby(re_id_col, group_keys=False)
        .apply(lambda group: group.sample(n=max_n, random_state=42) if len(group) > max_n else group)
        .sample(frac=1, random_state=42) # Shuffle the final result
        .reset_index(drop=True)
    )
    return undersampled_df

train = undersample_df(train, args.re_id_col, args.max_n_ents)
test = undersample_df(test, args.re_id_col, args.max_n_ents)
dev = undersample_df(dev, args.re_id_col, args.max_n_ents)

logging.info(f'train_test_split successfull with train set of length {len(train)}, dev set of length {len(dev)} and test set of length {len(test)}.')

to_jsonl_df(train, os.path.join(output_dir, 'voll', 'train.jsonl'), real_candidates='voll')
to_jsonl_df(dev, os.path.join(output_dir, 'voll', 'valid.jsonl'), real_candidates='voll')
to_jsonl_df(test, os.path.join(output_dir, 'voll', 'test.jsonl'), real_candidates='voll')

to_jsonl_df(train, os.path.join(output_dir, 'kurz', 'train.jsonl'), real_candidates='kurz')
to_jsonl_df(dev, os.path.join(output_dir, 'kurz', 'valid.jsonl'), real_candidates='kurz')
to_jsonl_df(test, os.path.join(output_dir, 'kurz', 'test.jsonl'), real_candidates='kurz')

logging.info(f'voll and kurz versions saved.')


# In[ ]: