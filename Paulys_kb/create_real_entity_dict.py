
import argparse
import pandas as pd
import numpy as np

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument("input_export", type=str, help="Path to the input file")
arg_parser.add_argument("output_file_name", type=str, help="Path to the output file")
arg_parser.add_argument("--real_candidates", type=str, default='Voll', help="Whether to create a Volltext or a Kurztext")

args = arg_parser.parse_args()

import pandas as pd
df = pd.read_csv(args.input_export, dtype={'RE_id': str})
print(len(df))
# %%

def to_jsonl_df(df, output_file):
    entities = []
    count = 0
    for row in df.itertuples():
        if isinstance(row.RE_id, str) == True:
            example = {}
            if pd.notna(row.Volltext) == False and pd.notna(row.Kurztext) == False:
                example['text'] = ' '
            elif isinstance(row.Volltext, str) and args.real_candidates == 'Voll':
                example["text"] = row.Volltext
            #some items have Volltexts but not Kurztexts, in that case, we use the Voll
            elif isinstance(row.Volltext, str) and pd.notna(row.Kurztext) == False:
                example["text"] = row.Volltext
            else:
                example["text"] = row.Kurztext
            #if we just want the kurztext
            example["title"] = row.Artikel
            assert row.label_id == count
            example["label_id"] = count
            example['RE_id'] = row.RE_id
            entities.append(example)
            count = count + 1

    import json

    with open(output_file, 'w', encoding='UTF-8') as f:
        for entry in entities:
            print(entry)
            json.dump(entry, f, ensure_ascii=False)
            f.write('\n')

    print(f"Saved {len(entities)} entries to {output_file}")
to_jsonl_df(df, args.output_file_name + f'_{args.real_candidates}.jsonl')