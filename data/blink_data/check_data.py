import json
import sys
import argparse
from collections import defaultdict
from typing import List, Dict, Set
import pandas as pd

REQUIRED_FIELDS = [
    "context_left", "context_right", "mention", "data_status",
    "text", "label_id", "RE_id", "label_title", "glaux_id"
]

def load_jsonl_file(filepath: str) -> List[Dict]:
    with open(filepath, 'r', encoding='utf-8') as f:
        return [json.loads(line.strip()) for line in f if line.strip()]

def check_missing_or_empty_fields(data: List[Dict], filename: str) -> List[str]:
    issues = []
    for i, entry in enumerate(data):
        for field in REQUIRED_FIELDS:
            if field not in entry or entry[field] in ("", None, []):
                issues.append(f"{filename}, line {i+1}: Missing or empty field '{field}'")
    return issues

def check_duplicates(data: List[Dict]) -> List[str]:
    seen_glaux_ids = []
    doubles = []
    for i, entry in enumerate(data):
        if entry['glaux_id'] in set(seen_glaux_ids):
            doubles.append(entry['glaux_id'])
        seen_glaux_ids.append(entry['glaux_id'])
    return doubles


def analyze_jsonl_files(filepaths: List[str]) -> Dict[str, List[str]]:
    re_ids_by_file = {}
    glaux_ids_by_file = {}
    all_issues = []
    doubles = {}

    for filepath in filepaths:
        data = load_jsonl_file(filepath)
        all_issues.extend(check_missing_or_empty_fields(data, filepath))

        doubles[filepath] = check_duplicates(data)

        re_ids = {entry['RE_id'] for entry in data if 'RE_id' in entry and entry['RE_id']}
        glaux_ids = {entry['glaux_id'] for entry in data if 'glaux_id' in entry and entry['glaux_id']}

        re_ids_by_file[filepath] = re_ids
        glaux_ids_by_file[filepath] = glaux_ids

    re_overlaps = defaultdict(set)
    glaux_overlaps = defaultdict(set)
    re_unique_f1s = defaultdict(set)
    re_unique_f2s = defaultdict(set)


    for i in range(len(filepaths)):
        for j in range(i + 1, len(filepaths)):
            f1, f2 = filepaths[i], filepaths[j]
            re_overlap = re_ids_by_file[f1].intersection(re_ids_by_file[f2])
            glaux_overlap = glaux_ids_by_file[f1].intersection(glaux_ids_by_file[f2])
            re_unique_f1 = re_ids_by_file[f1].difference(re_ids_by_file[f2])
            re_unique_f2 = re_ids_by_file[f2].difference(re_ids_by_file[f1])


            if re_overlap:
                re_overlaps[f"{f1} & {f2}"] = sorted(re_overlap)
            if glaux_overlap:
                glaux_overlaps[f"{f1} & {f2}"] = sorted(glaux_overlap)
            if re_unique_f1:
                re_unique_f1s[f"in {f1} but not in {f2}"] = sorted(re_unique_f1)
            if re_unique_f2:
                re_unique_f1s[f"in {f2} but not in {f1}"] = sorted(re_unique_f2)



    return {
        "issues": all_issues,
        "re_overlaps": re_overlaps,
        "glaux_overlaps": glaux_overlaps,
        "re_unique_f1": re_unique_f1s,
        "re_unique_f2": re_unique_f2s,
        "doubles": doubles
    }

def write_log(results: Dict[str, List[str]], log_file: str):
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write("Validation Issues:\n")
        for issue in results["issues"]:
            f.write(issue + "\n")

        f.write("\nRE_id Overlaps Between Files:\n")
        for pair, overlaps in results["re_overlaps"].items():
            f.write(f"{pair}: {overlaps}\n")

        f.write("\nGlaux_id Overlaps Between Files:\n")
        for pair, overlaps in results["glaux_overlaps"].items():
            f.write(f"{pair}: {overlaps}\n")
        f.write("\nduplicate Glaux_ids within files:\n")
        for pair, overlaps in results["doubles"].items():
            f.write(f"{pair}: {overlaps}\n")

def main():
    parser = argparse.ArgumentParser(description="Validate and compare multiple JSONL files.")
    # parser.add_argument("files", nargs="+", help="Path to .jsonl files to validate")
    parser.add_argument("dir", help="Path to .jsonl files to validate")
    parser.add_argument("--log", default="validation_log.txt", help="Path to output log file")
    args = parser.parse_args()
    import os
    files = [os.path.join(os.path.abspath(args.dir), file) for file in os.listdir(args.dir)]

    results = analyze_jsonl_files(files)
    with open(args.log, 'w', encoding='utf-8') as f:
        for file in files:
            subset = (os.path.basename(file))
            f.write(f'-----------{subset}------------')
            df = pd.read_json(file, lines=True)
            f.write(f'\nlength of the dataframe {len(df)}\n')
            f.write(f'\nsubset of gold vs silver: {df["data_status"].value_counts()}\n')
            f.write(f'\nnumber of unique entity {len(df["RE_id"].unique())}\n')
            f.write(f'\n{df["RE_id"].value_counts()}\n')
            silver = df[df['data_status'] == 'silver']
            gold = df[df['data_status'] == 'gold']
            f.write(f'\nnumber of unique entities silver {len(silver["RE_id"].unique())}\n')
            f.write(f'\n{df["RE_id"].value_counts()}\n')
            f.write(f'\nnumber of unique entities gold {len(gold["RE_id"].unique())}\n')
            f.write(f'\n{df["RE_id"].value_counts()}\n')


    write_log(results, args.log)
    print(f"Validation completed. Results saved to '{args.log}'.")

if __name__ == "__main__":
    main()