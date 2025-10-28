import os
from pathlib import Path
import argparse

def concatenate_matching_files(dir1, dir2, output_dir):
    dir1 = Path(dir1)
    dir2 = Path(dir2)
    output_dir = Path(output_dir)

    # Create the output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)

    for root, _, files in os.walk(dir1):
        for file in files:
            relative_path = Path(root).relative_to(dir1) / file
            file1 = dir1 / relative_path
            file2 = dir2 / relative_path
            output_file = output_dir / relative_path

            if file2.exists():
                # Ensure output subdirectory exists
                output_file.parent.mkdir(parents=True, exist_ok=True)

                with open(file1, 'r', encoding='utf-8') as f1, \
                     open(file2, 'r', encoding='utf-8') as f2, \
                     open(output_file, 'w', encoding='utf-8') as out:
                    out.write(f1.read())
                    out.write(f2.read())
                print(f"Concatenated: {relative_path}")
            else:
                print(f"Skipped (no match in dir2): {relative_path}")

def main():
    parser = argparse.ArgumentParser(
        description="Concatenate matching files from two directories and save them in a new directory."
    )
    parser.add_argument("dir1", help="Path to the first input directory")
    parser.add_argument("dir2", help="Path to the second input directory")
    parser.add_argument("output_dir", help="Path to the output directory (will be created if it doesn't exist)")

    args = parser.parse_args()
    concatenate_matching_files(args.dir1, args.dir2, args.output_dir)

if __name__ == "__main__":
    main()