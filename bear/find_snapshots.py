import argparse
import json
import os
import re

from tqdm import tqdm


def find_generated_at_time(filename):
    """
    Esplora il file specificato e restituisce tutti gli oggetti dei predicati prov:generatedAtTime.
    """
    results = set()
    date_pattern = re.compile(r'"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2})"')

    with open(filename, 'r', encoding='utf-8') as file:
        for line in file:
            if "<http://www.w3.org/ns/prov#generatedAtTime>" in line:
                matches = date_pattern.findall(line)
                results.update(matches)

    return results

def main(directory, output_file):
    """
    Esplora tutti i file .nq nella directory specificata e cerca gli oggetti dei predicati prov:generatedAtTime.
    """
    timestamps = set()

    # Calcola il numero totale di file .nq
    total_files = sum(1 for _, _, files in os.walk(directory) for filename in files if filename.endswith('.nq'))

    # Utilizza tqdm per creare una barra di progressione basata sul numero di file .nq
    with tqdm(total=total_files, desc="Processing files", unit="file") as pbar:
        for root, _, files in os.walk(directory):
            for filename in files:
                if filename.endswith('.nq'):
                    full_path = os.path.join(root, filename)
                    timestamps.update(find_generated_at_time(full_path))
                    pbar.update(1)

    timestamps_sorted = sorted(list(timestamps))

    with open(output_file, "w", encoding='utf-8') as json_file:
        json.dump({str(i+1): ts for i, ts in enumerate(timestamps_sorted)}, json_file, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Find prov:generatedAtTime objects in .nq files.")
    parser.add_argument("directory", type=str, help="The directory to explore for .nq files")
    parser.add_argument("--output", type=str, default="timestamps.json", help="Output file path (default: timestamps.json)")
    args = parser.parse_args()
    main(args.directory, args.output)
