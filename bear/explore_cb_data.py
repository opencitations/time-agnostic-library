import argparse
import gzip
import os
import re

def search_string_in_nt_files(folder_path, search_string):
    versions = set()  # Utilizziamo un set per mantenere solo versioni uniche

    for root, _, files in os.walk(folder_path):
        for filename in files:
            if filename.endswith('.gz'):
                file_path = os.path.join(root, filename)
                try:
                    with gzip.open(file_path, 'rt', encoding='utf-8') as gz_file:
                        for line in gz_file:
                            if search_string in line:
                                match = re.search(r'(data-(?:added|deleted)_(\d+-\d+))\.nt\.gz', filename)
                                if match:
                                    versions.add(match.group(2))
                except Exception as e:
                    print(f"Errore durante la lettura del file {file_path}: {e}")
    
    # Stampiamo l'elenco delle versioni uniche
    print("Versioni uniche trovate:")
    for version in versions:
        print(version)

def main():
    parser = argparse.ArgumentParser(description="Cerca una stringa nei file .gz contenenti file .nt")
    parser.add_argument("folder_path", help="Percorso della cartella contenente i file .gz")
    parser.add_argument("search_string", help="Stringa da cercare nei file .nt")
    
    args = parser.parse_args()
    folder_path = args.folder_path
    search_string = args.search_string

    search_string_in_nt_files(folder_path, search_string)

if __name__ == "__main__":
    main()