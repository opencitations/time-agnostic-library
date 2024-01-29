import argparse
import gzip
import os

def search_string_in_nt_files(folder_path, search_string):
    for root, _, files in os.walk(folder_path):
        for filename in files:
            if filename.endswith('.gz'):
                file_path = os.path.join(root, filename)
                try:
                    with gzip.open(file_path, 'rt', encoding='utf-8') as gz_file:
                        for line in gz_file:
                            if search_string in line:
                                print(line.strip(), filename)
                except Exception as e:
                    print(f"Errore durante la lettura del file {file_path}: {e}")

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
