import os
import argparse

def find_highest_numbered_file(directory):
    highest_numbered_file = None
    highest_number = -1

    for filename in os.listdir(directory):
        if filename.endswith(".nt.gz"):
            try:
                file_number = int(filename.split('.')[0])
                if file_number > highest_number:
                    highest_numbered_file = filename
                    highest_number = file_number
            except ValueError:
                pass

    return highest_numbered_file

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Trova il file con il numero più alto nel nome nella directory.")
    parser.add_argument("directory", help="La directory in cui cercare il file con il numero più alto nel nome.")

    args = parser.parse_args()
    directory = args.directory

    if not os.path.exists(directory):
        print(f"La directory '{directory}' non esiste.")
    else:
        highest_numbered_file = find_highest_numbered_file(directory)
        if highest_numbered_file:
            print(f"Il file con il numero più alto nel nome nella directory '{directory}' è: {highest_numbered_file}")
        else:
            print(f"Nessun file trovato nella directory '{directory}' con numeri nel nome.")