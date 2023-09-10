import os


def count_statements_in_file(filename):
    """
    Conta il numero di statements nel file specificato.
    """
    with open(filename, 'r', encoding='utf-8') as file:
        return sum(1 for _ in file)

def main(directory):
    """
    Esplora tutti i file .nq nella directory specificata e conta gli statements.
    """
    total_statements = 0

    for root, _, files in os.walk(directory):
        for filename in files:
            if filename.endswith('.nq'):
                full_path = os.path.join(root, filename)
                statements = count_statements_in_file(full_path)
                total_statements += statements
                print(f"Statements in {filename}: {statements}")

    print("\nTotal statements across all files:", total_statements)

if __name__ == "__main__":
    directory = input("Inserisci il percorso della cartella da esplorare: ")
    main(directory)