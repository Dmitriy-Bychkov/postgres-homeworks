import csv


def csv_reader(filename):
    """Читает файл csv"""

    with open(filename, newline='') as csv_file:
        reader = csv.DictReader(csv_file)
        total_list = []
        for row in reader:
            total_list.append(row)

    return total_list
