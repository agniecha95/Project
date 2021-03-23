import os
import time

import db_handler
from db_executer import Db_executer
from CustomLogger import logger


path_to_watch = '.'
before = dict([(f, None) for f in os.listdir(path_to_watch)])

while 1:
    db_name =r'E:\Moje\Python szkolenie\Python zaawansowany\Project\clinic.db'
    db = Db_executer(db_name)
    after = dict([(f, None) for f in os.listdir(path_to_watch)])

    added = []
    for name in after: #sprawdzamy czy pojawił sie nowy plik
        if not name in before: #porównanie
            added.append(name) #jeśli nie ma tej nazwy w before to dodaj

    removed = []
    for name in before:
        if not name in after:
            removed.append(name)

    if added: #jeśli lista nie jest pusta to wchodze do tego bloku
        logger.info(f"Added : {added}")
        for added_file in added: #dla każdego pliku z listy dodanych wykonaj:
            if 'patients' in added_file: #jeśli w nazwie jest słowo patients to :
                with open(added_file, 'r') as file: #otwórz ten plik
                    rows = file.readlines() #odczytaj wiersz po wierszu
                    for row in rows: #dla kazdego wiersza wykonaj:
                        row = row.split(',') #podziel string na liste
                        print(row)
                        db.insert_new_patient(row[0], row[1], row[2], row[3])
            if 'antarctica' in added_file:
                for name in added_file:
                    db.antarctica_analysis(name)
    else:
        logger.info("Nothing was added")

    if removed:
        logger.info(f'Removed {removed}')
    else:
        logger.info('Nothing was removed')

    before = after #before było przed while, musimy nadpisać jego wartość
    db.close_conn()
    time.sleep(10)











r'''db = db_handler.Database(r"E:\Moje\Python szkolenie\Python zaawansowany\W1_cwiczenia\Project\patients.db")
db.execute_sql(sql_create_table)
print("DB created")

sql_insert = """
    INSERT INTO patients(
    id, name, surname, pesel)
    VALUES (?,?,?,?)
"""

with open(r"E:\Moje\Python szkolenie\Python zaawansowany\W1_cwiczenia\Project\project-data.txt") as in_file:
    for line in in_file.readlines():
        patient_data = line.strip().split(sep=",")
        print(patient_data)
        db.execute_sql(sql_insert, patient_data)
'''