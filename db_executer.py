from db_handler import Database
from CustomLogger import logger
import csv
from datetime import datetime
from send_sms import send_sms


class Db_executer(Database):
    def __init__(self, db_name):
        super().__init__(db_name)

    def create_analysis_table(self):
        sql_create_analysis = """
            CREATE TABLE IF NOT EXISTS analysis(
                probe_number integer PRIMARY KEY,
                analysis_id integer, 
                patient_id integer,
                collection time timestamp without time zone,
                result text) ;
        """
        try:
            super().__init__(self.db_name)
            super().execute_sql(sql_create_analysis)
        except Exception as e:
            logger.error(f"Can not create table analysis {e}")

    def insert_new_patient(self, id, name, surname, pesel):
        sql = """
            INSERT INTO patients
            (id, name, surname, pesel)
            VALUES (?,?,?,?) 
        """
        patient_data = [id, name, surname, pesel] #ustawi parametry w odpowiedniej kolejności
        try:
            super().__init__(self.db_name)
            super().execute_sql(sql, patient_data)
        except Exception as e:
            logger.error(f"Can not insert patient data {e}")
            exit()

    def insert_analysis(self, probe_number, analysis_id, patient_id, collection_time, result):
        sql = """
            INSERT INTO analysis
            (probe_number, analysis_id, patient_id, collection_time, result)
            VALUES (?,?,?,?,?)    
        """

        analysis_data = [probe_number, analysis_id, patient_id, collection_time, result]
        try:
            super().__init__(self.db_name)
            super().execute_sql(sql, analysis_data)
        except Exception as e:
            logger.error(f"Can not insert new analysis probe_number {probe_number} {e}")
            exit()

    def diagnostica_analysis(self):
        try:
            with open(r"E:\Moje\Python szkolenie\Python zaawansowany\W1_cwiczenia\Project\Diagnostica.csv",
                      encoding="UTF8") as csv_file:
                all_row = csv.reader(csv_file, delimiter=',')
                for row in all_row:
                    # print(row)
                    if row[1] == 'blood':
                        row[1] = '1'
                    if row[3] != 'collection_time':
                        row[3] = str(datetime.strptime(row[3], "%d.%m.%Y %H:%M"))
                    if row[0] != "external_patient_id":  # opuszczamy 1 wiersz
                        self.insert_analysis(probe_number=row[2], analysis_id=row[1], patient_id=row[0], collection_time=row[3],
                                           result=row[4])
        except Exception as e:
            logger.error("Diagnostica analysis failed", exc_info=True)

    def hospital_analysis(self, csv_hospital_path):
        try:
            with open(csv_hospital_path,encoding="UTF8") as csv_hospital:
                all_row = csv.reader(csv_hospital, delimiter=',')
                for row in all_row:
                    if row[0] != "external_patient_id":
                        row[2] = str(datetime.strptime(row[2], "%d.%m.%Y %H:%M"))
                        if row[4] == 'blood':
                            row[4] = '1'
                        if row[3] == 'T':
                            row[3] = 'True'
                        else:
                            row[3] = 'False'
                        self.insert_analysis(probe_number=row[1], analysis_id=row[4], patient_id=row[0],
                                           collection_time=row[2],
                                           result=row[3])
        except Exception as e:
            logger.error("Hospital analysis failed", exc_info=True)

    def antarctica_analysis(self, csv_antarctica_path):
        try:
            with open(csv_antarctica_path, encoding="UTF-8-SIG") as csv_antarctica:
                all_row = csv.reader(csv_antarctica, delimiter=';')
                all_row_db = self.select_all_tasks("patients")

                week_days = ["Poniedzialek", "Wtorek", "Środa", "Czwartek", "Piątek", "Sobota", "Niedziela"]

                for row in all_row:
                    for row_db in all_row_db:
                        if row[2] == str(row_db[3]):
                            if row[4] != 'collection_time':
                                row[4] = str(datetime.strptime(row[4], "%d.%m.%Y %H:%M"))
                            if row[5] == 'T':
                                row[5] = 'True'
                            if row[5] == 'F':
                                row[5] = 'False'
                            if row[0] != "id":  # opuszczamy 1 wiersz
                                self.insert_analysis(probe_number=row[1], analysis_id=row[3], patient_id=row_db[0],
                                                   collection_time=row[4],
                                                   result=row[5])
                            if row[5] == 'False':
                                obj_date = datetime.strptime(row[6], "%d.%m.%Y %H:%M")
                                week_num = obj_date.weekday()
                                send_sms(
                                    f'Zapraszamy na wizyte kontrolna w {week_days[week_num]} dokładna data {obj_date.date()} '
                                    f'o godzinie {obj_date.time()}', row[7])
        except Exception as e:
            logger.error("Antarctica analysis failed", exc_info=True)
