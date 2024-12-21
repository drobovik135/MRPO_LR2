import csv
import psycopg2
import configparser
import pandas as pd


def insert_data(cursor, table_name, columns, data):
    query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
    cursor.execute(query, data)


def data_generate(conn):
    cur = conn.cursor()

    statuses = "procurementstatuses"
    providers = "providers"
    procurements = "souvenirprocurements"
    ps = "ProcurementSouvenirs"
    souvenir = "Souvenirs"
    stories = "SouvenirStores"

    cur.execute(f"INSERT INTO {statuses} (Name) VALUES ('cool') RETURNING ID")
    statusId = cur.fetchone()

    cur.execute(
        f"INSERT INTO {providers} (Name, Email, ContactPerson) VALUES ('Shop1', 'test@mail.ru', 'Dima') RETURNING ID")
    providerId = cur.fetchone()

    cur.execute(
        f"INSERT INTO {procurements} (IdProvider, Data, IdStatus) VALUES ('{providerId[0]}', '2023-02-05', '{statusId[0]}') RETURNING ID")
    procurementsId = cur.fetchone()

    cur.execute(f"SELECT ID FROM {souvenir} WHERE Id = '354'")
    souvenirId = cur.fetchone()
    cur.execute(
        f"INSERT INTO {ps} (IdSouvenir, IdProcurement, Amount, Price) VALUES ('{souvenirId[0]}', '{procurementsId[0]}', '50', '10') RETURNING ID")
    psId = cur.fetchone()

    cur.execute(
        f"INSERT INTO {stories} (IdProcurement, IdSouvenir, Amount) VALUES ('{procurementsId[0]}', '{souvenirId[0]}', '10') RETURNING ID")
    storiesId = cur.fetchone()

    conn.commit()


def execute_sql_file(connection, file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        sql_commands = file.read()

    with connection.cursor() as cursor:
        try:
            cursor.execute(sql_commands)
            connection.commit()
            print(f"Start of {file_path} so nice.")
        except Exception as e:
            connection.rollback()
            print(f"Failed start: {file_path} \nby error:\n {e}")


def create_conn(file):
    config = configparser.ConfigParser()
    config.read(file)

    return psycopg2.connect(
        dbname=config['database']['dbname'],
        user=config['database']['user'],
        password=config['database']['password'],
        host=config['database']['host'],
        port=config['database']['port']
    )


def add_value_to_dict_and_db(conn, table, value, dict_name):
    cur = conn.cursor()
    cur.execute(f"SELECT ID FROM {table} WHERE Name = '{value}'")
    row = cur.fetchone()
    if row is None:
        cur.execute(f"INSERT INTO {table} (Name) VALUES ('{value}') RETURNING ID")
        row = cur.fetchone()
        dict_name[value] = row[0]
    else:
        dict_name[value] = row[0]
    return dict_name[value]


def xlsx_reader(conn, file):
    df = pd.read_excel(file)

    colors = {}
    materials = {}
    application_methods = {}
    categories = {}

    for index, row in df.iterrows():
        colors[row['color']] = add_value_to_dict_and_db(conn, 'Colors', row['color'], colors)
        materials[row['material']] = add_value_to_dict_and_db(conn, 'SouvenirMaterials', row['material'], materials)
        application_methods[row['applicMetod']] = add_value_to_dict_and_db(conn, 'ApplicationMethods',
                                                                           row['applicMetod'], application_methods)

    cur = conn.cursor()
    for index, row in df.iterrows():
        try:
            cur.execute("""
                INSERT INTO Souvenirs (
                    ShortName,
                    Name,
                    Description,
                    Rating,
                    IdCategory,
                    IdColor,
                    Size,
                    IdMaterial,
                    Weight,
                    QTopics,
                    PicsSize,
                    IdApplicMethod,
                    AllCategories,
                    DealerPrice,
                    Price
                ) VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                ) RETURNING ID
            """, (
                row['shortname'],
                row['name'],
                row['description'],
                row['rating'],
                int(row['categoryid']),
                colors[row['color']],
                row['prodsize'],
                materials[row['material']],
                float(row['weight']) if pd.notna(row['weight']) else None,
                float(row['qtypics']) if pd.notna(row['qtypics']) else None,
                row['picssize'],
                application_methods[row['applicMetod']],
                False,
                float(row['dealerPrice']) if pd.notna(row['dealerPrice']) else None,
                float(row['price']) if pd.notna(row['price']) else None
            ))
        except psycopg2.errors.NumericValueOutOfRange as e:
            print(f"Ошибка NumericValueOutOfRange: {e}")
            print(f"Столбцы: {row}")
            print(
                f"Значения: {[float(x) if pd.notna(x) else None for x in [row['weight'], row['qtypics'], row['dealerPrice'], row['price']]]}")
        except Exception as e:
            print(f"Ошибка в заполнении данных : {e}")

    conn.commit()


def category_parse(conn, file):
    cursor = conn.cursor()
    with open(file, encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            row[1] = row[1] if row[1] else None
            insert_data(cursor, 'SouvenirCategories', ['ID', 'IdParent', 'Name'], row)
    conn.commit()


def data_reader(conn):
    category_parse(conn, "../data/categories.txt")
    xlsx_reader(conn, "../data/data.xlsx")


def init_table(conn):
    # execute_sql_file(conn, "sql/db_create.sql")
    execute_sql_file(conn, "sql/table_create.sql")
    execute_sql_file(conn, "sql/select.sql")


def main():
    conn = create_conn("config.ini")

    print("Инициализация таблицы")
    init_table(conn)

    print("Чтение данных")
    try:
        data_reader(conn)
    except Exception as e:
        print("Ошибка чтения данных")

    print("Генерация данных")
    try:
        data_generate(conn)
    except:
        print("Ошибка генерации данных")

    conn.close()


if __name__ == "__main__":
    main()
