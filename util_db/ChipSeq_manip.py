import sqlite3




def connect_db(db_name, logger):
    try:
        conn = sqlite3.connect(db_name + '.db')
        logger.info(f'Connetion stablished with DB: {db_name}.db')

        return conn


    except sqlite3.OperationalError:
        logger.error(f'Could not connect with {db_name}.db. Make sure the DB name is right')


def create_table(conn, logger):
    c = conn.cursor()

    try:
        c.execute('CREATE TABLE IF NOT EXISTS ChipSeq_template(cell_type_category TEXT NOT NULL,'
                  'cell_type TEXT NOT NULL,'
                  'cell_type_track_name TEXT NOT NULL,'
                  'cell_type_short TEXT NOT NULL,'
                  'assay_category TEXT NOT NULL,'
                  'assay TEXT NOT NULL,'
                  'assay_track_name TEXT NOT NULL,'
                  'assay_short TEXT NOT NULL,'
                  'donor TEXT NOT NULL,'
                  'time_point TEXT NOT NULL,'
                  'view TEXT NOT NULL,'
                  'track_name TEXT NOT NULL,'
                  'track_type TEXT NOT NULL,'
                  'track_density TEXT NOT NULL,'
                  'provider_institution TEXT NOT NULL,'
                  'source_server TEXT NOT NULL,'
                  'source_path_to_file TEXT NOT NULL,'
                  'server TEXT NOT NULL,'
                  'path_to_file TEXT NOT NULL,'
                  'new_file_name TEXT NOT NULL);')

        logger.info('Table ChipSeq_template was created')

    except sqlite3.OperationalError:
        logger.error('Table ChipSeq_template could not be created')


def insert_data(conn, list_of_data, logger):
    c = conn.cursor()

    try:
        with conn:

            for data in list_of_data:
                print(data)
                print()
                c.execute("INSERT INTO ChipSeq_template VALUES(:cell_type_category, "
                          ":cell_type, "
                          ":cell_type_track_name, "
                          ":cell_type_short, "
                          ":assay_category, "
                          ":assay, "
                          ":assay_track_name, "
                          ":assay_short, "
                          ":donor, "
                          ":time_point, "
                          ":view, "
                          ":track_name, "
                          ":track_type, "
                          ":track_density, "
                          ":provider_institution, "
                          ":source_server, "
                          ":source_path_to_file, "
                          ":server, "
                          ":path_to_file, "
                          ":new_file_name);", data)

                # c.execute("INSERT INTO test VALUES(:cell);",  {'cell': 'blood'})
            logger.info('Data was inserted on the DB')

    except sqlite3.OperationalError:
        logger.error('Data could not be inserted')



def select_column(conn, column, logger):

    c = conn.cursor()

    try:
        with conn:
            c.execute(f"SELECT DISTINCT {column} FROM ChipSeq_template")
            all_values = c.fetchall()

            logger.info(f'Selected {column}')
            return all_values

    except sqlite3.OperationalError:
        logger.error(f'Could not Select authors. Check if the table exists.')

def select_tracks(conn, assay, logger):
    c = conn.cursor()

    try:
        with conn:
            c.execute(f"SELECT track_name, track_type, track_density FROM ChipSeq_template WHERE assay = :assay", {"assay": assay})
            all_tracks = c.fetchall()

            logger.info(f'Selected tracks with assay')

            return all_tracks

    except sqlite3.OperationalError:
        logger.error(f'Could not Select track from assay. Check if the table exists')

def select_track_name(conn,assay_track_name, logger):
    c = conn.cursor()

    try:
        with conn:
            c.execute(f"SELECT track_name FROM ChipSeq_template WHERE assay_track_name = :assay_track_name", {"assay_track_name": assay_track_name})
            all_track_names = c.fetchall()

            logger.info(f'Selected tracks_name of assay_track_name')

            return all_track_names

    except sqlite3.OperationalError:
        logger.error(f'Could not Select track_name from assay_track_name. Check if the table exists')

def select_cell_type(conn, assay, logger):
    c = conn.cursor()

    try:
        with conn:
            c.execute(f"SELECT cell_types FROM ChipSeq_template WHERE assay = :assay", {"assay": assay})
            all_track_names = c.fetchall()

            logger.info(f'Selected cell_type from assay')

            return all_track_names

    except sqlite3.OperationalError:
        logger.error(f'Could not Select cell_type from assay. Check if the table exists')


def update_assay(conn, assay, up_assay, logger):

    c = conn.cursor()

    try:
        with conn:
            c.execute("UPDATE ChipSeq_template SET assay = :up_assay WHERE assay= :assay", {'assay': assay, 'up_assay': up_assay})
            logger.info(f'Assay:{assay} was updated to new assay: {up_assay}')

    except sqlite3.OperationalError:
        logger.error(f'COLD NOT UPDATE assay:{assay} for new assay: {up_assay}')


def update_donor(conn, donor, up_donor, logger):

    c = conn.cursor()

    try:
        with conn:
            c.execute("UPDATE ChipSeq_template SET donor = :up_donor WHERE donor= :donor", {'donor': donor, 'up_donor': up_donor})
            logger.info(f'Assay:{donor} was updated to new donor: {up_donor}')

    except sqlite3.OperationalError:
        logger.error(f'COLD NOT UPDATE donor:{donor} for new donor: {up_donor}')



def delete_track_name(conn, track_name, logger):
    c = conn.cursor()

    try:
        with conn:
            c.execute("DELETE FROM ChipSeq_template WHERE track_name = :track_name", {"track_name": track_name})

            logger.info(f'Rows where track_name is: "{track_name}",  were deleted')

    except sqlite3.OperationalError:
        logger.error(f'Could not delete {track_name}')