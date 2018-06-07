import argparse
import logging
import os
from util_db import ChipSeq_manip as db
import util.loggerinitializer as utl



# Initialize log object
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
utl.initialize_logger(os.getcwd(), logger)





def main():
    parser = argparse.ArgumentParser(description="A Tool manipulate the ChipSeq DB")

    subparsers = parser.add_subparsers(title='actions',
                                       description='valid actions',
                                       help='Use ChipSeq_DB.py {action} -h for help with each action',
                                       dest='command')

    parser_index = subparsers.add_parser('createdb', help='Create database and tables.'
                                                          'Usage: ChipSeq_DB.py createdb --db <DB_NAME>')

    parser_index.add_argument("--db",
                              dest='db',
                              default=None,
                              action="store",
                              help="The DB name",
                              required=True)

    parser_insert = subparsers.add_parser('insert', help='Insert data on tables.'
                                                         'Usage: ChipSeq_DB.py insert --file <FILE_NAME.CVS> --db <DB_NAME>')

    parser_insert.add_argument("--file",
                               default=None,
                               action="store",
                               help="CSV file with the data to be inserted",
                               required=True)

    parser_insert.add_argument("--db",
                               default=None,
                               action="store",
                               help="The DB name",
                               required=True)


    parser_select = subparsers.add_parser('select', help='Select  fields from the db. '
                                                         'Usage: ChipSeq_DB.py select {option} {option} --db <DB_NAME>')

    parser_select.add_argument("--db",
                               default=None,
                               action="store",
                               help="The DB name",
                               required=True)

    parser_select.add_argument("--header",
                               dest="header",
                               action="store_true",
                               help="Show columns header."
                                    "Usage: ChipSeq_DB.py select --header ",
                               required=False)

    parser_select.add_argument("-c, --column",
                               dest="column",
                               action="store",
                               help="Select all unique values of column. "
                                    "Usage: ChipSeq_DB.py select -c <COLUMN_NAME> --db <DB_NAME>",
                               required=False,
                               default=False)

    parser_select.add_argument("-a, --assay",
                               dest="assay",
                               action="store",
                               help="Assay's name",
                               required=False,
                               default=False)

    parser_select.add_argument("--tracks",
                               dest="tracks",
                               action="store_true",
                               help="Get all tracks from one specific assay. "
                                    "Usage: ChipSeq_DB.py select --tracks -a <ASSAY> --db <DB_NAME>",
                               required=False,
                               default=False)

    parser_select.add_argument("--cell_type",
                               dest="cell_type",
                               action="store_true",
                               help="Get all cell_types from one specific assay. "
                                    "Usage: ChipSeq_DB.py select --cell_type -a <ASSAY> --db <DB_NAME>",
                               required=False,
                               default=False)

    parser_select.add_argument("--atn",
                               #atn = assay track name
                               dest="atn",
                               action="store",
                               help="Get track_name from specific assay_track_name (atn)."
                                    "Usage: ChipSeq_DB.py select --atn <ASSAY_TRACK_NAME> --db <DB_NAME>",
                               required=False,
                               default=False)


    parser_update = subparsers.add_parser('update', help='Update a field in a db')

    parser_update.add_argument("--db",
                               default=None,
                               action="store",
                               help="The DB name",
                               required=True)

    parser_update.add_argument("-a",
                               dest="assay",
                               action="store",
                               help="Assay's name",
                               required=False,
                               default=False)

    parser_update.add_argument("--up_assay",
                               dest="up_assay",
                               action="store",
                               help="Update an assay. "
                                    "Usage: ChipSeq_DB.py update -a <ASSAY> --up_assay <NEW_ASSAY> --db <DB_NAME>",
                               required=False,
                               default=False)

    parser_update.add_argument("-d",
                               dest="donor",
                               action="store",
                               help="Donor's name",
                               required=False,
                               default=False)

    parser_update.add_argument("--up_donor",
                               action="store",
                               help="Update a donor. "
                                    "Usage: ChipSeq_DB.py update -d <DONOR> --up_donor <NEW_DONOR> --db <DB_NAME>",
                               required=False,
                               default=False)



    parser_delete = subparsers.add_parser('delete', help='delete rows from the db')

    parser_delete.add_argument("--db",
                               default=None,
                               action="store",
                               help="The DB name",
                               required=True)

    parser_delete.add_argument("--del",
                               dest="delete",
                               action="store_true",
                               help="Delete rows where a specific item appears",
                               required=False,
                               default=False)

    parser_delete.add_argument("--track_name",
                               dest="track_name",
                               action="store",
                               help="track_name name to be used as reference do delete row. "
                                    "Usage: ChipSeq_DB.py delete --del --track_name <TRACK_NAME> --db <DB_NAME>",
                               required=False,
                               default=False)


    args = parser.parse_args()

    conn = db.connect_db(args.db, logger)

    if args.command == "createdb":

        db.create_table(conn, logger)



    elif args.command == "insert":
        list_of_data = []

        with open(args.file, 'r') as f:
            for line in f:

                # reset dictionary
                line_dict = dict()

                # Skip empty lines
                if not line.strip():
                    continue

                if line.startswith(','):
                    continue

                # split line
                values = line.strip().split(',')

                # put each field in a dict
                line_dict["cell_type_category"] = values[0]
                line_dict["cell_type"] = values[1]
                line_dict["cell_type_track_name"] = values[2]
                line_dict["cell_type_short"] = values[3]
                line_dict["assay_category"] = values[4]
                line_dict["assay"] = values[5]
                line_dict["assay_track_name"] = values[6]
                line_dict["assay_short"] = values[7]
                line_dict["donor"] = values[8]
                line_dict["time_point"] = values[9]
                line_dict["view"] = values[10]
                line_dict["track_name"] = values[11]
                line_dict["track_type"] = values[12]
                line_dict["track_density"] = values[13]
                line_dict["provider_institution"] = values[14]
                line_dict["source_server"] = values[15]
                line_dict["source_path_to_file"] = values[16]
                line_dict["server"] = values[17]
                line_dict["path_to_file"] = values[18]
                line_dict["new_file_name"] = values[19]

                # append the dict to a list
                list_of_data.append(line_dict)



        db.insert_data(conn, list_of_data, logger)


    elif args.command == "select" and args.header is not False :
        print('cell_type_category\n'
              'cell_type\n'
              'cell_type_track_name\n'
              'cell_type_short\n'
              'assay_category\n'
              'assay \n'
              'assay_track_name\n'
              'assay_short \n'
              'donor \n'
              'time_point \n'
              'view \n'
              'track_name \n'
              'track_type \n'
              'track_density \n'
              'provider_institution \n'
              'source_server \n'
              'source_path_to_file \n'
              'server \n'
              'path_to_file \n'
              'new_file_name\n')


    elif args.command == "select" and args.column is not False:

        all_values = db.select_column(conn, args.column, logger)

        for value in all_values:
            print(value[0])


    elif args.command == "select" and args.assay is not False:

        if args.tracks is not False:

            all_tracks = db.select_tracks(conn, args.assay, logger)

            print("\n| track_name\t| track_type\t| track_density")
            for track in all_tracks:
                print("|", "\t| ".join(track))


        elif args.cell_type is not False:

            all_cell_types = db.select_cell_type(conn, args.assay, logger)

            print("\n| cell_type")
            for cell_type in all_cell_types:
                print("|", "\t| ".join(cell_type))

    elif args.command == "select" and args.atn is not False:

        all_track_name = select_track_name(conn, args.atn, logger)

        print("\n| track_name")
        for tn in all_track_name:
            print("|", "\t| ".join(tn))


    elif args.command == "update":

        if args.up_assay is not False:

            db.update_assay(conn, args.assay, args.up_assay, logger)


        elif args.up_donor is not False:

            db.update_donor(conn, args.donor, args.up_donor, logger)


    elif args.command == "delete":

        if args.delete is not False:
            db.delete_track_name(conn, args.track_name, logger)

if __name__ == '__main__':
    main()