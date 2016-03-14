import sqlite3


class EdXDataIngester(object):
    """ Ingests edX TSV data into a SQLite3 database. """
    db = None

    def __init__(self, database_path):
        """
        Instantiates a new EdXDataIngester.

        Arguments:
            database_path (str) -- Path where the SQLite database should be created.
        """
        self.database_path = database_path
        self.db = sqlite3.connect(self.database_path)

    def ingest_table(self, table_name, data_path, metadata_path):
        """
        Creates a new table, and ingest the specified data.

        Arguments:
             table_name (str) -- Name of the table
             data_path (str) -- Path to the TSV file containing the data to be ingested
             metadata_path (str) -- Path to the table metadata (column definitions)
        """
        with self.db as conn:
            # Delete the table if it exists already
            conn.execute('DROP TABLE IF EXISTS {}'.format(table_name))

            # Get the column names from the metadata file
            columns = self.get_columns(metadata_path)

    def get_columns(self, metadata_path):
        """
        Returns column names and data types from the specified metadata file.

        Arguments:
             metadata_path (str) -- Path to the table metadata (column definitions)
        """
        pass
