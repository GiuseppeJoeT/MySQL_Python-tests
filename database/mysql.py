import MySQLdb as _mysql


class MySQLDatabase:
    def __init__(self, database_name, username, password, host='localhost'):
        # Class Constructor
        try:
            # Connection instance
            self.db = _mysql.connect(db=database_name,
                                     host=host,
                                     user=username,
                                     passwd=password)

            self.database_name = database_name

            print "Connected to MySQL!"

        except _mysql.Error, e:
            print e

    def __del__(self):
        # Class Destructor
        if hasattr(self, 'db'):  # close our connection to free it up in the pool
            self.db.close()

            print "MySQL Connection closed"

    # Query the tables that are available in your Database
    def get_available_tables(self):
        cursor = self.db.cursor()
        cursor.execute("SHOW TABLES;")

        tables = cursor.fetchall()

        cursor.close()

        return tables

    # After using our cursor to execute the SQL we then just call fetchall() to retrieve
    # the list of tables.

    def get_columns_for_table(self, table_name):

        cursor = self.db.cursor()
        cursor.execute("SHOW COLUMNS FROM %s " % table_name)

        columns = cursor.fetchall()

        cursor.close()

        return columns

    def select(self, table, columns=None, named_tuples=False, **kwargs):
        """
        select(table_name, [list of column names])
        --- columns=None --->
        ----  **kwargs is a list of parameters we might need to use
        """
        sql_str = "SELECT "

        # add columns or just the wildcard

        if not columns:
            sql_str += " * "
        else:
            for column in columns:
                sql_str += "%s, " % column

            sql_str = sql_str[:-2]  # remove the last comma!

        # add the table to select from
        sql_str += "FROM %s.%s " % (self.database_name, table)

        # there a JOIN clause attached
        if kwargs.has_key('join'):
            sql_str += " JOIN %s" % kwargs.get('join')

        # there a WHERE clause attached
        if kwargs.has_key('where'):
            sql_str += " WHERE %s" % kwargs.get('where')

        # there is an ORDER BY clause attached - Challenge
        if kwargs.has_key('order_by'):
            sql_str += " ORDER BY %s" % kwargs.get('order_by')

        # there a LIMIT clause attached - Challenge
        if kwargs.has_key('limit'):
            sql_str += " LIMIT %s" % kwargs.get('limit')

        sql_str += ";"  # finalise our sql string

        # SELECT * FROM employee02_db.employee WHERE ...

        cursor = self.db.cursor()

        cursor.execute(sql_str)

        if named_tuples:
            results = cursor.fetchall()
        else:
            results = cursor.fetchall()

        cursor.close()

        return results
