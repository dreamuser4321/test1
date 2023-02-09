import pyodbc

class SqlConnect:
    def __init__(self, server_name, database_name, user_name, password
                 , driver_name='ODBC Driver 17 for SQL Server'):
        self.server_name = server_name
        self.database_name = database_name
        self.user_name = user_name
        self.password = password
        self.driver_name = driver_name
        #self.driver_name = "FreeTDS"

    def get_connected(self):
        """
        Instantiate pyodbc connection to SQL Server to be used for Extract
        :return:
        """
        conn_string = self.create_conn_string()
        conn = pyodbc.connect(conn_string)
        return conn

    def test_connection(self):
        """
        Test method for connection object - we make a simple query to validate the connection
        :return:
        """
        with self.get_connected() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT @@VERSION;')
            for row in cursor:
                print(row)

    def create_conn_string(self):
        """
        Build connection string - it's actually a string at this point
        :return: String
        """
        print("in create conn string: {}".format(self.user_name))
        conn_str = ('Driver={' + self.driver_name + '};' +
                    'Server=' + self.server_name + ';'
                    'Database=' + self.database_name + ';'
                    'Uid=' + self.user_name + ';'
                    'Pwd=' + self.password + ';'
                    'port=1433'
                    )
        return conn_str
