import MySQLdb as _mysql
# To connect to we need to import the module and use it to create a connection instance

from database.mysql import MySQLDatabase

employees02_db = MySQLDatabase('employees02_db', 'root', '123root', 'localhost')

my_tables = employees02_db.get_available_tables()

print my_tables

my_select = employees02_db.select('employees')

print my_select


kwargs = { 'where': "employees.gender=M",
           'order_by': 'emp_no'}

results = my_database.select('people',
                             columns=["concat('first_name', '' , 'last_name') as full_name"],
                             named_tuples=True, **kwargs)

print kwargs


'''
db = _mysql.connect(db='employees02_db',
                    host='localhost',
                    user='root',
                    passwd='123root')


my_cursor = db.cursor()

my_cursor.execute("UPDATE employees SET first_name = 'Robbie', last_name = 'Keane' WHERE emp_no = 10001 ")
my_cursor.execute("SELECT * FROM employees WHERE emp_no = 10001")

db.commit()

results = my_cursor.fetchall()
# we could use: fetchall() - fectchmany() - fectchone()


my_cursor.close()

db.close()


print results
'''
