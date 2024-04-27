import os
import random
import sqlite3
from collections import defaultdict
from typing import List

class Employee:
    """Class representing an employee."""

    def __init__(self, name, factory, departmentName, id=-1, IMES="", WorkerCodIMES=""):
        """Initialize an employee object."""
        self.id = id
        self.name = name
        self.factory = factory
        self.departmentName = departmentName
        self.IMES = IMES
        self.WorkerCodIMES = WorkerCodIMES

    def add_imes(self, imes: str):
        """Add IMES to the employee."""
        self.imes = imes

    def add_worker_cod_imes(self, worker_cod_imes: str):
        """Add WorkerCodIMES to the employee."""
        self.worker_cod_imes = worker_cod_imes

class EmployeeRecords:
    """Class for managing employee records in a database."""

    def __init__(self, database_path):
        """Initialize EmployeeRecords object."""
        self.database_path = database_path
        self.create_table_if_not_exists()

    def create_table_if_not_exists(self):
        """Create 'Employees' table if it does not exist."""
        with sqlite3.connect(self.database_path) as connection:
            cursor = connection.cursor()
            cursor.execute("CREATE TABLE IF NOT EXISTS Employees (id INTEGER PRIMARY KEY, name TEXT NOT NULL, factory TEXT NOT NULL, departmentName TEXT NOT NULL)")

    def add_employee(self, employee):
        """Add an employee to the database."""
        with sqlite3.connect(self.database_path) as connection:
            cursor = connection.cursor()
            cursor.execute("INSERT INTO Employees (name, factory, departmentName) VALUES (?, ?, ?)",
                           (employee.name, employee.factory, employee.departmentName))
            connection.commit()

def delete_database_if_exists(path):
    """Delete database file if it exists."""
    if os.path.exists(path):
        os.remove(path)

class DatabaseManager:
    """Class for managing databases."""

    def __init__(self, db_paths):
        """Initialize DatabaseManager object."""
        self.db_paths = db_paths

    def merge_databases(self, new_db_path):
        """Merge multiple databases into a new database."""
        with sqlite3.connect(new_db_path) as new_connection:
            new_cursor = new_connection.cursor()
            new_cursor.execute("CREATE TABLE IF NOT EXISTS Employees (id INTEGER , name TEXT NOT NULL, factory TEXT NOT NULL, departmentName TEXT NOT NULL)")
            for db_path in self.db_paths:
                with sqlite3.connect(db_path) as old_connection:
                    old_cursor = old_connection.cursor()
                    old_cursor.execute("SELECT id, name, factory, departmentName FROM Employees")
                    employees_data = old_cursor.fetchall()
                    new_cursor.executemany("INSERT INTO Employees (id, name, factory, departmentName) VALUES (?, ?, ?, ?)", employees_data)

def generate_random_data():
    """Generate random data for employee."""
    factory_names = ["Gates USA", "Gates Canada", "Gates UK", "Gates Germany", "Gates France", "Gates Australia", "Gates Poland", "Gates Spain", "Gates Italy", "Gates Japan"]
    department_names = ["Production Department", "Research and Development Department", "Marketing Department", "Sales Department", "Human Resources Department", "Finance Department", "Information Technology Department", "Customer Service Department", "Quality Assurance Department", "Supply Chain Department"]
    names = ["John", "Mary", "James", "Linda", "Robert", "Patricia", "Michael", "Jennifer", "William", "Elizabeth", "David", "Barbara", "Joseph", "Jessica", "Richard", "Sarah", "Thomas", "Karen", "Charles", "Nancy", "Daniel", "Margaret", "Matthew", "Lisa", "Anthony", "Betty", "Donald", "Dorothy", "Mark", "Sandra", "Paul", "Ashley", "Steven", "Kimberly", "Andrew", "Donna", "Kenneth", "Emily", "George", "Carol", "Joshua", "Michelle", "Kevin", "Amanda", "Brian", "Melissa", "Edward", "Deborah", "Ronald", "Stephanie"]
    surnames = ["Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", "Wilson", "Moore", "Taylor", "Anderson", "Thomas", "Jackson", "White", "Harris", "Martin", "Thompson", "Garcia", "Martinez", "Robinson", "Clark", "Rodriguez", "Lewis", "Lee", "Walker", "Hall", "Allen", "Young", "Hernandez", "King", "Wright", "Lopez", "Hill", "Scott", "Green", "Adams", "Baker", "Gonzalez", "Nelson", "Carter", "Mitchell", "Perez", "Roberts", "Turner", "Phillips", "Campbell", "Parker", "Evans", "Edwards", "Collins", "Stewart", "Sanchez", "Morris", "Rogers", "Reed", "Cook", "Morgan", "Bell", "Murphy", "Bailey", "Rivera", "Cooper", "Richardson", "Cox", "Howard", "Ward", "Torres", "Peterson", "Gray", "Ramirez", "James", "Watson", "Brooks", "Kelly", "Sanders", "Price", "Bennett", "Wood", "Barnes", "Ross", "Henderson", "Coleman", "Jenkins", "Perry", "Powell", "Long", "Patterson", "Hughes", "Flores", "Washington", "Butler", "Simmons", "Foster", "Gonzales", "Bryant", "Alexander", "Russell", "Griffin", "Diaz", "Hayes"]
    name = random.choice(names) + " " + random.choice(surnames)
    factory = random.choice(factory_names)
    department = random.choice(department_names)
    return Employee(name, factory, department)

class ProcessingData:
    """Class for processing employee data."""

    def __init__(self, connection_string: str):
        """Initialize ProcessingData object."""
        self.employees = []  # List to store employees
        self.read_employees_from_database(connection_string)

    def read_employees_from_database(self, connection_string: str):
        """Read employees from database."""
        with sqlite3.connect(connection_string) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT id, name, factory, departmentName FROM Employees")
            rows = cursor.fetchall()
            for row in rows:
                id, name, factory, departmentName = row
                self.add_employee(name, factory, departmentName, id)

    def add_employee(self, name: str, factory: str, department_name: str, id: int):
        """Add an employee to the list."""
        employee = Employee(name, factory, department_name, id)
        self.employees.append(employee)

    def print_employees_table(self):
        """Print the employees table."""
        print("{0:<5} {1:<20} {2:<20} {3:<20} {4:<20} {5:<20}".format("ID", "Name", "Factory", "Department", "IMES", "WorkerCodIMES"))
        print('-' * 100)
        for employee in self.employees:
            print("{0:<5} {1:<20} {2:<20} {3:<20} {4:<20} {5:<20}".format(employee.id, employee.name, employee.factory, employee.department_name, employee.imes, employee.worker_cod_imes))

    def generate_uniq_im_es(self):
        """Generate unique IMES for employees."""
        generated_im_es = set()
        for employee in self.employees:
            chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
            im_es = ''.join(random.choice(chars) for _ in range(20))
            while im_es in generated_im_es:
                im_es = ''.join(random.choice(chars) for _ in range(20))
            generated_im_es.add(im_es)
            employee.add_imes(im_es)

    def generate_worker_cod_im_es(self):
        """Generate WorkerCodIMES for employees."""
        id_counter = defaultdict(int)
        for employee in self.employees:
            key_suffix = self.int_to_base26(id_counter[employee.id])
            employee.add_worker_cod_imes(str(employee.id) + key_suffix)
            id_counter[employee.id] += 1

    @staticmethod
    def int_to_base26(value: int) -> str:
        """Convert an integer to base-26."""
        chars = "abcdefghijklmnopqrstuvwxyz"
        result = ""
        while True:
            result = chars[value % 26] + result
            value //= 26
            if value == 0:
                break
            value -= 1  # Decrease value by 1 for indexing: "a" = 0, "b" = 1, ..., "z" = 25
        return result

    def get_employees(self) -> List['Employee']:
        """Get the list of employees."""
        return self.employees

    def save_employees_to_database(self, db_name: str):
        """Save employees to a database."""
        if os.path.exists(db_name):
            os.remove(db_name)
            print(f"{db_name} deleted successfully")
        with sqlite3.connect(db_name) as connection:
            cursor = connection.cursor()
            cursor.execute('''CREATE TABLE IF NOT EXISTS Employees (
                                id INTEGER ,
                                name TEXT,
                                factory TEXT,
                                departmentName TEXT,
                                IMES TEXT,
                                WorkerCodIMES TEXT
                            )''')
            for employee in self.employees:
                cursor.execute(
                    "INSERT INTO Employees (id, name, factory, departmentName, IMES, WorkerCodIMES) VALUES (?, ?, ?, ?, ?, ?)",
                    (employee.id, employee.name, employee.factory, employee.departmentName, employee.imes,
                     employee.worker_cod_imes))
            connection.commit()


class EmulationIMES:
    """Class for emulating IMES."""

    def __init__(self, connection_string):
        """Initialize EmulationIMES object."""
        self._connection_string = connection_string

    def login(self, worker_cod_imes):
        """Simulate login using WorkerCodIMES."""
        with sqlite3.connect(self._connection_string) as connection:
            cursor = connection.cursor()
            query = f"SELECT * FROM Employees WHERE WorkerCodIMES = '{worker_cod_imes}'"
            cursor.execute(query)
            row = cursor.fetchone()
            if row:
                id, name, factory, department_name, IMES, worker_cod_imes_result = row
                print(f"ID: {id}")
                print(f"Name: {name}")
                print(f"Factory: {factory}")
                print(f"Department Name: {department_name}")
                print(f"IMES: {IMES}")
                print(f"WorkerCodIMES: {worker_cod_imes_result}")
            else:
                print("User not found")

def GenerateData():
    """Generate employee data."""
    for factory_num in range(1, 4):
        db_path = f"WorkersFactory{factory_num}.db"
        delete_database_if_exists(db_path)
        employee_records = EmployeeRecords(db_path)
        for _ in range(random.randint(500, 20000)):
            employee = generate_random_data()
            employee_records.add_employee(employee)
        print(f"{factory_num} file created successfully")

    database_paths = [f"WorkersFactory{factory_num}.db" for factory_num in range(1, 4)]
    merged_database_path = "merged_employees.db"
    delete_database_if_exists(merged_database_path)
    db_manager = DatabaseManager(database_paths)
    db_manager.merge_databases(merged_database_path)
    print("File merged successfully")

    delete_database_if_exists("Finish")
    pd = ProcessingData("merged_employees.db")
    pd.generate_uniq_im_es()
    print("IMES generated successfully")
    pd.generate_worker_cod_im_es()
    print("Worker Cod IMES generated successfully")
    pd.save_employees_to_database("Finish.db")
    print("Data saved successfully")

def main():
    """Main function."""
    operation = input("Choose operation \n1.generate data, \n2.Employee search (after data generation):\n ")

    if operation == "1":
        GenerateData()

    if not os.path.exists("Finish.db"):
        print("Data has not been generated, or generation is incomplete")
        print("Start generation")
        GenerateData()


    emulationIMES = EmulationIMES("Finish.db")
    print("Enter your code")
    code = input()
    emulationIMES.login(code)

    print("Press enter to exit the program")
    input()

main()
