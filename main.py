import decimal
import json
import os
import threading
from queue import Queue
import mysql.connector
from mysql.connector import pooling
import tkinter as tk
from tkinter import messagebox, filedialog
import configparser
import sys
import logging
from datetime import datetime, date, time
import requests
import zipfile
import io

logging.basicConfig(filename='exe.log', filemode='w', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Set up error logger
error_logger = logging.getLogger('error_logger')
error_handler = logging.FileHandler('err.log', mode='w')
error_handler.setLevel(logging.ERROR)
error_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
error_logger.addHandler(error_handler)
error_logger.propagate = False

# Set up Null and Empty Logger
null_logger = logging.getLogger('null_logger')
null_handler = logging.FileHandler('null.log', mode='w')
null_handler.setLevel(logging.INFO)
null_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
null_logger.addHandler(null_handler)
null_logger.propagate = False

CONFIG_FILE = "config.ini"
previous_data = None
connection_pool = None
# call_count = 0


############################################################
# Git hub Info:
# GitHub Info
GITHUB_VERSION_FILE_URL = "https://github.com/jpelt/CackyMaps/blob/TBI_COLOR/version.txt"
GITHUB_EXE_DOWNLOAD_URL = "https://github.com/jpelt/CackyMaps/releases/download/v1.0/main.exe"  # Example URL

# Internal Version Number
CURRENT_VERSION = "v1.1"  # Replace with your current version

def get_latest_version():
    try:
        response = requests.get(GITHUB_VERSION_FILE_URL)
        if response.status_code == 200:
            return response.text.strip()
        else:
            raise Exception(f"Failed to fetch version file: {response.status_code}")
    except Exception as e:
        print(f"Error fetching latest version: {e}")
        return None

def download_latest_exe():
    try:
        response = requests.get(GITHUB_EXE_DOWNLOAD_URL, stream=True)
        if response.status_code == 200:
            with open("app.exe", "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
            print(f"Downloaded latest version of app.exe.")
            return True
        else:
            raise Exception(f"Failed to download EXE: {response.status_code}")
    except Exception as e:
        print(f"Error downloading EXE: {e}")
        return False

def check_for_updates():
    latest_version = get_latest_version()

    if latest_version:
        if latest_version != CURRENT_VERSION:
            root = tk.Tk()
            root.withdraw()
            update_prompt = messagebox.askokcancel(
                "Update Available",
                f"A new version ({latest_version}) is available. Would you like to update?"
            )
            root.destroy()

            if update_prompt:
                if download_latest_exe():
                    messagebox.showinfo("Update Complete", "The application has been updated to the latest version.")
                    sys.exit("Application needs to restart to apply updates.")
                else:
                    messagebox.showerror("Update Failed", "Failed to download the latest version.")
        else:
            print("No updates available.")
    else:
        print("Unable to check for updates.")


################################################################################

def init_connection_pool(connection_details):
    global connection_pool
    try:

        connection_pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="mypool",
            pool_size=5,
            pool_reset_session=True,
            charset='utf8',
            **connection_details
        )
        logging.info("Initialized database connection pool")
    except mysql.connector.Error as error:
        error_logger.error(f"Error initializing connection pool: {error}")
        print(f"Error initializing connection pool: {error}")


def show_error_popup(message):
    root = tk.Tk()
    root.withdraw()
    messagebox.showerror("Error", message)
    root.destroy()


def show_json_popup(data):
    root = tk.Tk()
    root.title("JSON Data")

    text = tk.Text(root, wrap="word")
    text.insert(tk.END, json.dumps(data, indent=4))
    text.pack(expand=True, fill='both')

    scrollbar = tk.Scrollbar(root, command=text.yview)
    text.configure(yscrollcommand=scrollbar.set)
    scrollbar.pack(side='right', fill='y')

    root.mainloop()


def connect_to_database():
    try:
        connection = connection_pool.get_connection()
        cursor = connection.cursor(dictionary=True)
        logging.info("Connected to the database")
        print("Connected to the database")
        return connection, cursor
    except mysql.connector.Error as error:
        error_logger.error(f"Error connecting to the database: {error}")
        print(f"Error connecting to the database: {error}")
        return None, None


def release_connection(connection, cursor):
    try:
        cursor.close()
        connection.close()
    except mysql.connector.Error as error:
        error_logger.error(f"Error releasing the connection: {error}")


def get_connection_details(default_connection=None):
    logging.info("Opening connection dialog box")
    root = tk.Tk()
    root.title("Connection Details")
    root.geometry("300x200+100+100")

    tk.Label(root, text="Host:").grid(row=0, column=0)
    tk.Label(root, text="Database:").grid(row=1, column=0)
    tk.Label(root, text="User:").grid(row=2, column=0)
    tk.Label(root, text="Password:").grid(row=3, column=0)

    host_entry = tk.Entry(root)
    database_entry = tk.Entry(root)
    user_entry = tk.Entry(root)
    password_entry = tk.Entry(root, show="*")

    host_entry.grid(row=0, column=1)
    database_entry.grid(row=1, column=1)
    user_entry.grid(row=2, column=1)
    password_entry.grid(row=3, column=1)

    save_default_var = tk.IntVar()
    save_default_checkbox = tk.Checkbutton(root, text="Save as default", variable=save_default_var)
    save_default_checkbox.grid(row=4, columnspan=2)

    if default_connection:
        host_entry.insert(0, default_connection.get('host', ''))
        database_entry.insert(0, default_connection.get('database', ''))
        user_entry.insert(0, default_connection.get('user', ''))
        password_entry.insert(0, default_connection.get('password', ''))

    def retrieve_details():
        global connection_details
        connection_details = {
            'host': host_entry.get(),
            'database': database_entry.get(),
            'user': user_entry.get(),
            'password': password_entry.get(),
        }

        if save_default_var.get() == 1:
            save_default_connection(connection_details)

        if connection_pool is None:
            init_connection_pool(connection_details)

        try:
            connection, cursor = connect_to_database()

            if connection:
                label = tk.Label(root, text="")
                label.grid(row=5, columnspan=2)
                label.config(text="Connected to Database")
                ok_button.config(text="Close", command=lambda: close_app(root), bg="red")
                root.destroy()
                logging.info("Closing connection window")

            else:
                label = tk.Label(root, text="")
                label.grid(row=5, columnspan=2)
                label.config(text="Error connecting to Database, please check credentials")
                error_logger.error("Error connecting to Database, please check credentials")

        except mysql.connector.Error as e:
            error_logger.error(f"Error querying get_connection_details: {e}")

        finally:
            release_connection(connection, cursor)

    ok_button = tk.Button(root, text="Connect", command=retrieve_details)
    ok_button.grid(row=6, columnspan=2, pady=10)

    root.mainloop()


def create_default_ini(filename='config.ini'):
    if not os.path.exists(filename):
        config = configparser.ConfigParser()
        config['mysql'] = {
            'host': '',
            'database': '',
            'user': '',
            'password': ''
        }
        config['json_file_path'] = {
            'json_file_path': ''
        }

        with open(filename, 'w') as configfile:
            config.write(configfile)

        print(f"Created default config file {filename}. Please update it with your database credentials")
        logging.info(f"Created default config file {filename}. Please update it with your database credentials")


def save_default_connection(connection_details):
    try:
        logging.info("Saving connection information as Default")
        config = configparser.ConfigParser()

        config.read(CONFIG_FILE)

        if 'mysql' not in config:
            config['mysql'] = {}

        config['mysql'].update(connection_details)

        with open(CONFIG_FILE, 'w') as configfile:
            config.write(configfile)
            logging.info("Default connection saved to configuration file")
    except Exception as e:
        error_logger.error(f"Error saving to default configuration file: {e}")
        show_error_popup(f"Error saving to default configuration file: {e}")


def read_default_connection():
    connection_details = {}
    json_file_path = None
    try:
        logging.info("Starting to read the default connection file.")
        config = configparser.ConfigParser()
        config.read(CONFIG_FILE)

        if 'mysql' in config:
            connection_details = dict(config['mysql'])

            if not all(connection_details.values()):
                error_logger.error("MySQL details are missing. Opening connection dialog box...")
                connection_details = None

            else:
                logging.info("Successfully read default connection details.")
        else:
            error_logger.error("mysql section not found in the configuration file.")
            connection_details = None

        if 'json_file_path' in config:
            json_file_path = config['json_file_path'].get('json_file_path', '')
            if not json_file_path:
                error_logger.error("JSON file path is empty in the configuration file. Opening file dialog box...")
            else:
                logging.info("Successfully read JSON file path.")
        else:
            error_logger.error("JSON section not found in the configuration file.")

        logging.info("Completed reading the default connection file.")
    except Exception as e:
        error_logger.error(f"Error reading default configuration file: {e}")
        show_error_popup(f"Error reading default configuration file: {e}")

    return connection_details, json_file_path


def close_app(root):
    def close():
        logging.info("Closing application")
        root.quit()
        root.destroy()
        sys.exit()

    root.after(0, close)


def default_converter(obj):
    if isinstance(obj, (datetime, date, time)):
        return obj.isoformat()
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def convert_mysql_types(data):
    if isinstance(data, dict):
        converted_data = {}
        for key, value in data.items():
            if value is None:
                converted_data[key] = None
            elif isinstance(value, (datetime, date, time)):
                converted_data[key] = value.isoformat()
            elif isinstance(value, decimal.Decimal):
                converted_data[key] = float(value)
                # logging.info(f"Converted {key} from Decimal to float: {converted_data[key]}")
            else:
                converted_data[key] = value
            # logging.info(f"Key: {key}, Type: {type(value)}, Value: {value}")
        return converted_data
    elif isinstance(data, list):
        return [convert_mysql_types(element) for element in data]
    else:
        return data


def process_json_file(data, field_values):
    # global call_count
    # call_count += 1
    matched_fields = []
    logging.info("Scanning JSON file for field names")
    # Ensure the JSON data is a dictionary with a list of features
    if isinstance(data, dict) and "features" in data:
        for item in data["features"]:
            if 'properties' in item:
                cur_co_field_no = item['properties'].get('field')
                if cur_co_field_no:
                    cur_co_field_no_lower = cur_co_field_no.strip().lower()
                    # null_logger.info(f"field: {cur_co_field_no}")
                    if cur_co_field_no_lower in field_values:
                        matched_fields.append(cur_co_field_no_lower)
                        # null_logger.info(f"Match found: field: {cur_co_field_no}")

        return matched_fields


def run_query_on_matched_fields(matched_fields):
    if not isinstance(matched_fields, list) or not matched_fields:  # Ensure matched_fields is a list
        return []

    connection, cursor = connect_to_database()
    if connection:
        converted_results = []  # Initialize an empty list to store converted results
        try:
            # Create a SQL query with the matched CUR_CO_FIELD_NO values
            logging.info("Trying to query the mapexport table with matching fields")
            placeholders = ', '.join(['%s'] * len(matched_fields))
            query = f"SELECT * FROM mapexport WHERE field IN ({placeholders})"
            cursor.execute(query, matched_fields)
            results = cursor.fetchall()

            # Convert each row using convert_mysql_types and store in converted_results
            for row in results:
                converted_row = convert_mysql_types(row)
                # null_logger.info(f"Converted Result: {converted_row}")
                converted_results.append(converted_row)

        except mysql.connector.Error as e:
            error_logger.error(f"Error querying mapexport database: {e}")
        finally:
            release_connection(connection, cursor)

        return converted_results  # Return the list of converted results
    else:
        return []


def search_json_file_path():
    # Create a hidden root window to host the file dialog
    root = tk.Tk()
    root.withdraw()  # Hide the root window

    # Open the file dialog to select a JSON file
    file_path = filedialog.askopenfilename(
        title="Select JSON File",
        filetypes=[("JSON Files", "*.json"), ("All Files", "*.*")]
    )

    # Check if a file was selected
    if file_path:
        logging.info(f"Selected JSON file: {file_path}")
        print(f"Selected JSON file: {file_path}")

        # Save the selected file path to the config.ini file
        save_json_file_path_to_config(file_path)

        global json_file_path
        config = configparser.ConfigParser()
        config.read(CONFIG_FILE)
        json_file_path = config['json_file_path'].get('json_file_path', '')

    else:
        logging.info("No file selected")
        print("No file selected")

    # Destroy the root window after selection
    root.destroy()

    return file_path


def save_json_file_path_to_config(file_path):
    # Initialize the ConfigParser
    config = configparser.ConfigParser()

    # Read the existing config file
    config.read(CONFIG_FILE)

    # Ensure the section exists
    if 'json_file_path' not in config:
        config['json_file_path'] = {}

    # Update the json_file_path in the config file
    config['json_file_path']['json_file_path'] = file_path

    # Write the changes back to the config file
    with open(CONFIG_FILE, 'w') as configfile:
        config.write(configfile)

    logging.info(f"Saved JSON file path to {CONFIG_FILE}")
    # print(f"Saved JSON file path to {CONFIG_FILE}")


def fetch_results_in_map_export():
    map_export_results = []
    primary_keys = set()
    connection, cursor = connect_to_database()
    if connection:
        try:
            logging.info("Trying to query the mapexport table for field names")

            field_query = "SELECT field FROM mapexport"
            cursor.execute(field_query)
            fields = cursor.fetchall()
            field_values = [field['field'].strip().lower() for field in fields]
            return field_values

        except mysql.connector.Error as e:
            error_logger.error(f"Error querying mapexport database: {e}")

        finally:
            release_connection(connection, cursor)
    else:
        error_logger.error("Error connecting to database in fetch_results_in_map_export")

    return map_export_results, primary_keys


def map_export_json_data(json_file_path):
    if not json_file_path:
        error_logger.error("No JSON file path provided. Exiting function")
        return False

    try:
        # Fetch field values from the database
        field_values = fetch_results_in_map_export()
        if field_values:
            with open(json_file_path, "r") as json_file:
                try:
                    data = json.load(json_file)
                except json.JSONDecodeError as jde:
                    error_logger.error(f"JSON Decode Error: {jde}")
                    with open(json_file_path, "r") as error_file:
                        content = error_file.read()
                        error_logger.error(f"Content of the JSON file causing the issue: {content}")
                    return False

            changes_made = False

            # Process the JSON file and get matched CUR_CO_FIELD_NO values
            matched_fields = process_json_file(data, field_values)  # This returns a list of CUR_CO_FIELD_NO values
            # null_logger.info(f"Matched_fields in map export: {matched_fields}")

            # Run the query on the matched fields and get the converted results
            converted_results = run_query_on_matched_fields(matched_fields)
            # null_logger.info(f"Converted results in map export: {converted_results}")

            if converted_results:
                # Create a dictionary to map CUR_CO_FIELD_NO to their converted results
                result_mapping = {result['field'].strip().lower(): result for result in converted_results}

                # Loop through JSON features and update based on matched fields
                for feature in data.get("features", []):
                    cur_co_field_no = feature.get("properties", {}).get("field")
                    if cur_co_field_no is not None:  # Check if CUR_CO_FIELD_NO is not None
                        cur_co_field_no_lower = cur_co_field_no.strip().lower()
                        if cur_co_field_no_lower in result_mapping:
                            # Update the feature's properties with the corresponding converted result
                            feature["properties"].update(result_mapping[cur_co_field_no_lower])
                            null_logger.info(f"Updated feature with field: {cur_co_field_no_lower} "
                                             f"with data: {result_mapping[cur_co_field_no_lower]}")
                            changes_made = True

            if changes_made:
                base_dir = os.path.dirname(json_file_path)
                new_filename = f"merged_{datetime.now().strftime('%Y%m%d')}.json"
                new_file_path = os.path.join(base_dir, new_filename)

                try:
                    with open(new_file_path, "w") as new_json_file:
                        json.dump(data, new_json_file, indent=4, default=default_converter)
                    logging.info(f"JSON data merged and saved to {new_file_path}")
                    print(f"JSON conversion complete and saved to {new_file_path}")
                except Exception as e:
                    error_logger.error(f"Error saving JSON file to {new_file_path}: {e}")
                    raise

            # print(f"Called {call_count}")

    except Exception as e:
        error_logger.error(f"Error in map_export_JSON data: {e}")
        show_error_popup(f"Error in map_export_JSON data: {e}")


def json_conversion():
    logging.info("Starting JSON conversion...")
    print("Starting JSON conversion")
    try:
        threading.Thread(target=map_export_json_data, args=(json_file_path,)).start()

    except Exception as e:
        error_logger.error(f"Error in periodic check: {e}")
        show_error_popup(f"Error in periodic check: {e}")


##############################################################

if __name__ == "__main__":
    check_for_updates("develop") # check for updates in develop branch
    create_default_ini(CONFIG_FILE)

    connection_details, json_file_path = read_default_connection()

    if not connection_details:
        get_connection_details()

    if not json_file_path:
        json_file_path = search_json_file_path()

    if connection_details and json_file_path:
        # print(connection_details)
        init_connection_pool(connection_details)
        json_conversion()


