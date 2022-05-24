import os
import fnmatch
from zipfile import ZipFile

from airflow.configuration import AIRFLOW_HOME


def create_package():
    print("Started package creation")

    package_path = os.path.join(AIRFLOW_HOME, 'dags', 'core')
    print("package path is ", package_path)

    zip_file_path = os.path.join(AIRFLOW_HOME, 'package.zip')

    if os.path.exists(zip_file_path):
        print("Deleteing existing of package.zip")
        os.remove(zip_file_path)
        print("Deleted existing of package.zip")
    with open(os.path.join(package_path, ".packageignore"), "r") as package_ignore_file:
        ignore_wildcards = package_ignore_file.readlines()

    with ZipFile(zip_file_path, 'w') as zip_file:
        for root, directories, files in os.walk(package_path):
            for filename in files:
                if any(map(lambda pattern: fnmatch.fnmatch(filename, pattern), ignore_wildcards)):
                    continue

                file_path = os.path.join(root, filename)
                arc_name = f"{file_path}.arc"
                zip_file.write(filename=file_path, arcname=arc_name)
                print("Added to package ", file_path)

    print("package is created at ", zip_file_path)


if __name__ == '__main__':
    create_package()
