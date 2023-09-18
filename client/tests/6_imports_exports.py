import pytest
from click.testing import CliRunner
from cli import cli
import os

def read_file(filename):
    with open(filename, "r") as file:
        content = file.read()
    return content

def test_cli():
    runner = CliRunner()
    # Reset DB
    runner.invoke(cli, ['reset'], input="y\n")

    output_files = ["empty_export.json", "half_empty_export.json", "full_export.json"]
    
    for filename in output_files:
        try:
            os.remove(filename)
        except OSError:
            pass
    
    # Load empty json schema
    result = runner.invoke(cli, ['import', 'empty_import.json'])
    assert result.output.strip() == "Successfully imported data from JSON file empty_import.json"

    # Export to json file
    runner.invoke(cli, ['export', 'empty_export.json'])
    assert read_file('empty_import.json') == read_file('empty_export.json')

    # Load json schema with empty fields
    result = runner.invoke(cli, ['import', 'half_empty_import.json'])
    assert result.output.strip() == "Successfully imported data from JSON file half_empty_import.json"

    # Export to another json file
    runner.invoke(cli, ['export', 'half_empty_export.json'])
    assert read_file('half_empty_import.json') == read_file('half_empty_export.json')


    runner.invoke(cli, ['reset'], input="y\n")
    # Load complete json schema
    result = runner.invoke(cli, ['import', 'full_import.json'])
    assert result.output.strip() == "Successfully imported data from JSON file full_import.json"

    # Export to another json file
    runner.invoke(cli, ['export', 'full_export.json'])
    assert read_file('full_import.json') == read_file('full_export.json')
    

    # # Reset DB
    # runner.invoke(cli, ['reset'], input="y\n")
    # # Load csv schema
    # runner.invoke(cli, ['import', '-f', 'csv', './testfiles/import.csv'])
    # # Export to another json file
    # runner.invoke(cli, ['export', './testfiles/export_csv.json'])

    # assert read_file('./testfiles/import.json') == read_file('./testfiles/export_csv.json')

if __name__ == '__main__':
    test_cli()
