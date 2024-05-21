import pytest
from click.testing import CliRunner
from cli import cli

def read_file(filename):
    with open(filename, "r") as file:
        content = file.read()
    return content

def test_cli():
    runner = CliRunner()
    # Reset DB
    result = runner.invoke(cli, ['reset'], input="y\n")

    # Test 'get all medias' with empty DB
    result = runner.invoke(cli, ['get', 'medias'])
    print('Result: ', result.output)
    assert result.output.strip() == "Exception('No results were fetched')"
    
    # Test 'get 1 media' with empty DB
    result = runner.invoke(cli, ['get', 'media', '-i', '1'])
    print('Result: ', result.output)
    assert result.output.strip() == "Exception('No results were fetched')"

    result = runner.invoke(cli, ['get', 'media', '-u', 'anyURI'])
    assert result.output.strip() == "Exception('No results were fetched')"

    # Test 'add directory'
    result = runner.invoke(cli, ['add', 'medias', './testfiles'])
    print('Result: ', result.output)
    assert result.output.strip() == read_file('output_addmedias')

    # Test 'add directory' with format filter
    result = runner.invoke(cli, ['add', 'medias', './testfiles', '-f', 'raw'])
    print('Result: ', result.output)
    assert result.output.strip() == "Info: added 1 medias to database.\nInfo: 1 files were found in the directory."
    
    # Test 'add directory' with format filter, no files found
    result = runner.invoke(cli, ['add', 'medias', './testfiles', '-f', 'gpx'])
    print('Result: ', result.output)
    assert result.output.strip() == "Info: no files of the specified format were found in the directory."

    # Test 'add single file' with non-added file
    result = runner.invoke(cli, ['add', 'media', './testfiles/single.file'])
    print('Result: ', result.output)
    assert result.output.strip().startswith("id: 856")

    # And test to add it again
    result = runner.invoke(cli, ['add', 'media', './testfiles/single.file'])
    print('Result: ', result.output)
    assert result.output.strip().startswith("id: 856")

    # Test 'get medias'
    result = runner.invoke(cli, ['get', 'medias'])
    output_lines = result.output.split("\n\n")
    num_lines = len(output_lines)
    assert num_lines == 857     # counting the last \n which only appears here for some reason

    # Test 'get media' with unexistant ID
    result = runner.invoke(cli, ['get', 'media', '-i', '888'])
    assert result.output.strip() == "No results were fetched"

    # Test 'get media' with unexistant uri
    result = runner.invoke(cli, ['get', 'media', '-u', 'C:/bonjour'])
    assert result.output.strip() == "No results were fetched"

    # Test 'get media' with ID 1
    result = runner.invoke(cli, ['get', 'media', '-i', '856'])
    assert "single.file" in result.output        


    

if __name__ == '__main__':
    test_cli()
