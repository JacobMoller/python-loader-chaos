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

    # Test 'get all tagsets' with empty DB
    result = runner.invoke(cli, ['get', 'tagsets'])
    assert result.output.strip() == "No results were fetched"
    
    # Test 'get tagset with id=1' with empty DB
    result = runner.invoke(cli, ['get', 'tagset', '-i', '1'])
    assert result.output.strip() == "No results were fetched"

    # Test 'get tagset with name=Location' with empty DB
    result = runner.invoke(cli, ['get', 'tagset', '-n', 'Location'])
    assert result.output.strip() == "No results were fetched"

    # Test 'add tagset' type=default (Alphanumerical), name=Location
    result = runner.invoke(cli, ['add', 'tagset', 'Location'])
    assert result.output.strip() == """id: 1
name: "Location"
tagTypeId: 1"""

    # Test 'add tagset' type=5 (Numerical), name=Resolution
    result = runner.invoke(cli, ['add', 'tagset', 'Resolution', '5'])
    assert result.output.strip() == """id: 2
name: "Resolution"
tagTypeId: 5"""

    # Test 'get tagsets'
    result = runner.invoke(cli, ['get', 'tagsets'])
    assert result.output.strip() == """id: 1
name: "Location"
tagTypeId: 1

id: 2
name: "Resolution"
tagTypeId: 5"""

    # Test 'get tagset' with id=2
    result = runner.invoke(cli, ['get', 'tagset', '-i', '2'])
    assert result.output.strip() == """id: 2
name: "Resolution"
tagTypeId: 5"""

    # Test 'get tagset' with name=Resolution
    result = runner.invoke(cli, ['get', 'tagset', '-n', 'Resolution'])
    assert result.output.strip() == """id: 2
name: "Resolution"
tagTypeId: 5"""

    # Test duplicate names with different types
    result = runner.invoke(cli, ['add', 'tagset', 'Resolution', '1'])
    assert result.output.strip() == "Error: Tagset name 'Resolution' already exists with a different type"

    # Test duplicate names with same types
    result = runner.invoke(cli, ['add', 'tagset', 'Resolution', '5'])
    assert result.output.strip() == """id: 2
name: "Resolution"
tagTypeId: 5"""
    

if __name__ == '__main__':
    test_cli()
