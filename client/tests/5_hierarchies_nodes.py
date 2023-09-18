import pytest
from click.testing import CliRunner
from cli import cli

def read_file(filename):
    with open(filename, "r") as file:
        content = file.read()
    return content

def test_cli():
    runner = CliRunner()
    # Reset DB and add some tagsets
    runner.invoke(cli, ['reset'], input="y\n")
    runner.invoke(cli, ['add', 'tagset', 'Location'])          # Alphanumerical
    runner.invoke(cli, ['add', 'tagset', 'TimeStamp', '2'])    # Timestamp format YYYY-MM-dd hh:mm:ss
    runner.invoke(cli, ['add', 'tagset', 'Time', '3'])         # Time format hh:mm:ss  
    runner.invoke(cli, ['add', 'tagset', 'Date', '4'])         # Date format YYYY-MM-dd
    runner.invoke(cli, ['add', 'tagset', 'Resolution', '5'])   # Numerical
    # Add some tags
    runner.invoke(cli, ['add', 'tag', 'Europe', '1', '1'])      # ID : 1
    runner.invoke(cli, ['add', 'tag', 'Iceland', '1', '1'])     # ID : 2    
    runner.invoke(cli, ['add', 'tag', 'Norway', '1', '1'])      # ID : 3
    runner.invoke(cli, ['add', 'tag', 'Reykjavik', '1', '1'])   # ID : 4
    runner.invoke(cli, ['add', 'tag', 'Hella', '1', '1'])       # ID : 5
    runner.invoke(cli, ['add', 'tag', 'Oslo', '1', '1'])        # ID : 6
    runner.invoke(cli, ['add', 'tag', '101', '1', '1'])         # ID : 7

    # Add the medias
    runner.invoke(cli, ['add', 'media', './testfiles'])

    # Test 'getters' with empty table
    result = runner.invoke(cli, ['get', 'hierarchies'])
    assert result.output.strip() == "No results were fetched"

    result = runner.invoke(cli, ['get', 'hierarchy', '1'])
    assert result.output.strip() == "No results were fetched"

    result = runner.invoke(cli, ['get', 'node', '1'])
    assert result.output.strip() == "No results were fetched"

    result = runner.invoke(cli, ['get', 'nodes'])
    assert result.output.strip() == "No results were fetched"

    result = runner.invoke(cli, ['get', 'nodes', '-h', '1'])
    assert result.output.strip() == "No results were fetched"

    result = runner.invoke(cli, ['get', 'nodes', '-p', '1'])
    assert result.output.strip() == "No results were fetched"

    result = runner.invoke(cli, ['get', 'nodes', '-t', '1'])
    assert result.output.strip() == "No results were fetched"


    # Add hierarchies
    result = runner.invoke(cli, ['add', 'hierarchy', 'Geography', '1'])
    assert result.output.strip() == """id: 1
name: "Geography"
tagSetId: 1"""

    result = runner.invoke(cli, ['add', 'hierarchy', 'Days of week', '4'])
    assert result.output.strip() == """id: 2
name: "Days of week"
tagSetId: 4"""

    # Add rootnode
    result = runner.invoke(cli, ['add', 'rootnode', '1', '1'])
    assert result.output.strip() == """id: 1
tagId: 1
hierarchyId: 1"""

    # Retrieve the existing node if already existent
    result = runner.invoke(cli, ['add', 'rootnode', '1', '1'])
    assert result.output.strip() == """id: 1
tagId: 1
hierarchyId: 1"""

    # Add nodes according to the following tree:
    #                   Europe(1)
    #                /         \
    #       Iceland(2)         Norway(3)
    #       /       \                  \
    #  Hella(5)    Reykjavik(4)       Oslo(6)
    #                   \
    #                  101(7)

    result = runner.invoke(cli, ['add', 'node', '2', '1', '1'])
    assert result.output.strip() == """id: 2
tagId: 2
hierarchyId: 1
parentNodeId: 1"""

    result = runner.invoke(cli, ['add', 'node', '5', '1', '2'])
    assert result.output.strip() == """id: 3
tagId: 5
hierarchyId: 1
parentNodeId: 2"""

    result = runner.invoke(cli, ['add', 'node', '4', '1', '2'])
    assert result.output.strip() == """id: 4
tagId: 4
hierarchyId: 1
parentNodeId: 2"""

    result = runner.invoke(cli, ['add', 'node', '7', '1', '4'])
    assert result.output.strip() == """id: 5
tagId: 7
hierarchyId: 1
parentNodeId: 4"""

    result = runner.invoke(cli, ['add', 'node', '3', '1', '1'])
    assert result.output.strip() == """id: 6
tagId: 3
hierarchyId: 1
parentNodeId: 1"""

    result = runner.invoke(cli, ['add', 'node', '6', '1', '6'])
    assert result.output.strip() == """id: 7
tagId: 6
hierarchyId: 1
parentNodeId: 6"""

    # Now let's get some nodes
    result = runner.invoke(cli, ['get', 'node', '1'])
    assert result.output.strip() == """id: 1
tagId: 1
hierarchyId: 1"""

    result = runner.invoke(cli, ['get', 'nodes', '-t', '6'])
    assert result.output.strip() == """id: 7
tagId: 6
hierarchyId: 1
parentNodeId: 6"""

    # Child nodes of "Europe"
    result = runner.invoke(cli, ['get', 'nodes', '-p', '1'])
    assert result.output.strip() == """id: 2
tagId: 2
hierarchyId: 1
parentNodeId: 1

id: 6
tagId: 3
hierarchyId: 1
parentNodeId: 1"""

if __name__ == '__main__':
    test_cli()
