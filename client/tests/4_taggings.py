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
    runner.invoke(cli, ['add', 'tag', 'Paris', '1', '1'])
    runner.invoke(cli, ['add', 'tag', '1789-07-14/12:00:01', '2', '2'])
    runner.invoke(cli, ['add', 'tag', '12:00:01', '3', '3'])   
    runner.invoke(cli, ['add', 'tag', '1789-07-14', '4', '4'])
    runner.invoke(cli, ['add', 'tag', '1080', '5', '5'])
    runner.invoke(cli, ['add', 'tag', '44100', '5', '5'])
    # Add the medias
    runner.invoke(cli, ['add', 'medias', './testfiles'])

    # Test 'get all taggings' with empty table
    result = runner.invoke(cli, ['get', 'taggings'])
    assert result.output.strip() == "No results were fetched"

    # Test 'get tags-of-media' with inexistant media ID
    result = runner.invoke(cli, ['get', 'tags-of-media', '11111'])
    assert result.output.strip() == "No results were fetched"

    # Test 'get tags-of-media' with no tags attributed to media
    result = runner.invoke(cli, ['get', 'tags-of-media', '1'])
    assert result.output.strip() == "No results were fetched"

    # Test 'get medias-with-tag' with inexistant tag ID
    result = runner.invoke(cli, ['get', 'medias-with-tag', '11111'])
    assert result.output.strip() == "No results were fetched"

    # Test 'get medias-with-tag' with no medias linked to specified tag
    result = runner.invoke(cli, ['get', 'medias-with-tag', '1'])
    assert result.output.strip() == "No results were fetched"

    # Add correct taggings
    result = runner.invoke(cli, ['add', 'tagging', '1', '1'])
    assert result.output.strip() == "mediaId: 1\ntagId: 1"
    
    result = runner.invoke(cli, ['add', 'tagging', '1', '2'])
    assert result.output.strip() == "mediaId: 1\ntagId: 2"

    result = runner.invoke(cli, ['add', 'tagging', '1', '3'])
    assert result.output.strip() == "mediaId: 1\ntagId: 3"

    result = runner.invoke(cli, ['add', 'tagging', '1', '4'])
    assert result.output.strip() == "mediaId: 1\ntagId: 4"

    result = runner.invoke(cli, ['add', 'tagging', '1', '5'])
    assert result.output.strip() == "mediaId: 1\ntagId: 5"

    result = runner.invoke(cli, ['add', 'tagging', '2', '5'])
    assert result.output.strip() == "mediaId: 2\ntagId: 5"

    result = runner.invoke(cli, ['add', 'tagging', '2', '6'])
    assert result.output.strip() == "mediaId: 2\ntagId: 6"

    # Test add duplicate tagging
    result = runner.invoke(cli, ['add', 'tagging', '2', '6'])
    assert result.output.strip() == "mediaId: 2\ntagId: 6"

    result = runner.invoke(cli, ['add', 'tagging', '1', '5'])
    assert result.output.strip() == "mediaId: 1\ntagId: 5"

    # Test get all taggings
    result = runner.invoke(cli, ['get', 'taggings'])
    assert result.output.strip() == read_file('output_gettaggings')

    # Test get tags of media id=1
    result = runner.invoke(cli, ['get', 'tags-of-media', '1'])
    assert result.output.strip() == "[1, 2, 3, 4, 5]"

    # Test get tags of media id=2
    result = runner.invoke(cli, ['get', 'tags-of-media', '2'])
    assert result.output.strip() == "[5, 6]"

    # Test get medias with tag 1 (Location = Paris)
    result = runner.invoke(cli, ['get', 'medias-with-tag', '1'])
    assert result.output.strip() == "[1]"

    # Test get medias with tag 5 (Resolution = 1080)
    result = runner.invoke(cli, ['get', 'medias-with-tag', '5'])
    assert result.output.strip() == "[1, 2]"

    # Test get medias with tag 6 (Resolution = 44100)
    result = runner.invoke(cli, ['get', 'medias-with-tag', '6'])
    assert result.output.strip() == "[2]"


if __name__ == '__main__':
    test_cli()
