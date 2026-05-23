"""
CLI for interacting with the data loader. It is based on the Click python library, and handles most of the input checking.
"""

from __future__ import print_function

import click
import os
import re
import datetime


from grpc import RpcError
from grpc_client import LoaderClient
from filemgmt.filehandler import FileHandler
from filemgmt.json_hr import JSONHandler
from filemgmt.csv import CSVHandler
from filemgmt.json_fast import FastJSONHandler
from filemgmt.tagset_importer import TagsetImporter


client = LoaderClient(grpc_host='localhost', grpc_port='50051')

SERVER_ADDRESS = "localhost:50051"

@click.group()
def cli():
    """CLI interface to interact with the M3 database. From this interface you can access and add Medias, Tagsets, Tags, Taggings, Nodes and Hierarchies."""
    pass

#!================ GET functions ======================================================================

@cli.group()
def get():
    """Retrieve stored information from the database."""
    pass


@get.command()
@click.option("-i", "--id", "media_id", type=int, help="Media ID")
@click.option("-u", "--uri", "media_uri", type=str, help="Media URI")
def media(media_id, media_uri): # type: ignore
    """Retrieve a single media. You can either provide its ID using [-i] or its URI using [-u]."""
    if media_id is not None:
        if media_id > 0 :
            try :
                response = client.get_media_by_id(media_id)
                click.echo(response)
            except RpcError as e:
                return click.echo(f"Grpc error: {e.details()}")
        else:
            click.echo("Error: index must be > 0")

    elif media_uri is not None:
        try:
            response = client.get_media_by_uri(media_uri)
            click.echo(response)
        except RpcError as e:
            return click.echo(f"Grpc error: {e.details()}")

    else:
        click.echo("Usage: loader get media [-i ID | -u URI]\nTry 'loader get media --help' for help.\n\nError: Missing argument 'ID' or 'URI'")


@get.command()
@click.option("-ft", "--file_type", "file_type", type=int, default=-1, help="File type filter (default: all)")
def medias(file_type): # type: ignore
    """List and filter medias. Use [-tp] to filter results with a specific file type (1 = Images, 2 = Videos, 3 = Audio, 4 = Other)"""
    try:
        response_iterator = client.get_medias(file_type)
    except RpcError as e:
        return click.echo(f"Grpc error: {e.details()}")
    for response in response_iterator:
        if response.HasField("error"):
            click.echo(f"\tGrpc error: {response.error.message}")
        click.echo(response)


@get.command()
@click.option("-i", "--id", "tagset_id", type=int, help="Tagset ID")
@click.option("-n", "--name", "tagset_name", type=str, help="Tagset name")
def tagset(tagset_id, tagset_name): # type: ignore
    """Retrieve a single tagset. You can either provide its ID using [-i] or its name using [-n]."""
    if tagset_id is not None:
        try:
            response = client.get_tagset_by_id(tagset_id)
            click.echo(response)
        except RpcError as e:
            return click.echo(f"Grpc error: {e.details()}")
    elif tagset_name is not None:
        try:
            response = client.get_tagset_by_name(tagset_name)
            click.echo(response)
        except RpcError as e:
            return click.echo(f"Grpc error: {e.details()}")
    else:
        click.echo("Usage: loader get tagset [-i ID | -n NAME]\nTry 'loader get tagset --help' for help.\n\nError: Missing argument 'ID' or 'NAME'")


@get.command()
@click.option("-tp", "--type", "tagtype_id", type=int, default=-1, help="Tag type filter (default: all)")
def tagsets(tagtype_id): # type: ignore
    """List and filter tagsets. Use [-tp] to filter results with a specific tag type."""
    try:
        response_iterator = client.get_tagsets(tagtype_id)
    except RpcError as e:
        return click.echo(f"Grpc error: {e.details()}")
    for response in response_iterator:
        if response.HasField("error"):
            click.echo(f"\tGrpc error: {response.error.message}")
        else:
            click.echo(response)


@get.command()
@click.argument("id", type=int)
def tag(id): # type: ignore
    """Get a single tag with given ID."""
    if id > 0:
        try:
            response = client.get_tag(id)
            click.echo(response)
        except RpcError as e:
            return click.echo(f"Grpc error: {e.details()}")
    else:
        click.echo("Error: index must be > 0")


@get.command()
@click.option("-tp", "--type", "tagtype_id", type=int, default=-1, help="Tag type filter (default: all)")
@click.option("-ts", "--tagset", "tagset_id", type=int, default=-1, help="Tagset filter (default: all)")
def tags(tagtype_id, tagset_id): # type: ignore
    """List and filter tags. Use [-tp] to filter results with a specific tag type or [-ts] for a specific tagset."""
    try:
        response_iterator = client.get_tags(tagtype_id=tagtype_id, tagset_id=tagset_id)
    except RpcError as e:
        return click.echo(f"Grpc error: {e.details()}")
    for response in response_iterator:
        if response.HasField("error"):
            click.echo(f"\tGrpc error: {response.error.message}")
        else:
            click.echo(response)


@get.command()
@click.argument("tag_id", type=int)
def medias_with_tag(tag_id): # type: ignore
    """List medias with the specified tag."""
    if tag_id > 0:
        try:
            response = client.get_medias_with_tag(tag_id)
            click.echo(response)
        except RpcError as e:
            return click.echo(f"Grpc error: {e.details()}")
    else:
        click.echo("Error: index must be > 0")


@get.command()
@click.argument("media_id", type=int)
def tags_of_media(media_id): # type: ignore
    """List the tags of a given media."""
    if media_id > 0:
        try:
            response = client.get_media_tags(media_id)
            click.echo(response)
        except RpcError as e:
            return click.echo(f"Grpc error: {e.details()}")
    else:
        click.echo("Error: index must be > 0")


@get.command()
def taggings():
    """List all taggings."""
    try:
        response_iterator = client.get_taggings()
    except RpcError as e:
        return click.echo(f"Grpc error: {e.details()}")
    for response in response_iterator:
        if response.HasField("error"):
            click.echo(f"\tGrpc error: {response.error.message}")
        else:
            click.echo(response)

@get.command()
@click.argument("id", type=int)
def hierarchy(id): # type: ignore
    """Get a single hierarchy with the given ID."""
    if id > 0:
        try:
            response = client.get_hierarchy(id)
            click.echo(response)
        except RpcError as e:
            return click.echo(f"Grpc error: {e.details()}")
    else:
        click.echo("Error: index must be > 0")

@get.command()
@click.option("-ts", "--tagset", "tagset_id", type=int, default=-1, help="Tagset filter (default: all)")
def hierarchies(tagset_id):
    """List and filter hierarchies. Use [-ts] to filter hierarchies of a specific tagset only."""
    try:
        response_iterator = client.get_hierarchies(tagset_id)
    except RpcError as e:
        return click.echo(f"Grpc error: {e.details()}")
    for response in response_iterator:
        if response.HasField("error"):
            click.echo(f"\tGrpc error: {response.error.message}")
        else:
            click.echo(response)


@get.command()
@click.argument("id", type=int)
def node(id): # type: ignore
    """Get a single node with the given ID."""
    if id > 0:
        try:
            response = client.get_node(id)
            click.echo(response)
        except RpcError as e:
            return click.echo(f"Grpc error: {e.details()}")
    else:
        click.echo("Error: index must be > 0")


@get.command()
@click.option("-h", "--hierarchy", "hierarchy_id", type=int, default=-1, help="Hierarchy filter (default: all)")
@click.option("-t", "--tag", "tag_id", type=int, default=-1, help="Tag filter (default: all)")
@click.option("-p", "--parent", "parentnode_id", type=int, default=-1, help="Parent node filter (default: all)")
def nodes(hierarchy_id, tag_id, parentnode_id):
    """List and filter nodes. Use [-h], [-t] and [-p] to filter on hierarchy, tag or parent node respectively."""
    try:
        response_iterator = client.get_nodes(hierarchy_id, tag_id, parentnode_id)
    except RpcError as e:
        return click.echo(f"Grpc error: {e.details()}")
    for response in response_iterator:
        if response.HasField("error"):
            click.echo(f"\tGrpc error: {response.error.message}")
        else:
            click.echo(response)

#!================ ADD functions ======================================================================

@cli.group()
def add():
    """Add elements to the database model"""
    pass

@add.command()
@click.argument("path", type=click.Path(exists=True))
def media(path): # type: ignore
    """Add a given file to the database."""
    if os.path.isfile(path):
        try:
            response = client.add_file(path)
            click.echo(response)
        except RpcError as e:
            return click.echo(f"Grpc error: {e.details()}")
    else:
        click.echo("Error: invalid path provided.")

@add.command()
@click.argument("path", type=click.Path(exists=True))
@click.option(
    "--formats",
    "-f",
    multiple=True,
    default=["jpg", "png", "bmp", "mp3", "wav", "flac", "mp4", "avi"],
    help="File formats to include (default: jpg, png, bmp, mp3, wav, flac, mp4, avi)",
)
def medias(path, formats): # type: ignore
    """Add multiple files from a specified directory to the database. Be careful, the same file location cannot be added twice to the database."""
    if os.path.isdir(path):
        try:
            response_iterator = client.add_dir(path, formats)
        except RpcError as e:
            return click.echo(f"Grpc error: {e.details()}")
        for response in response_iterator:
            if response.HasField("error"):
                click.echo(f"\tGrpc error: {response.error.message}")
            else:
                click.echo(response)
    else:
        click.echo("Error: invalid path provided.")


@add.command()
@click.argument("name", type=str)
@click.argument("type", type=int, default=1, required=False)
def tagset(name, type): # type: ignore
    """Create a new tagset with specified [name] and [type] (default = 1 - AlphaNumerical)
    Supported types are 1 - Alphanumerical, 2 - Timestamp, 3 - Time, 4 - Date, 5 - Numerical."""
    if type not in range(1, 6):
        raise click.BadParameter("Type doesn't exist")
    try:
        response = client.add_tagset('%s' % name, type)
        click.echo(response)
    except RpcError as e:
        return click.echo(f"Grpc error: {e.details()}")


def validate_format(value, type):
    match type:
        case 1:
            return value
        case 2:
            format_regex = r'^\d{1,4}-\d{1,2}-\d{1,2}/\d{1,2}:\d{2}:\d{2}$'
        case 3:
            format_regex = r'^\d{1,2}:\d{2}:\d{2}$'
        case 4:
            format_regex = r'^\d{1,4}-\d{1,2}-\d{1,2}$'
        case 5:
            if not value.isdigit():
                raise click.BadParameter('Value must be an integer')
            return int(value)
        case _:
            raise click.BadParameter('Invalid type')

    if not re.match(format_regex, value):
        raise click.BadParameter('Invalid format')

    return value

@add.command()
@click.argument("value")
@click.argument("tagset_id", type=int)
@click.argument("type_id", type=int)
def tag(value, tagset_id, type_id): # type: ignore
    """Create a new tag with specified value. Based on the type, formats are as follows:\n
1 - Alphanumerical: any\n\n2 - Timestamp: YYYY-MM-dd/hh:mm:ss\n\n3 - Time: hh:mm:ss\n\n4 - Date: YYYY-MM-dd\n\n5 - Numerical: integer"""
    value = validate_format(value, type_id)
    try:
        response = client.add_tag(tagset_id, type_id, value)
        click.echo(response)
    except RpcError as e:
        return click.echo(f"Grpc error: {e.details()}")


@add.command()
@click.argument("media_id", type=int)
@click.argument("tag_id", type=int)
def tagging(tag_id, media_id): # type: ignore
    """Create a tagging between Media ID and Tag ID"""
    try:
        response = client.add_tagging(tag_id, media_id)
        click.echo(response)
    except RpcError as e:
        return click.echo(f"Grpc error: {e.details()}")

@add.command()
@click.argument("name", type=str)
@click.argument("tagset_id", type=int)
def hierarchy(name, tagset_id): # type: ignore
    """Create an empty hierarchy with given name and tagset_id"""
    try:
        response = client.add_hierarchy(name, tagset_id)
        click.echo(response)
    except RpcError as e:
        return click.echo(f"Grpc error: {e.details()}")

@add.command()
@click.argument("tag_id", type=int)
@click.argument("hierarchy_id", type=int)
@click.argument("parentnode_id", type=int)
def node(tag_id, hierarchy_id, parentnode_id): # type: ignore
    """Create a node with given tag_id, hierarchy_id and parentnode_id"""
    try:
        response = client.add_node(tag_id, hierarchy_id, parentnode_id)
        click.echo(response)
    except RpcError as e:
        return click.echo(f"Grpc error: {e.details()}")

@add.command()
@click.argument("tag_id", type=int)
@click.argument("hierarchy_id", type=int)
def rootnode(tag_id, hierarchy_id): # type: ignore
    """Create a root node for the given hierarchy"""
    try:
        response = client.add_rootnode(tag_id, hierarchy_id)
        click.echo(response)
    except RpcError as e:
        return click.echo(f"Grpc error: {e.details()}")

#!================ UPDATE functions ======================================================================

@cli.group()
def update():
    """Update tags and taggings in the database.

    \b
    Two update operations are available:
      - tag-name : rename a tag globally (all medias follow)
      - tagging  : change the tag of ONE specific media only
    """
    pass


@update.command(name='tag-name')
@click.argument("tagset_name", type=str)
@click.argument("old_value", type=str)
@click.argument("new_value", type=str)
def tag_name(tagset_name, old_value, new_value):
    """Rename a tag globally. ALL medias with this tag are affected.

    \b
    Examples:
      loader update tag-name faces Unknown3 Alice
        → The tag "Unknown3" becomes "Alice" everywhere in the database.
        → Every media tagged "Unknown3" is now tagged "Alice".

    \b
      loader update tag-name Country "Czech Republic" Czechia
        → All medias tagged "Czech Republic" are now tagged "Czechia".

    \b
    TAGSET_NAME : name of the tagset (e.g. "faces", "Country", "City")
    OLD_VALUE   : current value of the tag
    NEW_VALUE   : new value to assign

    \b
    Notifies plugins via RabbitMQ on 'tag_update.<tagset_name>'.
    """
    try:
        response = client.change_tag_name(tagset_name, old_value, new_value)
        click.echo(response)
    except RpcError as e:
        return click.echo(f"Grpc error: {e.details()}")


@update.command(name='tagging')
@click.argument("media_uri", type=str)
@click.argument("tagset_name", type=str)
@click.argument("old_value", type=str)
@click.argument("new_value", type=str)
def tagging(media_uri, tagset_name, old_value, new_value):
    """Change the tag of ONE specific media. Other medias are NOT affected.

    \b
    Examples:
      loader update tagging http://.../photo.jpg faces Unknown3 Bob
        → Only photo.jpg switches from "Unknown3" to "Bob".
        → Other medias tagged "Unknown3" stay unchanged.

    \b
      loader update tagging http://.../img_042.jpg City Dublin Cork
        → Only img_042.jpg switches from "Dublin" to "Cork".
        → Other medias tagged "Dublin" stay unchanged.
        → The geodata plugin updates its local database for this media.

    \b
    MEDIA_URI   : URI of the media to update
    TAGSET_NAME : name of the tagset (e.g. "faces", "Country", "City")
    OLD_VALUE   : current tag value for this media
    NEW_VALUE   : new tag value to assign (created if it doesn't exist)

    \b
    Notifies plugins via RabbitMQ on 'tagging_update.<tagset_name>'.
    For geodata tagsets (Country, City, Location Type, POI Category),
    the server also forwards to 'update_geodatas.*' so the geodata
    plugin can update its local JSON database.
    """
    try:
        response = client.change_tagging(media_uri, tagset_name, old_value, new_value)
        click.echo(response)
    except RpcError as e:
        return click.echo(f"Grpc error: {e.details()}")


#!================ DELETE functions ======================================================================

@cli.group()
def delete():
    """Delete elements from the database"""
    pass

@delete.command()
@click.argument("media_id", type=int)
def media(media_id):
    """Delete a single media with the given ID"""
    if media_id > 0:
        try:
            response = client.delete_media(media_id)
            click.echo(response)
        except RpcError as e:
            return click.echo(f"Grpc error: {e.details()}")
    else:
        click.echo("Input error: index must be > 0")

@delete.command()
@click.argument("node_id", type=int)
def node(node_id):
    """Delete a single node with the given ID"""
    if node_id > 0:
        try:
            response = client.delete_node(node_id)
            click.echo(response)
        except RpcError as e:
            return click.echo(f"Grpc error: {e.details()}")
    else:
        click.echo("Input error: index must be > 0")


#!================ General functions ======================================================================

@cli.command(name='import')
@click.option(
    "--format", "-f",
    default="json",
    help="Format of the import file. Currently supported formats: json, csv.",
)
@click.option(
    "--lsc",
    is_flag=True,
    default=False,
    help="Parse the CSV as an LSC dataset (header row with column names, one media per row).",
)
@click.option(
    "--media-host",
    default="http://localhost:5005",
    show_default=True,
    help="Base URL used to build media URIs when importing an LSC CSV.",
)
@click.option(
    "--column-map",
    "column_map",
    default=None,
    type=click.Path(exists=True),
    help="Path to a JSON file overriding the default LSC column mapping.",
)
@click.argument("path", type=click.Path(exists=True))
def import_command(format, lsc, media_host, column_map, path):
    """Add tagsets, objects and tags from a setup file in the selected format.

    \b
    Standard CSV format:
      Line 1 : tagset_name_1;tagset_type_1;tagset_name_2;tagset_type_2;...
      Line N : media_path;tagset_index_1;value_1;tagset_index_2;value_2;...

    \b
    LSC CSV format (use --lsc flag):
      Line 1 : minute_id,utc_time,local_time,timezone,lat,lon,...
      Line N : 20150223_0000,UTC_2015-02-23_00:00,...
      Media URI is built as: <media-host>/lsc/<YYYYMM>/<DD>/<minute_id>_000.jpg
    """
    if lsc and format != "csv":
        click.echo("Error: --lsc flag is only valid with --format csv.")
        return

    if column_map is not None and not lsc:
        click.echo("Error: --column-map is only valid with --lsc.")
        return

    if not os.path.isfile(path):
        click.echo("Error: invalid file path.")
        return

    if not path.lower().endswith(format):
        click.echo(f"Error: file extension does not match format '{format}'.")
        return

    if format == "json":
        fileHandler = JSONHandler()
        fileHandler.importFile(path)
    elif format == "csv":
        fileHandler = CSVHandler()
        fileHandler.importFile(path, lsc=lsc, media_host=media_host, column_map=column_map)
    else:
        click.echo(f"Error: format '{format}' is not supported.")


@cli.command(name='export')
@click.option(
    "--format", "-f",
    default="json",
    help="Format of the export file. Currently supported formats: json.",
)
@click.argument("path", type=click.Path())
def export_command(format, path):
    """Export the current collection configuration to a file in the specified format (default: json)."""
    if os.path.isdir(path):
        # Generate the file name automatically based on the current date
        dir_path = path
        file_id = 1
        today = datetime.datetime.today().strftime("%Y-%m-%d")
        while True:
            path = f"{dir_path}/{today}-{file_id}.{format}"
            if not os.path.isfile(path):
                break
            file_id += 1

    if format == "json":
        fileHandler = JSONHandler()
    elif format == "csv":
        fileHandler = CSVHandler()
    else:
        click.echo("Error: format '%s' is not supported." % format)
        return

    fileHandler.exportFile(path)
    click.echo("File created at %s" % path)


@cli.command()
@click.argument("path", type=click.Path(exists=True))
def import_fast(path):
    """Add tagsets, objects and tags from a JSON setup file. Optimised for speed."""
    if os.path.isfile(path) & path.lower().endswith('json'):
        fileHandler = FastJSONHandler()
        fileHandler.importFile(path)
    else:
        click.echo("Error: invalid file path or format.")

@cli.command()
@click.argument("path", type=click.Path())
def export_fast(path):
    """Export the current collection configuration. Human-readability is sacrificed for speed."""
    if os.path.isdir(path):
        # Generate the file name automatically based on the current date
        dir_path = path
        file_id = 1
        today = datetime.datetime.today().strftime("%Y-%m-%d")
        while True:
            path = f"{dir_path}/{today}-{file_id}.{format}"
            if not os.path.isfile(path):
                break
            file_id += 1

    fileHandler = FastJSONHandler()
    fileHandler.exportFile(path)
    click.echo("File created at %s" % path)


@cli.command()
def reset():
    """Reset the database"""
    if click.confirm(
        "Do you want to reset the database ? All data will be lost.", default=False
    ):
        try:
            response = client.reset()
            click.echo(response)
        except RpcError as e:
            return click.echo(f"Grpc error: {e.details()}")


@cli.command(name='import_tagsets')
@click.option(
    "--format", "-f",
    default=None,
    type=click.Choice(["csv", "json"], case_sensitive=False),
    help="Format of the file (csv or json). Auto-detected from the file extension if omitted.",
)
@click.argument("path", type=click.Path(exists=True))
def import_tagsets_command(format, path):
    """Import tagsets from a CSV or JSON file without creating any media.

    \b
    Expected CSV format (comma or semicolon separator, with or without header):
      name,type
      Day of week (number),numerical
      Timestamp UTC,timestamp

    \b
    Expected JSON format:
      [
        {"name": "Day of week (number)", "type": "numerical"},
        {"name": "Timestamp UTC",        "type": "timestamp"}
      ]

    \b
    Accepted types: alphanumerical, timestamp, time, date, numerical
    """
    # Auto-detect format from file extension if not provided
    if format is None:
        if path.lower().endswith('.json'):
            format = 'json'
        elif path.lower().endswith('.csv'):
            format = 'csv'
        else:
            click.echo("Error: unable to detect format. Use -f csv or -f json.")
            return

    importer = TagsetImporter()
    importer.importFile(path)


if __name__ == "__main__":
    cli(obj={})
