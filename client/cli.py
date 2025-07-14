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
    """Add a multiple files from a specified directory to the database. Be careful, the same file location cannot be added twice to the database."""
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
    if type not in range(1,6):
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
def tagging(tag_id, media_id): #type: ignore
    """Create a tagging between Media ID and Tag ID"""
    try:
        response = client.add_tagging(tag_id, media_id)
        click.echo(response)
    except RpcError as e:
        return click.echo(f"Grpc error: {e.details()}")

@add.command()
@click.argument("name", type=str)
@click.argument("tagset_id", type=int)
def hierarchy(name, tagset_id): #type: ignore
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
def node(tag_id, hierarchy_id, parentnode_id): #type: ignore
    """Create a hierarchy with given name, tagset_id and rootnode_id"""
    try:
        response = client.add_node(tag_id, hierarchy_id, parentnode_id)
        click.echo(response)
    except RpcError as e:
        return click.echo(f"Grpc error: {e.details()}")

@add.command()
@click.argument("tag_id", type=int)
@click.argument("hierarchy_id", type=int)
def rootnode(tag_id, hierarchy_id): #type: ignore
    """Create rootnode for given hierarchy"""
    try:
        response = client.add_rootnode(tag_id, hierarchy_id)
        click.echo(response)
    except RpcError as e:
        return click.echo(f"Grpc error: {e.details()}")

#!================ DELETE functions ======================================================================

@cli.group()
def delete():
    """Delete elements from database"""
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
    "--format",
    "-f",
    default="json",
    help="Format of the import file. Currently supported formats: json, csv.",
)
@click.argument("path", type=click.Path(exists=True))
def import_command(format, path):
    """Add tagsets, objects and tags from a setup file in the selected format."""
    if os.path.isfile(path) & path.lower().endswith(format):
        if format == "json":
            fileHandler = JSONHandler()
            fileHandler.importFile(path)
        elif format == "csv":
            fileHandler = CSVHandler()
            fileHandler.importFile(path)
        else:
            click.echo("Error: format '%s' is not supported." % format)
    else:
        click.echo("Error: invalid file path or format.")


@cli.command(name='export')
@click.option(
    "--format",
    "-f",
    default="json",
    help="Format of the import file. Currently supported formats: json.",
)
@click.argument("path", type=click.Path())
def export_command(format, path):
    """Export the current collection configuration to a file in specified format (default: json)."""
    if os.path.isdir(path):
        # Generate the file name automatically
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
    """Add tagsets, objects and tags from a setup file in the selected format."""
    if os.path.isfile(path) & path.lower().endswith('json'):
        fileHandler = FastJSONHandler()
        fileHandler.importFile(path)
    else:
        click.echo("Error: invalid file path or format.")

@cli.command()
@click.argument("path", type=click.Path())
def export_fast(path):
    """Export the current collection configuration. Human-readability is sacrificed for more speed."""
    
    if os.path.isdir(path):
        # Generate the file name automatically
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


if __name__ == "__main__":
    cli(obj={})
