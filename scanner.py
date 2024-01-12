import sys
import requests
import argparse
import sys
import os
from app.index import Indexer
from app.flow import FlowServer
from app.scan import Scanner

# we need python 3.9
if sys.version_info < (3, 9):
    sys.stderr.write("\nPython 3.9+ required\n\n")
    sys.exit(1)

# constants
PATH_SCRIPT = os.path.dirname(os.path.realpath(__file__))

"""
Parse the command line arguments.
"""
def parse_parameters():
    parser = argparse.ArgumentParser(description="")
    parser.prog = "dupe-scanner"
    parser.description = "Detect duplicate images in local folders using pixolution Flow as analysis and search backend."
    parser.epilog = u"Crafted with \u2665 in Berlin by pixolution.io"
    parser.add_argument("--host", help="host of Flow server", dest="host", default="http://localhost:8983")
    parser.add_argument("--collection", help="collection name", metavar="NAME", dest="collection", default="my-collection")
    subparsers = parser.add_subparsers(help="available commands")
    subparsers.required = True
    subparsers.dest = "command"
    parser_status = subparsers.add_parser("status", help="Ping a Flow node to check it is up and running.")
    parser_indexer = subparsers.add_parser("index", help="Analyze and index images to Flow.")
    parser_indexer.add_argument("--dir",help="Path to image folder to index.", dest="dir", required=True)
    parser_indexer.add_argument("--recursive",help="Also index subdirectories.", dest="recursive", default=True, action=argparse.BooleanOptionalAction)
    parser_clear = subparsers.add_parser("clear", help="Empty Flow collection.")
    parser_scan = subparsers.add_parser("scan", help="Scan Flow collection for duplicate images.")
    parser_scan.add_argument("--threshold",help="Relevance threshold (0-1). Exact duplicates=0.9, near duplicates=0.6", default=0.6, dest="threshold", type=float)
    try:
        args = parser.parse_args()
    except:
        raise
    return args




def main(args=None):
    args = parse_parameters()
    flow = FlowServer(host=args.host, collection=args.collection)
    # Available cpu cores for this process
    cpu_avail = len(os.sched_getaffinity(0))
    if args.command == "status":
        flow.status()
    elif args.command == "clear":
        flow.clear_collection()
    elif args.command == "index":
        indexer = Indexer(flow)
        indexer.index_local_images(args.dir, args.recursive, threads=cpu_avail)
    elif args.command == "scan":
        scanner = Scanner(server=flow, threshold=args.threshold, threads=cpu_avail)
        scanner.scan()
        scanner.save_json()
        scanner.save_html()
    else:
        print("Unknown command: "+args.command)

if __name__ == "__main__":
    main()
