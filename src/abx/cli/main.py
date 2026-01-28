import argparse
from abx.cli.convert_cmd import add_convert_subcommand

def main() -> None:
    parser = argparse.ArgumentParser(prog="ab", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--version", action="store_true", help="Print version and exit")

    subparsers = parser.add_subparsers(dest="cmd")

    #Register: ab convert ...
    add_convert_subcommand(subparsers)

    args = parser.parse_args()

    if args.version:
        print("abx 0.0.1")
        return

    #If user didn't provide a command, show help
    if not hasattr(args, "func"):
        parser.print_help()
        return

    args.func(args)
