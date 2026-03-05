"""CLI entrypoint (placeholder)"""
import argparse

def main(argv=None):
    parser = argparse.ArgumentParser("gridflow cli")
    parser.add_argument("--hello", help="say hello", action="store_true")
    args = parser.parse_args(argv)
    if args.hello:
        print("hello from gridflow")

if __name__ == '__main__':
    main()
