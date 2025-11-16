#!/usr/bin/env python3
from database import reindex_db, reindex_db_continue, CONTINUE_REWIND

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Slimcoin explorer indexer utilities')
    parser.add_argument('--reindex', action='store_true',
                        help='Full reindex (drops tables). Combine with --height to reindex from a given height without dropping earlier data.')
    parser.add_argument('--continue', dest='cont', action='store_true',
                        help='Continue indexing to tip, rewinding a few blocks first to be safe.')
    parser.add_argument('--height', type=int, default=None,
                        help='Start height for partial reindex (used with --reindex).')
    parser.add_argument('--rewind', type=int, default=None,
                        help='Override number of blocks to rewind for --continue (default CONTINUE_REWIND or 10).')

    args = parser.parse_args()

    if args.cont:
        rw = args.rewind if args.rewind is not None else CONTINUE_REWIND
        reindex_db_continue(rewind=rw)
    elif args.reindex:
        if args.height and args.height > 0:
            reindex_db(start_height=args.height)
        else:
            reindex_db()
    else:
        parser.print_help()
