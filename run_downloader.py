# -*- coding: utf-8 -*-
"""
run_downloader.py
=================

Script to configure the download_nlm_data module and
download data from the NLM servers.
"""
import argparse

from nlm_data_import import download_nlm_data


def handle_args():
    """Parse command-line arguments to this script"""
    parser = argparse.ArgumentParser(
        description="""Script to download new files from the NLM public
                       FTP server.
                    """)
    # Server settings
    parser.add_argument(
        '-n', '--netrc', default='~/.netrc',
        help="""netrc file containing login parameters for the NLM
                server. See `man 5 netrc` for details on generating this
                file.
             """)
    parser.add_argument(
        'server_data_dir',
        help='Directory containing desired files on the NLM FTP server')
    parser.add_argument(
        '-l', '--limit', type=int, default=0,
        help='Only download LIMIT files.')

    # Download settings
    parser.add_argument(
        '-d', '--download_database', default='~/.ftp_download_db',
        help='Path to SQLite database detailing past downloads')
    parser.add_argument(
        '-o', '--output_dir', default='~/medline_data',
        help='Directory where downloads will be saved')
    parser.add_argument(
        '-x', '--export_dir', default='~/medline_data_exports',
        help="""Directory where data to be retrieved by the
                `hypothesis_graph application server are staged.
             """)
    # Sending debug emails
    parser.add_argument(
        '--email_debugging', default=False, action='store_true',
        help="Send debugging emails. Defaults to FALSE.")
    parser.add_argument(
        '--from_email', required=False, help="FROM field for debugging emails")
    parser.add_argument(
        '--to_email', required=False, help="TO field for debugging emails")

    return parser.parse_args()

if __name__ == '__main__':
    download_nlm_data.main(handle_args())
