#! /usr/bin/env python
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
        description="""Script to download archives from the NLM public
                       FTP server.
                    """)
    # Server settings
    server_settings = parser.add_argument_group('FTP SERVER SETTINGS', '')
    server_settings.add_argument(
        '-n', '--netrc', default='~/.netrc',
        help="""netrc file containing login parameters for the NLM
                server. See `man 5 netrc` for details on generating this
                file or read nlm_data_import/netrc/example.netrc.
             """)
    server_settings.add_argument(
        'server_data_dir',
        help='Directory containing desired files on the NLM FTP server')
    server_settings.add_argument(
        '-l', '--limit', type=int, default=0,
        help='Only download LIMIT files.')

    # Download settings
    local_settings = parser.add_argument_group('LOCAL SETTINGS', '')
    local_settings.add_argument(
        '-d', '--download_database', default='~/.ftp_download_db',
        help='Path to SQLite database detailing past downloads')
    local_settings.add_argument(
        '-o', '--output_dir', default='~/medline_data',
        help='Directory where downloads will be saved')
    local_settings.add_argument(
        '-x', '--export_dir', default='~/medline_data_exports',
        help="""Directory where data to be retrieved by the
                `hypothesis_graph application server are staged.
             """)
    # Sending debug emails (requires the send_ses_messages module - see
    # setup.py)
    debugging_settings = parser.add_argument_group('DEBUGGING SETTINGS', '')
    debugging_settings.add_argument(
        '--email_debugging', default=False, action='store_true',
        help="Send debugging emails. Defaults to FALSE.")
    debugging_settings.add_argument(
        '--from_email', required=False, help="FROM field for debugging emails")
    debugging_settings.add_argument(
        '--to_email', required=False, help="TO field for debugging emails")

    return parser.parse_args()

if __name__ == '__main__':
    download_nlm_data.main(handle_args())
