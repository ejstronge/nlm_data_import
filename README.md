# nlm_data_import

## Edward J. Stronge <ejstronge@gmail.com>

A utility to import data from the National Library of Medicine's servers.
Used to collect data for [Hypothesis Graph](https://github.com/ejstronge/hypothesis_graph).


# Usage

  usage: run_downloader.py [-h] [-n NETRC] [-l LIMIT] [-d DOWNLOAD_DATABASE]
                           [-o OUTPUT_DIR] [-x EXPORT_DIR] [--email_debugging]
                           [--from_email FROM_EMAIL] [--to_email TO_EMAIL]
                           server_data_dir
  
  Script to download new files from the NLM public FTP server.
  
  positional arguments:
    server_data_dir       Directory containing desired files on the NLM FTP
                          server
  
  optional arguments:
    -h, --help            show this help message and exit
    -n NETRC, --netrc NETRC
                          netrc file containing login parameters for the NLM
                          server. See `man 5 netrc` for details on generating
                          this file.
    -l LIMIT, --limit LIMIT
                          Only download LIMIT files.
    -d DOWNLOAD_DATABASE, --download_database DOWNLOAD_DATABASE
                          Path to SQLite database detailing past downloads
    -o OUTPUT_DIR, --output_dir OUTPUT_DIR
                          Directory where downloads will be saved
    -x EXPORT_DIR, --export_dir EXPORT_DIR
                          Directory where data to be retrieved by the
                          `hypothesis_graph application server are staged.
    --email_debugging     Send debugging emails. Defaults to FALSE.
    --from_email FROM_EMAIL
                          FROM field for debugging emails
    --to_email TO_EMAIL   TO field for debugging emails

(c) 2014, Edward J. Stronge. Released under the MIT License - see LICENSE.
