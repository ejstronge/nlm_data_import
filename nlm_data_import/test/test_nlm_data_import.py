# -*- coding: utf-8 -*-
"""
test_nlm_data_import.py
=======================

Test suite for the archive downloading module

(c) 2014, Edward J. Stronge
Available under the GPLv3 - see LICENSE for details.
"""
from collections import namedtuple
import os
import shutil
import tempfile
import unittest

from ..nlm_data_import import nlm_downloads_db as downloads_db
from ..nlm_data_import import download_nlm_data as downloader


class TestNLMDatabase(unittest.TestCase):
    """Test that the database for downloaded files can be created
    successfully and works as expected.
    """
    NLMArchiveData = namedtuple(
        'NLMArchiveData',
        """size referenced_record filename unique_file_id modification_date
        observed_md5 md5_verified download_date download_location
        transferred_for_output export_location downloaded""")

    NLMHashData = namedtuple(
        'NLMHashData',
        """referenced_record, unique_file_id, md5_value download_date
        filename checksum_file_deleted""")

    NLMNoteData = namedtuple(
        'NLMNoteData',
        'filename referenced_record unique_file_id download_date')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.nlm_archive_test_records = [
            self.NLMArchiveData(*fields) for fields in
            (
                (93, 'nlm1', 'nlm1.xml.tar.gz', 'unique1', '20140731', 'hash1', 0, '20140731', 'downloads/', 0, None, 0),
                (92, 'nlm2', 'nlm2.xml.tar.gz', 'unique2', '20140722', 'hash2', 0, '20240732', 'downloads/', 0, None, 0),
                (93, 'nlm3', 'nlm3.xml.tar.gz', 'unique3', '20140723', 'hash3', 0, '30340733', 'downloads/', 0, None, 0),
                (94, 'nlm4', 'nlm4.xml.tar.gz', 'unique4', '20140714', 'hash4', 0, '40440744', 'downloads/', 0, None, 0),
                (95, 'nlm5', 'nlm5.xml.tar.gz', 'unique5', '20150705', 'hash5', 0, '50550755', 'downloads/', 0, None, 0),
            )]
        self.nlm_hash_test_records = [
            self.NLMHashData(*fields) for fields in
            (
                ('nlm1', 'unique-hash1', 'hash1', '20140731', 'nlm1.xml.tar.gz.md5', 0),
                ('nlm2', 'unique-hash2', 'hash2', '20140702', 'nlm2.xml.tar.gz.md5', 0),
                ('nlm3', 'unique-hash3', 'hash3', '30140703', 'nlm3.xml.tar.gz.md5', 0),
                ('nlm4', 'unique-hash4', 'hash4', '40140714', 'nlm4.xml.tar.gz.md5', 0),
                ('nlm5', 'unique-hash5', 'hash5', '20140715', 'nlm5.xml.tar.gz.md5', 0),
            )]
        self.nlm_note_test_records = [
            self.NLMNoteData(*fields) for fields in
            (
                ('special-note.txt', None, 'repeated-note1', '20140731'),
                ('nlm1-retracted.txt', 'nlm1', 'unique-note1', '20140731'),
                ('nlm4-revised.txt', 'nlm4', 'unique-note4', '20140704'),
            )]

    def setUp(self):
        """Make an in-memory database connection and load the downloads
        schema. Also move to a temporary directory
        """
        self.test_db = downloads_db.initialize_database_connection(':memory:')
        self.temp_dir = tempfile.mkdtemp()

        self.last_dir = os.getcwd()
        os.chdir(self.temp_dir)

    def tearDown(self):
        os.chdir(self.last_dir)
        shutil.rmtree(self.temp_dir)

    def populate_test_db(self):
        """ Insert test records into the test database.
        """
        self.test_db.executemany(
            downloads_db.NEW_ARCHIVE_SQL, self.nlm_archive_test_records)
        self.test_db.executemany(
            downloads_db.NEW_HASH_SQL, self.nlm_hash_test_records)
        self.test_db.executemany(
            downloads_db.NEW_NOTE_SQL, self.nlm_note_test_records)

    def generate_ftp_file_params(self):
        """Create namedtuples describing a downloaded file

        Returns a sequence of download_nlm_data.FTPFileParams
        """
        file_data = (
            ('20131125174213', '24847843', '4600001UE9FE',
                'medline14n0745.xml.gz', '20140702134531',
                '745 hash', './medline14n0745.xml.gz'),
            ('20131125174313', '24847843', '4600001UE9FF',
                'medline14n0749.xml.gz', '20140702134531',
                '749 hash', './medline14n0749.xml.gz'),
            ('20131125174556', '63', '4600001UEA02',
                'medline14n0745.xml.gz.md5', '20140702134531',
                '745 hash hash', './medline14n0745.xml.gz.md5'),
            ('20131125174559', '563', '4400001UEA02',
                'medline14n0002.important.info', '20140702134531',
                '002 note hash', './medline14n0002.important.info'),
            )

        # The application will expect the hash files to be downloaded
        # already
        with open('medline14n0745.xml.gz.md5', 'w') as test_hash_file:
            test_hash_file.write('745 hash')

        return [downloader.FTPFileParams(*d) for d in file_data]

    def test_insert_nlm_archive(self):
        """Test insertion of new nlm archive files to the downloads
        database.
        """
        self.test_db.execute(downloads_db.NEW_ARCHIVE_SQL, {
            'size':  1,
            'referenced_record': 'medline14n001',
            'filename': 'f',
            'unique_file_id': 'A',
            'modification_date': '20131128175433',
            'observed_md5': 'abcdefgh',
            'md5_verified': 0,
            'download_date': '20140702134531',
            'download_location': 'downloads',
            'transferred_for_output': 0,
            'output_path': 'exports',
            'export_location': '',
            'downloaded_by_application': 0})
        self.test_db.commit()

    def test_insert_md5_checksum(self):
        """Test insertion of an md5 hash to the downloads database."""
        self.test_db.execute(downloads_db.NEW_HASH_SQL, {
            'referenced_record': 'medline14n001',
            'unique_file_id': 'A',
            'md5_value': 'abcdefgh',
            'download_date': '20140702134531',
            'filename': 'test_file.md5',
            'checksum_file_deleted': 0})
        self.test_db.commit()

    def test_insert_note(self):
        """Test insertion of an auxiliary file to the downloads database."""
        self.test_db.execute(downloads_db.NEW_NOTE_SQL, {
            'filename': 'test_note.txt', 'referenced_record': 'medline14n001',
            'unique_file_id': 'A', 'download_date': '20140702134531', })
        self.test_db.commit()

    def test_get_downloaded_file_unique_ids(self):
        """Test whether we correctly identify all the unique file
        hashes present in the database.
        """
        self.populate_test_db()

        unique_ids = {record[3] for record in self.nlm_archive_test_records}
        for record_set in (self.nlm_hash_test_records,
                           self.nlm_note_test_records):
            for record in record_set:
                unique_ids.add(record.unique_file_id)

        self.assertSetEqual(
            downloads_db.get_downloaded_file_unique_ids(self.test_db),
            unique_ids)

    def test_record_downloads(self):
        """Test whether we correctly update the downloads database
        with newly downloaded files.
        """
        records = self.generate_ftp_file_params()
        downloads_db.record_downloads(records, self.test_db)


class TestNLMDownloader(unittest.TestCase):
    """
    Test utility functions used in the NLM downloading script.
    """

    TEST_FTP_LINES = (
        'modify=20131125174213;perm=adfr;size=24847843;'
        'type=file;unique=4600001UE9FE;UNIX.group=183;UNIX.mode=0644;'
        'UNIX.owner=505; medline14n0745.xml.gz',

        'modify=20131125174556;perm=adfr;size=63;type=file;'
        'unique=4600001UEA02;UNIX.group=183;UNIX.mode=0644;UNIX.owner=505;'
        ' medline14n0002.xml.gz.md5')

    PARSED_TEST_LINES = (
        # A tuple with fields 'modify', 'size', 'unique' and filename.
        # The first fields are from the key-value portion of the
        # test_ftp_lines elements; filename is the string separated by a
        # space from key-value portion
        ('20131125174213', '24847843', '4600001UE9FE',
            'medline14n0745.xml.gz', '', '', ''),
        ('20131125174556', '63', '4600001UEA02',
            'medline14n0002.xml.gz.md5', '', '', '')
        )

    FTP_LINES_TO_SKIP = (
        'modify=20131125174213;perm=adfr;size=24847843;'
        'type=file;unique=4600001UE9FE;UNIX.group=183;UNIX.mode=0644;'
        'UNIX.owner=505; archive.stats.html',

        'modify=20131125174556;perm=adfr;size=63;type=file;'
        'unique=4600001UEA02;UNIX.group=183;UNIX.mode=0644;UNIX.owner=505;'
        ' test.dat')

    def test_get_file_listing(self):

        self.assertListEqual(
            [],
            list(downloader.get_file_listing(self.FTP_LINES_TO_SKIP)))

        self.assertListEqual(
            list(self.PARSED_TEST_LINES),
            list(downloader.get_file_listing(self.TEST_FTP_LINES)))

    def test_parse_mlsd(self):
        """Test parsing of machine-readable FTP directory listing
        """

        for test_line, parsed_line in zip(self.TEST_FTP_LINES,
                                          self.PARSED_TEST_LINES):
            self.assertTupleEqual(
                downloader.parse_mlsd(test_line), parsed_line)
