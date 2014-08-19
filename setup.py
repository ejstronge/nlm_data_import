from setuptools import setup

setup(
    name='nlm_data_import',
    version='0.1.1',
    packages=['nlm_data_import'],
    scripts=['run_downloader.py'],
    license='MIT',

    # send_ses_message is a simple IMAP-based interface to
    # the Amazon SES service
    install_requires='send_ses_message>=0.1',
    dependency_links=[
        'https://github.com/ejstronge/send_ses_message/tarball/master#send_ses_message-0.1'
    ],

    package_data={
        'nlm_data_import': ['netrc/*.netrc'],
    },
    test_suite='nlm_data_import.test',
    author='Edward J. Stronge',
    author_email='ejstronge@gmail.com',
    description='Download XML records from the NLM',
    keywords='nlm medline',
    long_description=open('./README.md').read(),
)
