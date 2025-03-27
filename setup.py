from setuptools import setup, find_packages

setup(
    name='db_toolkit',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'pandas',
        'psycopg2-binary',
        'sshtunnel',
        'python-dotenv',
    ],
)