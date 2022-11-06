from setuptools import setup, find_packages
import codecs
import os

here = os.path.abspath(os.path.dirname(__file__))

with codecs.open(os.path.join(here, "README.md"), encoding="utf-8") as fh:
    long_description = "\n" + fh.read()

VERSION = '0.0.1'
DESCRIPTION = 'Sqlite database handler'
LONG_DESCRIPTION = 'This package allows you to easily manipulate any sqlite based database'

# Setting up
setup(
    name="PyDataSqlite",
    version=VERSION,
    author="codexfast (Gilberto Leandro)",
    author_email="<codexfast@gmail.com>",
    description=DESCRIPTION,
    long_description_content_type="text/markdown",
    long_description=long_description,
    packages=find_packages(),
    install_requires=[],
    python_requires='>=3.7',
    keywords=['python', 'database', 'sqlite', 'handler'],
    classifiers=[
        "Development Status :: 1 - Planning",
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent"
    ]
)