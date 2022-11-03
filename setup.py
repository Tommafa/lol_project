from setuptools import setup, find_packages

VERSION = "0.0.1"
DESCRIPTION = "Data reader from Lol api"
LONG_DESCRIPTION = "This package allows to read, given a list of summoners, a set of games and write the output to a file"
setup(
   name='Data Reader',
   version=VERSION,
   description=DESCRIPTION,
   long_description=LONG_DESCRIPTION,
   author='Tommaso Lanni',
   author_email='tommasolanni94@gmail.com',
   packages=find_packages(),
   install_requires=['wheel'], #external packages as dependencies
)