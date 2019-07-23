from setuptools import setup

setup(name='sqoop-utils',
      version='0.1',
      description='Sqoop Utils',
      url='https://github.com/panelladavide/sqoop-utils',
      packages=['sqoop_utils'],
      entry_points = {
        'console_scripts': ['sqoop_utils_cli=sqoop_utils.cli:main']
      },
      test_suite='nose.collector',
      tests_require=['nose'],
      zip_safe=False)