from setuptools import setup, find_packages

setup(name='sqoop-utils',
      version='0.1',
      description='Sqoop Utils',
      url='https://github.com/panelladavide/sqoop-utils',
      packages=find_packages(),
      package_data={'sqoop_utils.cli': ['config_template.json']},
      entry_points = {
        'console_scripts': ['sqoop_utils_cli=sqoop_utils.cli:main']
      },
      test_suite='nose.collector',
      tests_require=['nose'],
      zip_safe=False)