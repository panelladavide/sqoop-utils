from unittest import TestCase
from sqoop_utils import helpers
import os
import subprocess


class TestParse(TestCase):

    def setUp(self):
        self.THIS_DIR = os.path.dirname(os.path.realpath(__file__))
        self.TEST_LOG_FILE = os.path.join(self.THIS_DIR, 'test_sqoop_log')
        self.BASE_DIR = os.path.dirname(self.THIS_DIR)
        self.TEST_SCRIPT = os.path.join(self.THIS_DIR, 'test_script.sh')
        os.environ['TEST_LOG_FILE'] = self.TEST_LOG_FILE
        self.child_process = subprocess.run(['sh', self.TEST_SCRIPT], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")

    def test01(self):
        assert self.child_process.stderr == '', 'stderr is not empty'

    def test02(self):
        content = helpers.read_plain(self.TEST_LOG_FILE)
        to_match_output = '\n'.join(self.child_process.stdout.split('\n')[1:])
        assert to_match_output == content, "stdout doesn't match the expected result"
        