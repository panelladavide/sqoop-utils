from unittest import TestCase
from sqoop_utils import helpers
from sqoop_utils import script_utils
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

    def test_stderr_empty(self):
        assert self.child_process.stderr == '', 'stderr is not empty'

    def test_stdout_match(self):
        content = helpers.read_plain(self.TEST_LOG_FILE)
        to_match_output = '\n'.join(self.child_process.stdout.split('\n')[1:])
        assert to_match_output == content, "stdout doesn't match the expected result"

    def test_parse_table(self):
        cmd = helpers.read_plain(self.TEST_SCRIPT)
        table_name = script_utils.get_table_name(cmd)
        assert table_name == 'PMCO_tmp'

    def test_parse_output(self):
        (m_inputs, m_outputs) = script_utils.parse_output(self.child_process.stdout)
        assert m_inputs == 88
        assert m_outputs == 88
        