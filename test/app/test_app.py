#!/usr/bin/env python3
import unittest
import subprocess
import os
import re


TEST_TEXT = ['Enable short progress output from tests.',
             'When the output of ctest is being sent directly to a terminal, the progress through.',
             'the set of tests is reported by updating the same line rather than printing start and '
             'end messages for each test on new lines.']

class AppTest(unittest.TestCase):
    def setUp(self):
        assert not os.path.exists('./file.txt')
        assert not os.path.exists('./output.txt')
        assert not os.path.exists('./splits')

        with open('./file.txt', 'w') as f:
            f.writelines(TEST_TEXT)

    def tearDown(self):
        os.remove('./file.txt')
        os.remove('./output.txt')

    def test_run(self):
        real = {}
        result = {}
        for word in re.findall(r'\b\w+\b', '\n'.join(TEST_TEXT)):
            if word not in real:
                real[word] = 1
            else:
                real[word] += 1

        try:
            process = subprocess.run(['../../../bin/app', './file.txt', './splits', './output.txt'], timeout=5, shell=False)
            assert process.returncode == 0
            with open('./output.txt', 'r') as f:
                for line in f.readlines():
                    word, val = line.split(':')
                    result[word.strip()] = int(val.strip())

        except subprocess.TimeoutExpired:
            assert False, 'Timeout Expired'

        assert real == result


if __name__ == '__main__':
    unittest.main()