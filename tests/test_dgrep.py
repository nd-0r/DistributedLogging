import os
import sys
import shutil
import subprocess
import signal
import string
import tempfile
import random
import time
import argparse
from dataclasses import dataclass

from typing import List

parser = argparse.ArgumentParser(description="End-to-end tests for distributed logging system")
parser.add_argument("logservent_executable",
                    help="Absolute path to logservent executable",
                    type=str)
parser.add_argument("dgrep_executable",
                    help="Absolute path to dgrep executable",
                    type=str)

class Process(object):
    def __init__(self, args: List[str]):
        self._args = args
        self._popen = None

        self._outfile = None
        self._spawned = False
        self._running = False

    def wait(self, timeout: int):
        if self._spawned:
            assert self._popen
            retcode = self._popen.wait(timeout=timeout)

            if retcode is not None and self._outfile:
                self._outfile.close()
                self._running = False

            return retcode
        else:
            raise RuntimeError("Process not spawned yet")

    def spawn(self, output_file_path: str | None = None):
        if self._spawned:
            return

        if output_file_path:
            self._outfile = open(output_file_path, 'wb')
            self._popen = subprocess.Popen(
                self._args,
                start_new_session=True,
                stdout=self._outfile
            )
        else:
            self._popen = subprocess.Popen(
                self._args,
                start_new_session=True
            )

        self._spawned = True
        self._running = True

    def send_sigint(self):
        if not self._spawned or not self._running:
            return False

        assert self._popen

        self._popen.send_signal(signal.SIGINT)

    def force_kill(self):
        if not self._spawned or not self._running:
            return False

        assert self._popen

        self._popen.kill()

        if self._outfile:
            self._outfile.close()
        self._running = False

class Test(object):

    @dataclass
    class ExePathBundle():
        """Class for storing executable paths for dgrep tester"""
        dgrep_exe_path: str
        logservent_exe_path: str
        grep_cmd_path: str
        sort_cmd_path: str
        diff_cmd_path: str


    def __init__(self,
                 name: str,
                 log_size: int,
                 dist: List[List[float]],
                 qfreq: float,
                 query: str | None = None):
        """
        This class represents a test for distributed grep. Test generates 
          a log of randomly generated ASCII text in which a query occurs
          with a given frequency and shards the log according to the given
          distribution.

        Parameters:
        name (str):                       The name of the test
        log_size (int):                   The size of the log in bytes
        distribution (List[List[float]]): The proportion of the full log in each shard for
                                            each machine
                                            E.g. [[0.1], [0.4, 0.2], [0.3]]
                                          Distribution must sum to 1.0 and each element
                                            must be a float between 0.0 and 1.0
        query_frequency (float):          The frequency of the search term in the log
                                            must be a float between 0.0 and 1.0
        query (str | None):               The query to use with dgrep. Any regex valid for
                                            grep is valid here. Defaults to the query with
                                            query_frequency in the log file
        """
        self._name = name
        self._log_size = log_size
        if sum(sum(d) for d in dist) - 1.0 >= 1e-10 or\
                not all([all(type(e) == float and e >= 0.0 and e <= 1.0 for e in d)]\
                  for d in dist):
            raise ValueError("Distribution must sum to 1.0 and all elements must be floats between 0.0 and 1.0")
        self._dist = dist
        self._qfreq = qfreq
        self._query_bytes = bytearray(b'abc123')
        self._max_word_size = 24
        self._dgrep_query = query if query else self._query_bytes.decode('ascii')
        self._start_port_num = 1234

        # Populated during setup
        self._exe_path_bundle = None
        self._tmpdir = None
        self._expected_filepath = None
        self._dgrep_out_filepath = None
        self._machine_log_dirs = []
        self._machine_log_filepaths = []
        self._dgrep_proc = None
        self._logservent_procs = []

    def get_name(self):
        return self._name

    def _setup(self):
        self._tmpdir = tempfile.mkdtemp()
        self._make_logs()
        self._calc_expected_output()

    def _teardown(self, rm_tmpdir: bool = True):
        assert self._tmpdir

        if rm_tmpdir:
            shutil.rmtree(self._tmpdir)
        else:
            print(self._tmpdir)

        if self._dgrep_proc:
            self._dgrep_proc.force_kill()
        for proc in self._logservent_procs:
            proc.force_kill()

    def _build_log_word(self) -> bytearray:
        word = self._query_bytes
        while word == self._query_bytes:
            word = bytearray(''.join(random.choices(string.ascii_letters + string.digits,
                                                    k=random.randint(0, self._max_word_size))),
                             encoding='ascii')
        return word

    def _make_logs(self):
        q_timer_start = int(1 / self._qfreq)
        q_timer = q_timer_start

        for i, machine_dist in enumerate(self._dist):
            machine_dir = os.path.join(self._tmpdir, f'machine{i}')
            os.mkdir(machine_dir)
            self._machine_log_dirs.append(machine_dir)

            for j,d in enumerate(machine_dist):
                bytes_written = 0
                file_bytes = int(self._log_size * d)
                log_path = os.path.join(machine_dir, f'log{j}.log')
                self._machine_log_filepaths.append(log_path)

                with open(log_path, "+wb") as machine_logfile:
                    while bytes_written < file_bytes:
                        if q_timer <= 0:
                            to_write = self._query_bytes
                            q_timer = q_timer_start
                        else:
                            word = self._build_log_word()
                            punct = bytearray(random.choice(string.punctuation +\
                                                            string.whitespace),
                                              encoding='ascii')
                            to_write = word + punct
                            bytes_written += len(to_write)
                            q_timer -= len(to_write)

                        machine_logfile.write(to_write)

    def _calc_expected_output(self):
        assert self._dgrep_query
        assert self._exe_path_bundle

        grep_proc = Process([self._exe_path_bundle.grep_cmd_path,
                             '-e',
                             self._dgrep_query,
                             '-h',
                             *self._machine_log_filepaths])
        self._expected_filepath = os.path.join(self._tmpdir, "expected.txt")
        grep_proc.spawn(self._expected_filepath)
        retcode = grep_proc.wait(5)

        if retcode is None or retcode > 1:
            grep_proc.force_kill()
            raise RuntimeError(f"Failed waiting on grep calculating expected output for test {self._name} got retcode {retcode}")

        sort_proc = Process([self._exe_path_bundle.sort_cmd_path,
                             self._expected_filepath,
                             '-o',
                             self._expected_filepath])
        sort_proc.spawn()
        retcode = sort_proc.wait(5)

        if retcode is None or retcode != 0:
            sort_proc.force_kill()
            raise RuntimeError(f"Failed waiting on sort calculating expected output for test {self._name} got retcode {retcode}")

    def _sort_and_diff_output(self):
        assert self._expected_filepath
        assert self._dgrep_out_filepath
        assert self._exe_path_bundle

        sort_proc = Process([self._exe_path_bundle.sort_cmd_path,
                             self._dgrep_out_filepath,
                             '-o',
                             self._dgrep_out_filepath])
        sort_proc.spawn()
        retcode = sort_proc.wait(5)

        if retcode is None or retcode != 0:
            sort_proc.force_kill()
            raise RuntimeError(f"Failed sorting dgrep output for test {self._name}")

        diff_proc = Process([self._exe_path_bundle.diff_cmd_path,
                             self._expected_filepath,
                             self._dgrep_out_filepath])
        diff_proc.spawn()
        retcode = diff_proc.wait(5)

        if retcode is None:
            diff_proc.force_kill()
            raise RuntimeError(f"Failed diffing expected and actual for test {self._name}")

        return retcode == 0

    def run(self, exe_path_bundle: ExePathBundle):
        self._exe_path_bundle = exe_path_bundle

        self._setup()
        # "Usage: %s [-d <log directory>] [-v <integer debug level>] <port> <machine file>\n"

        try:
            for i,logdir in enumerate(self._machine_log_dirs):
                machine_filepath = os.path.join(self._tmpdir, f"machine{i}.txt")

                with open(machine_filepath, "w") as mfile:
                    for j in range(len(self._machine_log_dirs)):
                        if j != i:
                            mfile.write(f"localhost:{self._start_port_num + j}\n")

                logservent_proc = Process([self._exe_path_bundle.logservent_exe_path,
                                           "-d",
                                           logdir,
                                           "-v",
                                           "0",
                                           str(self._start_port_num + i),
                                           machine_filepath])
                logservent_proc.spawn()
                self._logservent_procs.append(logservent_proc)

            # "Usage: %s [-a <log server address>] <query string>\n"
            self._dgrep_proc = Process([self._exe_path_bundle.dgrep_exe_path,
                                        "-a",
                                        "localhost:" + str(self._start_port_num),
                                        self._dgrep_query])

            time.sleep(0.5)

            self._dgrep_out_filepath = os.path.join(self._tmpdir, "dgrep_out.txt")
            self._dgrep_proc.spawn(self._dgrep_out_filepath)

            retcode = self._dgrep_proc.wait(10)
            if retcode is None or retcode != 0:
                raise RuntimeError(f"Failed waiting on dgrep for test {self._name} got retcode {retcode}")

            for logservent_proc in self._logservent_procs:
                logservent_proc.send_sigint()
                retcode = logservent_proc.wait(1)
                if retcode is None:
                    raise RuntimeError(f"Failed waiting on logservent for test {self._name} got retcode {retcode}")

            return self._sort_and_diff_output()
        finally:
            self._teardown(rm_tmpdir=True)

def find_command_paths():
    return (subprocess.check_output(['which', 'grep']).strip().decode('utf-8'),
            subprocess.check_output(['which', 'sort']).strip().decode('utf-8'),
            subprocess.check_output(['which', 'diff']).strip().decode('utf-8'))

def main():
    args = parser.parse_args()

    tests = [
        Test("test_one_machine_basic_small",
             256,
             [[1.0]],
             0.25),
        Test("test_one_machine_basic_large",
             50000,
             [[1.0]],
             0.25),
        Test("test_one_machine_sharded_small",
             1024,
             [[0.25, 0.25, 0.25, 0.25]],
             0.25),
        Test("test_one_machine_sharded_large",
             50000,
             [[0.25, 0.25, 0.25, 0.25]],
             0.25),
        Test("test_four_machines_basic_small",
             1024,
             [[0.25], [0.25], [0.25], [0.25]],
             0.25),
        Test("test_four_machines_sharded_large",
             128000,
             [[0.1, 0.05, 0.05], [0.2, 0.05], [0.05], [0.3, 0.2]],
             0.25),
        Test("test_seventeen_machines_sharded_large_infrequent",
             1024000,
             [[0.01 for _ in range(10)],       # 1) 0.10 (0.10)
              [0.013, 0.027],                  # 2) 0.04 (0.14)
              [0.02 for _ in range(3)],        # 3) 0.06 (0.20)
              [0.001 for _ in range(10)],      # 4) 0.01 (0.21)
              [0.1],                           # 5) 0.10 (0.31)
              [0.03 for _ in range(3)],        # 6) 0.09 (0.40)
              [0.017, 0.013],                  # 7) 0.03 (0.43)
              [0.01 for _ in range(3)],        # 8) 0.03 (0.46)
              [0.002 for _ in range(5)],       # 9) 0.01 (0.47)
              [0.03],                          # 10) 0.03 (0.50)
              [0.00001 for _ in range(1000)],  # 11) 0.01 (0.51)
              [0.001 for _ in range(90)],      # 12) 0.09 (0.60)
              [0.01 for _ in range(7)],        # 13) 0.07 (0.67)
              [0.25],                          # 14) 0.25 (0.92)
              [0.01],                          # 15) 0.01 (0.93)
              [0.00001 for _ in range(1000)],  # 16) 0.01 (0.94)
              [0.0001 for _ in range(600)]],   # 17) 0.06 (1.00)
             0.01),
        Test("test_seventeen_machines_sharded_large_frequent",
             1024000,
             [[0.01 for _ in range(10)],       # 1) 0.10 (0.10)
              [0.013, 0.027],                  # 2) 0.04 (0.14)
              [0.02 for _ in range(3)],        # 3) 0.06 (0.20)
              [0.001 for _ in range(10)],      # 4) 0.01 (0.21)
              [0.1],                           # 5) 0.10 (0.31)
              [0.03 for _ in range(3)],        # 6) 0.09 (0.40)
              [0.017, 0.013],                  # 7) 0.03 (0.43)
              [0.01 for _ in range(3)],        # 8) 0.03 (0.46)
              [0.002 for _ in range(5)],       # 9) 0.01 (0.47)
              [0.03],                          # 10) 0.03 (0.50)
              [0.00001 for _ in range(1000)],  # 11) 0.01 (0.51)
              [0.001 for _ in range(90)],      # 12) 0.09 (0.60)
              [0.01 for _ in range(7)],        # 13) 0.07 (0.67)
              [0.25],                          # 14) 0.25 (0.92)
              [0.01],                          # 15) 0.01 (0.93)
              [0.00001 for _ in range(1000)],  # 16) 0.01 (0.94)
              [0.0001 for _ in range(600)]],   # 17) 0.06 (1.00)
             0.1),
        Test("test_seventeen_machines_sharded_large_very_frequent",
             1024000,
             [[0.01 for _ in range(10)],       # 1) 0.10 (0.10)
              [0.013, 0.027],                  # 2) 0.04 (0.14)
              [0.02 for _ in range(3)],        # 3) 0.06 (0.20)
              [0.001 for _ in range(10)],      # 4) 0.01 (0.21)
              [0.1],                           # 5) 0.10 (0.31)
              [0.03 for _ in range(3)],        # 6) 0.09 (0.40)
              [0.017, 0.013],                  # 7) 0.03 (0.43)
              [0.01 for _ in range(3)],        # 8) 0.03 (0.46)
              [0.002 for _ in range(5)],       # 9) 0.01 (0.47)
              [0.03],                          # 10) 0.03 (0.50)
              [0.00001 for _ in range(1000)],  # 11) 0.01 (0.51)
              [0.001 for _ in range(90)],      # 12) 0.09 (0.60)
              [0.01 for _ in range(7)],        # 13) 0.07 (0.67)
              [0.25],                          # 14) 0.25 (0.92)
              [0.01],                          # 15) 0.01 (0.93)
              [0.00001 for _ in range(1000)],  # 16) 0.01 (0.94)
              [0.0001 for _ in range(600)]],   # 17) 0.06 (1.00)
             0.6)
    ]

    grep_cmd_path, sort_cmd_path, diff_cmd_path = find_command_paths()
    exe_path_bundle = Test.ExePathBundle(args.dgrep_executable,
                                         args.logservent_executable,
                                         grep_cmd_path,
                                         sort_cmd_path,
                                         diff_cmd_path)
    num_passed = 0
    for test in tests:
        try:
            if not test.run(exe_path_bundle):
                print(f"Failed test: {test.get_name()}")
            else:
                num_passed += 1
        except RuntimeError as e:
            print(f"Test runtime error: {test.get_name()}, {e}")
        except Exception as e:
            print(f"Test other error: {test.get_name()}, {e}")

    print(f"Passed {num_passed} of {len(tests)} tests")
    if num_passed != len(tests):
        sys.exit(1)

if __name__ == "__main__":
    main()
