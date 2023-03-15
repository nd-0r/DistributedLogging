package chan_grep

import (
  "testing"
  "os"
  "path/filepath"
  "fmt"
  "sort"
)

type grepTest struct {
  Query        string
  FileContents []string
  Expected     []string
  LogsDir      string
}

var tests = []grepTest {
  {
    "",
    []string{},
    []string{},
    "",
  },
  {
    "a",
    []string{},
    []string{},
    "",
  },
  {
    "a",
    []string{"",},
    []string{},
    "",
  },
  {
    "a",
    []string{"", "b\nc", "d\ne\nfgh\n"},
    []string{},
    "",
  },
  {
    "a",
    []string{"a\nb\na\nb\nc\nbd\naba\nb\na\n",},
    []string{"a", "a", "aba", "a"},
    "",
  },
  {
    "a",
    []string{"", "a\nb\nc", "a\nb\nc\nabracadabra\nd", "d\neat\nfgh\n"},
    []string{"a", "a", "abracadabra", "eat"},
    "",
  },
}

func (test *grepTest) setupGrepTest() {
  test.LogsDir = filepath.Join(os.TempDir(), "/test_logs/")
  err := os.Mkdir(test.LogsDir, 0777)
  if err != nil {
    fmt.Fprintf(os.Stderr, "Could not create < %s >: %s\n", test.LogsDir, err)
    os.Exit(1)
  }


  for i, file_contents := range test.FileContents {
    filename := fmt.Sprintf("log%d.log", i)
    err := os.WriteFile(filepath.Join(test.LogsDir, filename), []byte(file_contents), 0777)
    if err != nil {
      fmt.Fprintf(os.Stderr, "Could not create < %s > at < %s >: %s\n", filename, test.LogsDir, err)
      os.Exit(1)
    }
  }
}

func (test *grepTest) teardownGrepTest() {
  err := os.RemoveAll(test.LogsDir)
  if err != nil {
    fmt.Fprintf(os.Stderr, "Could not teardown test logs directory: %s\n", err)
    os.Exit(1)
  }
}

func (test *grepTest) RunGrepTest(t *testing.T) {
  test.setupGrepTest()
  defer test.teardownGrepTest()

  var args = Args{test.Query}
  resultChan := make(chan string)
  errorChan := make(chan error)

  go GrepLogs(args, test.LogsDir, true, resultChan, errorChan)

  var out []string

  L:
  for {
    select {
    case err := <-errorChan:
      t.Error(fmt.Sprintf("Test: %+v\nUnexpected error from GrepLogs: %s\n", test, err))
      return
    case res,ok := <-resultChan:
      if !ok {
        break L
      }
      out = append(out, res)
    }
  }

  if len(out) != len(test.Expected) {
    t.Error(fmt.Sprintf("Test: %+v\nOUT: %+v\nUnexpected result length: GOT %d EXPECTED %d\n", test, out, len(out), len(test.Expected)))
    return
  }

  sort.Strings(out)
  sort.Strings(test.Expected)

  for i := 0; i < len(out); i++ {
    if out[i] != test.Expected[i] {
      t.Error(fmt.Sprintf("Test: %+v\nOUT: %+v\nUnexpected result: GOT %s EXPECTED %s\n", test, out, out[i], test.Expected[i]))
      return
    }
  }
}

func TestGrepLogs(t *testing.T) {
  for _,test := range tests {
    test.RunGrepTest(t)
  }
}

