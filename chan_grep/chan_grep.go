package chan_grep

import (
  "os"
  "os/exec"
  "path/filepath"
  "bufio"
  "errors"
  "bytes"
)


type Args struct {
  Query    string
}

// mostly from https://cs.opensource.google/go/go/+/refs/tags/go1.20.2:src/bufio/scan.go;l=350
var customLineSplit = func(data []byte, atEOF bool) (advance int, token []byte, err error) {
  if atEOF && len(data) == 0 {
    return 0, nil, nil
  }

  if i := bytes.IndexByte(data, '\n'); i >= 0 {
    // We have a full newline-terminated line.
    return i + 1, data[0:i], nil
  }

  // If we're at EOF, we have a final, non-terminated line. Return it.
  if atEOF {
    return len(data), data, nil
  }

  // Request more data.
  return 0, nil, nil
}

func GrepLogs(args Args,
              logDir string,
              closeResults bool,
              resultsCh chan<- string,
              errCh chan<- error) {
  var errOut error = nil
  defer func() {
    if errOut != nil && errCh != nil {
      errCh <- errOut
    }

    if closeResults {
      close(resultsCh)
    }
  }()

  dirents, err := os.ReadDir(logDir)
  if err != nil {
    errOut = errors.New("Could not read logs: " + err.Error())
    return
  }

  for _, dirent := range dirents {
    grepCmd := exec.Command("grep", "-e", args.Query, filepath.Join(logDir, dirent.Name()))
    grepOut, err := grepCmd.StdoutPipe()

    if err != nil {
      errOut = errors.New("Could not open pipe from grep: " + err.Error())
      return
    }

    if err := grepCmd.Start(); err != nil {
      errOut = errors.New("Could not start grep" + err.Error())
      return
    }

    grepOutScanner := bufio.NewScanner(grepOut)
    grepOutScanner.Split(customLineSplit)
    for grepOutScanner.Scan() {
      toout := grepOutScanner.Text()
      resultsCh <- toout
    }

    if err := grepCmd.Wait(); err != nil {
      if grepCmd.ProcessState.ExitCode() != 1 {
        errOut = errors.New("Failed to wait on grep process: " + err.Error())
        return
      }
    }
  }
}

