package chan_grep

import (
  "os"
  "os/exec"
  "path/filepath"
  "bufio"
  "errors"
)


type Args struct {
  Query    string
}

func GrepLogs(args Args, logDir string, resultsCh chan<- string, errCh chan<- error) {
  var errOut error = nil
  defer func() {
    if errOut != nil && errCh != nil {
      errCh <- errOut
    }
    close(resultsCh)
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

