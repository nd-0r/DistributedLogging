package main

import (
  "fmt"
  "flag"
  "log"
  "os"
  "os/exec"
  "path/filepath"
  "net"
  "net/rpc"
  "net/http"
  "bufio"
  "sync"
  "errors"
  "andreworals.com/dlogger/leveled_logger"
  "andreworals.com/dlogger/chan_grep"
  "github.com/hsfzxjy/go-srpc"
)

// Debug logging

// Global application state

var logger leveled_logger.LeveledLogger

const rpcPort = "1234"
const kListenPort = 1235
var logDir = flag.String("d", "logs", "log directory")
var debugLevel = flag.Int("v", 1, "verbosity level, 0=off 1=normal 2=debug")
var machineAddrs []string

type Nothing struct {}

type LogServerRpc string
type GrepClientRpc string

//==============================================================//

// RPC from queried log server to log server
// Takes output from grep and sends it over a TCP connection to the caller
func (*LogServerRpc) LoggerQuery(args Args, s *srpc.Session) error {
  return srpc.S( func() error {
    resultsCh := make(chan string)
    errCh := make(chan error)

    go grepLogs(args, *logDir, resultsCh, errCh)

    for {
      select {
      case line := <-resultsCh:
        s.PushValue(line)
      case err := <-errCh:
        return err
      }
    }

    return nil
  }, s, nil)
}

func serverReader(wg *sync.WaitGroup, args Args, addrStr string, listenPort int, ch chan<- string) {
  defer wg.Done()

  c, err := rpc.DialHTTP("tcp", addrStr + ":" + rpcPort)
  if err != nil {
    return
  }
  client := srpc.WrapClient(c)

  stream, err := client.CallStream("LogServerRpc.LoggerQuery", args)
  for line := range stream.C() {
    ch <- line.(string)
  }
}

func done(wg *sync.WaitGroup, doneCh chan<- Nothing, ch chan<- string) {
  wg.Wait()
  logger.PrintfDebug("done passed wait\n")
  doneCh <- Nothing{}
  close(ch)
}

// RPC from grep client to query log server
// Calls each log server to retrieve the its logs and populates a channel to send back to the grep client over TCP
func (*GrepClientRpc) GrepQuery(args Args, s *srpc.Session) error {
  logger.PrintfDebug("Received grep query: %s\n", args.Query)
  return srpc.S( func() error {
    // query the other servers
    var grepResultsCh = make(chan string)
    var doneCh = make(chan Nothing)
    var wg sync.WaitGroup

    // Grep from other servers
    for i, addrStr := range machineAddrs {
      wg.Add(1)
      go serverReader(&wg, args, addrStr, kListenPort + i, grepResultsCh)
    }
    // Grep from this server
    wg.Add(1)
    go func(wgroup *sync.WaitGroup) {
      defer wgroup.Done()
      grepLogs(args, grepResultsCh, nil)
      logger.PrintfDebug("grepLogs done\n")
    }(&wg)
    // Call back when the grep calls return
    go done(&wg, doneCh, grepResultsCh)

    for {
      select {
      case <-doneCh:
        logger.PrintfDebug("Done receiving results\n");
        return nil
      case line := <-grepResultsCh:
        s.PushValue(line)
      }
    }

    return nil
  }, s, nil)
}

func readMachineFile(fileName string) {
  defer logger.AutoPrefix("readMachineFile: ")()

  file, err := os.Open(fileName)
  if err != nil {
    fmt.Fprintf(os.Stderr, "Could not open machine file: " + err.Error())
    usage()
    os.Exit(1)
  }

  lines := bufio.NewScanner(file) // use default ScanLines
  for lines.Scan() {
    machineAddrs = append(machineAddrs, lines.Text())
  }

  logger.PrintfDebug("Addresses: %v\n", machineAddrs)
}

func usage() {
  fmt.Fprintf(os.Stderr,
              "Usage: %s [-d <log directory>] [-v <integer debug level>] <machine file>\n",
              os.Args[0])
  flag.PrintDefaults()
}

func main() {
  flag.Parse()

  logger.Logger = log.New(os.Stdout, "", log.LstdFlags)
  logger.SetLevel(leveled_logger.LogLevel(*debugLevel))
  defer logger.AutoPrefix("main: ")()

  if flag.NArg() == 1 {
    readMachineFile(flag.Args()[0])
  }

  rpc.Register(new(GrepClientRpc))
  rpc.Register(new(LogServerRpc))
  rpc.HandleHTTP()
  l, e := net.Listen("tcp", ":" + rpcPort)
  if e != nil {
    fmt.Fprintf(os.Stderr, "listen error: ", e.Error())
    os.Exit(1)
  }

  logger.PrintfDebug("Listening on all interfaces %s\n", rpcPort)

  http.Serve(l, nil)
}

