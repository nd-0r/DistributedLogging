package main

import (
  "fmt"
  "flag"
  "log"
  "os"
  "net"
  "net/rpc"
  "net/http"
  "bufio"
  "sync"
  "strings"
  "andreworals.com/dlogger/leveled_logger"
  "andreworals.com/dlogger/chan_grep"
  "github.com/hsfzxjy/go-srpc"
)

// Debug logging

// Global application state

var logger leveled_logger.LeveledLogger

var logDir = flag.String("d", "logs", "log directory")
var debugLevel = flag.Int("v", 1, "verbosity level, 0=off 1=normal 2=debug")
var machineAddrs []string

type Nothing struct {}

type LogServerRpc string
type GrepClientRpc string

var rpcPort *string = nil

//==============================================================//

// RPC from queried log server to log server
// Takes output from grep and sends it over a TCP connection to the caller
func (*LogServerRpc) LoggerQuery(args chan_grep.Args, s *srpc.Session) error {
  return srpc.S( func() error {
    resultsCh := make(chan string)
    errCh := make(chan error)

    go chan_grep.GrepLogs(args, *logDir, true, resultsCh, errCh)

    L:
    for {
      select {
      case line, ok := <-resultsCh:
        if !ok {
          break L
        }
        s.PushValue(line)
      case err := <-errCh:
        return err
      }
    }

    return nil
  }, s, nil)
}

func serverReader(wg *sync.WaitGroup, args chan_grep.Args, addrStr string, ch chan<- string) {
  defer wg.Done()

  c, err := rpc.DialHTTP("tcp", addrStr)
  if err != nil {
    logger.PrintfNormal("Could not dial logserver at %s: %s\n", addrStr, err)
    return
  }
  client := srpc.WrapClient(c)

  stream, err := client.CallStream("LogServerRpc.LoggerQuery", args)
  for line := range stream.C() {
    logger.PrintfDebug("Logservent received line from %s\n", addrStr)
    ch <- line.(string)
  }

  logger.PrintfDebug("Logservent receiving from %s done\n", addrStr)
}

func done(wg *sync.WaitGroup, doneCh chan<- Nothing, ch chan<- string) {
  wg.Wait()
  logger.PrintfDebug("done passed wait\n")
  doneCh <- Nothing{}
  // close(ch)
}

// RPC from grep client to query log server
// Calls each log server to retrieve the its logs and populates a channel to send back to the grep client over TCP
func (*GrepClientRpc) GrepQuery(args chan_grep.Args, s *srpc.Session) error {
  logger.PrintfDebug("< %s > received grep query: %s\n", *rpcPort, args.Query)
  return srpc.S( func() error {
    // query the other servers
    var grepResultsCh = make(chan string)
    var doneCh = make(chan Nothing)
    var wg sync.WaitGroup

    // Grep from other servers
    for _, addr := range machineAddrs {
      wg.Add(1)
      go serverReader(&wg, args, addr, grepResultsCh)
    }
    // Grep from this server
    wg.Add(1)
    go func(wgroup *sync.WaitGroup) {
      defer wgroup.Done()
      chan_grep.GrepLogs(args, *logDir, false, grepResultsCh, nil)
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
    str := lines.Text()
    addr_tmp := strings.Split(str, ":")
    if len(addr_tmp) != 2 {
      fmt.Fprintf(os.Stderr, "Invalid address or port: %s\nFormat is <address>:<port>\n", str)
    }
    machineAddrs = append(machineAddrs, str)
  }

  logger.PrintfDebug("Addresses: %v\n", machineAddrs)
}

func usage() {
  fmt.Fprintf(os.Stderr,
              "Usage: %s [-d <log directory>] [-v <integer debug level>] <port> <machine file>\n",
              os.Args[0])
  flag.PrintDefaults()
}

func main() {
  flag.Parse()

  logger.Logger = log.New(os.Stdout, "", log.LstdFlags)
  logger.SetLevel(leveled_logger.LogLevel(*debugLevel))
  defer logger.AutoPrefix("main: ")()

  if flag.NArg() != 2 {
    usage()
    os.Exit(1)
  }
  rpcPort = &flag.Args()[0]
  readMachineFile(flag.Args()[1])

  rpc.Register(new(GrepClientRpc))
  rpc.Register(new(LogServerRpc))
  rpc.HandleHTTP()
  l, e := net.Listen("tcp", ":" + *rpcPort)
  if e != nil {
    fmt.Fprintf(os.Stderr, "listen error: ", e.Error())
    os.Exit(1)
  }

  logger.PrintfDebug("Listening on all interfaces %s\n", rpcPort)

  http.Serve(l, nil)
}

