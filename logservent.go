package main

import (
  "fmt"
  "flag"
  "log"
  "os"
  "os/exec"
  "net"
  "net/rpc"
  "net/http"
  "bufio"
  "sync"
  "errors"
  "io/ioutil"
)

// Global

const rpcPort = "1234"
const kListenPort = 1235
var logDir = flag.String("d", "logs", "log directory")
var machineAddrs []string

type Nothing struct {}

type LogServerRpc Nothing
type GrepClientRpc Nothing

type Args struct {
  TcpAddr  net.Addr
  Query    string
}

//==============================================================//

// RPC from queried log server to log server
// Takes output from grep and sends it over a TCP connection to the caller
func (_ *LogServerRpc) LoggerQuery(args Args, reply *Nothing) error {
  conn, err := net.Dial("tcp", args.TcpAddr.String())
  defer conn.Close()
  if err != nil {
    return errors.New("Could not call back: " + err.Error())
  }

  files, err := ioutil.ReadDir(*logDir)
  if err != nil {
    return errors.New("Could not read logs: " + err.Error())
  }

  for _, file := range files {
    grepCmd := exec.Command("grep", "-e", args.Query, file.Name())
    grepOut, err := grepCmd.StdoutPipe()

    if err != nil {
      return errors.New("Could not open pipe from grep: " + err.Error())
    }
    if err := grepCmd.Start(); err != nil {
      return errors.New("Could not start grep" + err.Error())
    }
    if err != nil {
      continue
    }

    buf := make([]byte, 1024)
    for {
      n, err := grepOut.Read(buf)
      if err != nil {
        break
      }
      conn.Write(buf[:n])
    }
  }

  return nil
}

func serverReader(wg sync.WaitGroup, args Args, addrStr string, listenPort string, ch chan<- string) {
  wg.Add(1)
  defer wg.Done()
  client, err := rpc.DialHTTP("tcp", addrStr + ":" + rpcPort)
  if err != nil {
    return
  }

  listener, err := net.Listen("tcp", "localhost:" + listenPort)
  serverConnCh := make(chan net.Conn)
  go func() {
    conn, err := listener.Accept()
    if err != nil {
      close(serverConnCh)
    }
    serverConnCh <- conn
  }()

  doneCh := client.Go("LogServerRpc.LoggerQuery", args, nil, nil)

  serverConn := <-serverConnCh
  if serverConn == nil {
    return
  }
  input := bufio.NewScanner(serverConn) // use default ScanLines

  for input.Scan() {
    ch <- input.Text()
  }

  <-doneCh.Done
}

func done(wg sync.WaitGroup, doneCh chan Nothing) {
  wg.Wait()
  doneCh <- Nothing{}
}

// spawn 1 of these to send to the grep client
func clientWriter(clientConn net.Conn, ch <-chan string, doneCh <-chan Nothing) {
  for {
    select {
    case <-doneCh:
      for line := range ch {
        fmt.Fprintln(clientConn, line) // TODO ignoring errors
      }
      break
    case line := <-ch:
      fmt.Fprintln(clientConn, line) // TODO ignoring errors
    }
  }
}

// RPC from grep client to query log server
// Calls each log server to retrieve the its logs and populates a channel to send back to the grep client over TCP
func (_ *GrepClientRpc) GrepQuery(args Args, reply *Nothing) error {
  // query the other servers
  var grepResultsCh = make(chan string)
  var doneCh = make(chan Nothing)
  var wg sync.WaitGroup

  for i, addrStr := range machineAddrs {
    go serverReader(wg, args, addrStr, fmt.Sprintf("%s", kListenPort + i), grepResultsCh)
  }

  conn, err := net.Dial("tcp", args.TcpAddr.String())
  if err != nil {
    return errors.New("Could not call back: " + err.Error())
  }

  go clientWriter(conn, grepResultsCh, doneCh)
  done(wg, doneCh)

  return nil
}

func readMachineFile(fileName string) {
  file, err := os.Open(fileName)
  if err != nil {
    log.Fatal("Could not open machine file")
  }

  lines := bufio.NewScanner(file) // use default ScanLines
  for lines.Scan() {
    machineAddrs = append(machineAddrs, lines.Text())
  }
}

func usage() {
  fmt.Fprintf(os.Stderr, "Usage: %s [-d <log directory>] <machine file>\n", os.Args[0])
  flag.PrintDefaults()
}

func main() {
  flag.Parse()
  if flag.NArg() != 1 {
    usage()
    os.Exit(1)
  }

  readMachineFile(flag.Args()[0])

  grepClientRpc := new(GrepClientRpc)
  logServerRpc := new(LogServerRpc)

  rpc.Register(grepClientRpc)
  rpc.Register(logServerRpc)
  rpc.HandleHTTP()
  l, e := net.Listen("tcp", ":" + rpcPort)
  if e != nil {
    log.Fatal("listen error:", e)
  }
  http.Serve(l, nil)
}
