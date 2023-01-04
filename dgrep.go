package main

import (
  "flag"
  "fmt"
  "os"
  "net"
  "net/http"
  "bufio"
)

type Args struct {
  TcpAddr  net.TCPAddr
  Query    string
}

const kListenPort = 1233
const kRpcPort = "1234"

var address = flag.String("a", "localhost", "The address of the log servent")

func query(q string, addr string) {
  conn, err := net.Dial("tcp", addr + ":" + kRpcPort)
  defer conn.Close()
  if err != nil {
    log.Fatal("dialing:", err)
  }
  client := rpc.NewClient(conn)

  locAddr := conn.LocalAddr().(*net.TCPAddr)
  locAddr.Port = kListenPort
  args := Args {
    locAddr,
    q,
  }

  listener, err := net.ListenTCP("tcp", &args.TcpAddr)
  serverConnCh := make(chan net.Conn)
  go func() {
    serverConn, err := listener.Accept()
    if err != nil {
      close(serverConnCh)
    }
    serverConnCh <- serverConn
  }()

  doneCh := client.Go("GrepClientRpc.GrepQuery", args, nil, nil)

  serverConn := <-serverConnCh
  if serverConn == nil {
    return
  }
  defer serverConn.Close()
  input := bufio.NewScanner(serverConn)

  for input.Scan() {
    fmt.Println(input.Text())
  }

  <-doneCh.Done
}

func main() {
  flag.Parse()

  if flag.NArgs() != 1 {
    flag.PrintUsage()
    os.Exit(1)
  }

  expr := flag.Args()[0]

  query(expr, address)
}
