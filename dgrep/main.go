package main

import (
  "flag"
  "fmt"
  "log"
  "os"
  "net/rpc"
  "github.com/hsfzxjy/go-srpc"
)

type Args struct {
  Query string
}

const kListenPort = 1233
const kRpcPort = "1234"

var address = flag.String("a", "localhost", "The address of the log servent")

func query(q string, addr string) {
  c, err := rpc.DialHTTP("tcp", addr + ":" + kRpcPort)
  if err != nil {
    log.Fatal("dialing:", err)
  }
  client := srpc.WrapClient(c)

  args := Args {
    q,
  }

  stream, err := client.CallStream("GrepClientRpc.GrepQuery", args)
  if err != nil {
    log.Fatal("CallStream: ", err)
  }

  for line := range stream.C() {
    fmt.Println(line.(string))
  }
}

func main() {
  flag.Parse()

  if flag.NArg() != 1 {
    flag.PrintDefaults()
    os.Exit(1)
  }

  expr := flag.Args()[0]

  query(expr, *address)
}

