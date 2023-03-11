package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"cse224/proj5/pkg/surfstore"

	"google.golang.org/grpc"
)

// Usage String
const USAGE_STRING = "./run-server.sh -s <service_type> -p <port> -l -d (blockStoreAddr*)"

// Set of valid services
var SERVICE_TYPES = map[string]bool{"meta": true, "block": true, "both": true}

// Exit codes
const EX_USAGE int = 64

func main() {
	// Custom flag Usage message
	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "Usage of %s:\n", USAGE_STRING)
		flag.VisitAll(func(f *flag.Flag) {
			fmt.Fprintf(w, "  -%s: %v\n", f.Name, f.Usage)
		})
		fmt.Fprintf(w, "  (blockStoreAddr*): BlockStore Address (include self if service type is both)\n")
	}

	// Parse command-line argument flags
	service := flag.String("s", "", "(required) Service Type of the Server: meta, block, both")
	port := flag.Int("p", 8080, "(default = 8080) Port to accept connections")
	localOnly := flag.Bool("l", false, "Only listen on localhost")
	debug := flag.Bool("d", true, "Output log statements")
	flag.Parse()

	// Use tail arguments to hold BlockStore address
	args := flag.Args()
	blockStoreAddrs := []string{}

	for i:=0; i < len(args); i++{
		blockStoreAddrs = append(blockStoreAddrs, args[i])
	}
	
	// Valid service type argument
	if _, ok := SERVICE_TYPES[strings.ToLower(*service)]; !ok {
		flag.Usage()
		os.Exit(EX_USAGE)
	}

	// Add localhost if necessary
	addr := ""
	if *localOnly {
		addr += "localhost"
	}
	addr += ":" + strconv.Itoa(*port)

	// Disable log outputs if debug flag is missing
	if !(*debug) {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}
	log.Println("addr:",addr," service:",service, "blockStoreAddrs:", blockStoreAddrs)
	log.Fatal(startServer(addr, strings.ToLower(*service), blockStoreAddrs))
}

func startServer(hostAddr string, serviceType string, blockStoreAddrs []string) error {
	log.Println("Starting servers")
	//Step 1 : Create a new Server
	grpcServer := grpc.NewServer()

	//Step 2: Register rpc services
	if serviceType == "block" || serviceType == "both" {
		surfstore.RegisterBlockStoreServer(grpcServer, surfstore.NewBlockStore())
		log.Println("Server started for Block on: ", hostAddr )
	}
	if serviceType == "meta" || serviceType == "both" {
		surfstore.RegisterMetaStoreServer(grpcServer, surfstore.NewMetaStore(blockStoreAddrs))
		log.Println("Server started for Meta on: ", hostAddr )
	}

	//Step 3: start listening on hostAddr
	l, e := net.Listen("tcp", hostAddr)
	if e != nil {
		return e
	}
	
	return grpcServer.Serve(l)
}
