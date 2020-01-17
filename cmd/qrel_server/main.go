package main

import (
	"github.com/alexflint/go-arg"
	"github.com/hscells/groove/cmd/qrel_server/qrelrpc"
	"github.com/hscells/trecresults"
	"log"
	"net"
	"net/rpc"
	"os"
)

type args struct {
	QrelsFile string `arg:"required,positional" help:"path to qrels file to host"`
}

func (args) Version() string {
	return "qrel_server 17.Jan.2020"
}

func (args) Description() string {
	return `qrels server for fast access to relevance assessments`
}

type QrelsRPC struct {
	Qrels trecresults.QrelsFile
}

func (e *QrelsRPC) GetQrels(topic string, resp *qrelrpc.Response) error {
	q := make(map[string]trecresults.Qrels)
	q[topic] = e.Qrels.Qrels[topic]
	f := trecresults.QrelsFile{
		Qrels: q,
	}
	resp.Qrels = f
	return nil
}

func main() {
	var args args
	arg.MustParse(&args)

	log.Println("initialising server...")
	addy, err := net.ResolveTCPAddr("tcp", "0.0.0.0:8004")
	if err != nil {
		panic(err)
	}

	inbound, err := net.ListenTCP("tcp", addy)
	if err != nil {
		panic(err)
	}

	q, err := os.OpenFile(args.QrelsFile, os.O_RDONLY, 0664)
	if err != nil {
		log.Fatalln(err)
	}
	qrels, err := trecresults.QrelsFromReader(q)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println(len(qrels.Qrels))

	log.Println("registering listener...")
	listener := new(QrelsRPC)
	x := QrelsRPC{
		Qrels: qrels,
	}

	listener = &x
	err = rpc.Register(listener)
	if err != nil {
		panic(err)
	}
	log.Println("ready to go!")
	rpc.Accept(inbound)

}
