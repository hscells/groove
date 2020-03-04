package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/alexflint/go-arg"
	"github.com/hscells/boogie"
	"github.com/hscells/cqr"
	"github.com/hscells/groove/cmd/reverb/reverb"
	"github.com/hscells/groove/formulation"
	"github.com/hscells/groove/pipeline"
	"github.com/hscells/trecresults"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"syscall"
)

var (
	name    = "reverb"
	version = "03.Jan.2020"
	author  = "Harry Scells"
)

type args struct {
	Pipeline     string   `help:"Path to boogie experimental pipeline file"`
	Port         string   `help:"Port to run server on" arg:"-p"`
	Hosts        []string `help:"When in client mode, list of reverb servers to distribute the pipeline across" arg:"-s,separate"`
	Mode         string   `help:"Mode to run reverb in [client/server]" arg:"required,positional"`
	TemplateArgs []string `help:"Additional arguments to pass to experimental pipeline file" arg:"positional"`
}

func (args) Version() string {
	return version
}

func (args) Description() string {
	return fmt.Sprintf(`
                        __ 
  _______ _  _____ ____/ / 
 / __/ -_) |/ / -_) __/ _ \
/_/  \__/|___/\__/_/ /_.__/

%s
@ %s
# %s
`, name, author, version)
}

type Reverb struct{}

func (r *Reverb) Execute(dsl boogie.Pipeline, resp *reverb.Response) error {
	log.Println("received pipeline request")
	g, err := boogie.CreatePipeline(dsl)
	if err != nil {
		log.Println(err)
		return err
	}
	log.Println("successfully constructed pipeline, executing...")

	c := make(chan pipeline.Result)
	go g.Execute(c)
	for result := range c {
		resp.Results = append(resp.Results, result)
	}

	log.Println("experiments completed!")
	fmt.Print(args{}.Description())
	fmt.Println("[ready]")
	return nil
}

func main() {
	var args args
	arg.MustParse(&args)

	gob.Register(os.PathError{})
	gob.Register(syscall.Errno(0))
	gob.Register(map[string]interface{}{})
	gob.Register(cqr.BooleanQuery{})
	gob.Register(cqr.Keyword{})
	gob.Register(trecresults.Qrels{})
	gob.Register(trecresults.Qrel{})
	gob.Register(trecresults.ResultFile{})
	gob.Register(trecresults.ResultList{})
	gob.Register(trecresults.Result{})
	gob.Register(pipeline.SupplementalData{})
	gob.Register(pipeline.Data{})
	gob.Register(formulation.QueryCategory(0))

	fmt.Print(args.Description())

	if args.Mode == "server" {
		fmt.Println("[server mode]")

		addr, err := net.ResolveTCPAddr("tcp", "0.0.0.0:"+args.Port)
		if err != nil {
			panic(err)
		}

		inbound, err := net.ListenTCP("tcp", addr)
		if err != nil {
			panic(err)
		}

		listener := new(Reverb)
		err = rpc.Register(listener)
		if err != nil {
			panic(err)
		}
		log.Println("ready to go!")
		log.Println("port: ", args.Port)
		rpc.Accept(inbound)
	} else if args.Mode == "client" {
		fmt.Println("[client mode]")

		// Read the contents of the dsl file.
		b, err := ioutil.ReadFile(args.Pipeline)
		if err != nil {
			panic(err)
		}

		// Parse the dsl file into a struct.
		dsl, err := boogie.Template(bytes.NewBuffer(b), args.TemplateArgs...)
		if err != nil {
			panic(err)
		}

		reverb.Execute(dsl, args.Hosts...)
	}
}
