package main

import (
	"github.com/hscells/boogie"
	"github.com/hscells/groove/cmd/reverb/reverb"
	"github.com/hscells/groove/pipeline"
	"log"
	"net"
	"net/rpc"
)

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
	return nil
}

func main() {

	addr, err := net.ResolveTCPAddr("tcp", "0.0.0.0:8005")
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
	rpc.Accept(inbound)
}
