package reverb

import (
	"github.com/hscells/boogie"
	"github.com/hscells/groove/pipeline"
	"log"
	"net/rpc"
	"sync"
)

func Execute(dsl boogie.Pipeline, hosts ...string) {
	errs := make(chan error)
	res := make(chan pipeline.Result)
	log.Println("executing pipeline with hosts:", len(hosts))

	var wg sync.WaitGroup

	for _, host := range hosts {
		wg.Add(1)
		go func(h string) {
			log.Println("connecting to", h)
			client, err := rpc.Dial("tcp", h)
			if err != nil {
				panic(err)
			}
			log.Println("established connection to", h)
			var resp Response
			errs <- client.Call("Reverb.Execute", dsl, &resp)
			for _, result := range resp.Results {
				res <- result
			}
			wg.Done()
		}(host)
	}

	log.Println("executing results pipeline...")
	go func() {
		err := boogie.Execute(dsl, res)
		if err != nil {
			panic(err)
		}
	}()

	log.Println("accumulating results...")
	go func() {
		for err := range errs {
			if err != nil {
				panic(err)
			}
		}

	}()

	log.Println("waiting for all experiments to complete")

	wg.Wait()
	close(errs)
	close(res)

	log.Println("no errors found in experiments")
}
