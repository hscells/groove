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
	var conn sync.WaitGroup
	conn.Add(len(hosts))

	for i, host := range hosts {
		wg.Add(1)
		go func(h string, idx int) {
			d := splitSources(dsl, idx)
			log.Printf("[%s] connecting...\n", h)
			client, err := rpc.Dial("tcp", h)
			if err != nil {
				panic(err)
			}
			log.Printf("[%s] established connection\n", h)
			conn.Done()

			log.Printf("[%s] waiting for responses from other hosts\n", h)
			conn.Wait()
			log.Printf("[%s] executing experiments\n", h)
			var resp Response
			errs <- client.Call("Reverb.Execute", d, &resp)
			for _, result := range resp.Results {
				res <- result
			}
			wg.Done()
			log.Printf("[%s] completed experiments\n", h)
		}(host, i)
	}

	var wg2 sync.WaitGroup
	wg2.Add(1)
	go func() {
		err := boogie.Execute(dsl, res)
		if err != nil {
			// Should probably send a request to
			// all the other hosts to stop execution.
			panic(err)
		}
		log.Println("competed processing results")
		wg2.Done()
	}()

	go func() {
		for err := range errs {
			if err != nil {
				panic(err)
			}
		}
		log.Println("no errors found in experiments")
	}()

	wg.Wait()
	close(errs)
	close(res)
	wg2.Wait()
}

func splitSources(dsl boogie.Pipeline, idx int) boogie.Pipeline {
	if len(dsl.Statistic.Sources) > 0 {
		idx = idx % len(dsl.Statistic.Sources)
		dsl.Statistic.Options = dsl.Statistic.Sources[idx]
	}
	return dsl
}
