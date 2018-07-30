package learning

import (
	"io"
	"os/exec"
	"bufio"
	"log"
	"fmt"
	"strings"
)

type QuickRankQueryCandidateSelector struct {
	// The path to the binary file for execution.
	binary    string
	// Model file used for selection.
	model     string
	// Command-line arguments for configuration.
	arguments map[string]interface{}
}

func (QuickRankQueryCandidateSelector) Select(query TransformedQuery, transformations []CandidateQuery) (TransformedQuery, QueryChainCandidateSelector, error) {
	panic("implement me")
}

func (qr QuickRankQueryCandidateSelector) Train(lfs []LearntFeature) ([]byte, error) {
	// Load the arguments from the map.
	args := make([]string, len(qr.arguments)*2)
	i := 0
	for k, v := range qr.arguments {
		args[i] = fmt.Sprintf("--%s", k)
		args[i+1] = fmt.Sprintf("%v", v)
		i += 2
	}

	// Configure the command.
	cmd := exec.Command(qr.binary, args...)
	fmt.Println(strings.Join(args, " "))

	// Open channels to stdout and stderr.
	r, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	defer r.Close()

	e, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	defer e.Close()

	// Start the command.
	cmd.Start()

	// Output the stdout pipe.
	go func() {
		s := bufio.NewScanner(r)
		for s.Scan() {
			log.Println(s.Text())
		}
		return
	}()

	// Output the stderr pipe.
	go func() {
		s := bufio.NewScanner(e)
		for s.Scan() {
			log.Println(s.Text())
		}
		return
	}()

	// Wait for the command to finish.
	if err := cmd.Wait(); err != nil {
		return nil, err
	}

	return nil, nil
}

func (QuickRankQueryCandidateSelector) Output(lf LearntFeature, w io.Writer) error {
	panic("implement me")
}

func (QuickRankQueryCandidateSelector) StoppingCriteria() bool {
	panic("implement me")
}

func NewQuickRankQueryCandidateSelector(binary string, arguments map[string]interface{}) QuickRankQueryCandidateSelector {
	return QuickRankQueryCandidateSelector{
		binary:    binary,
		arguments: arguments,
	}
}
