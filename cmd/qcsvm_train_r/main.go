package main

import (
	"github.com/alexflint/go-arg"
	"log"
	"github.com/ewalker544/libsvm-go"
	"fmt"
	"math"
	"runtime"
)

type args struct {
	FeatureFile string `arg:"help:File containing features.,required"`
	ModelFile   string `arg:"help:File to output model to.,required"`
}

func (args) Version() string {
	return "Query Chain SVM (qcsvm) 23.Jan.2018"
}

func (args) Description() string {
	return `Train an SVM model for predicting query chain transformations.`
}

type param struct {
	begin float64
	end   float64
	step  float64
}

var (
	C     = param{-5, 15, 0.5}
	Gamma = param{3, -15, -0.5}
)

func setParam(c, gamma float64, problem *libSvm.Parameter) {
	problem.C = c
	problem.Gamma = gamma
}

func main() {
	// Parse the command line arguments.
	var args args
	arg.MustParse(&args)

	param := libSvm.NewParameter()
	param.SvmType = libSvm.C_SVC
	param.KernelType = libSvm.RBF
	param.NumCPU = runtime.NumCPU()

	problem, err := libSvm.NewProblem(args.FeatureFile, param)
	if err != nil {
		log.Fatal(err)
	}

	bestMSQ := math.Inf(1)
	bestC, bestGamma := 0.0, 0.0

	for c := C.begin; c < C.end; c += C.step {
		for gamma := Gamma.end; gamma < Gamma.begin; gamma -= Gamma.step {
			setParam(c, gamma, param)

			fmt.Printf("C: %v Gamma: %v\n", param.C, param.Gamma)

			targets := libSvm.CrossValidation(problem, param, 5)
			squareErr := libSvm.NewSquareErrorComputer()

			var i = 0
			for problem.Begin(); !problem.Done(); problem.Next() {
				y, _ := problem.GetLine()
				v := targets[i]
				squareErr.Sum(v, y)
				i++
			}

			fmt.Printf("Cross Validation Mean squared error = %.6g\n", squareErr.MeanSquareError())
			fmt.Printf("Cross Validation Squared correlation coefficient = %.6g\n", squareErr.SquareCorrelationCoeff())
			if squareErr.MeanSquareError() < bestMSQ {
				bestMSQ = squareErr.MeanSquareError()
				bestC = c
				bestGamma = gamma
			}
		}
	}

	param.C = bestC
	param.Gamma = bestGamma

	fmt.Printf("best msq: %v best c: %v best gamma: %v\n", bestMSQ, param.C, param.Gamma)
	fmt.Printf("best msq: %v best c: %v best gamma: %v\n", bestMSQ, bestC, bestGamma)

	model := libSvm.NewModel(param)
	model.Train(problem)

	fmt.Println(model.Predict(map[int]float64{0: 2.0, 1: 1.0}))

	err = model.Dump(args.ModelFile)
	if err != nil {
		log.Fatal(err)
	}

}
