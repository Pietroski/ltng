package main

import (
	"log"
	"sync"
)

var (
	wg sync.WaitGroup

	counterChan = make(chan int, 1)
)

func main() {

	wg.Add(1)
	go func() {
		defer wg.Done()
		counterChan <- 20
		//time.Sleep(time.Second * 1)

		//counter:
		//	for {
		//		select {
		//		case n := <-counterChan:
		//			log.Println("counter ->", n)
		//			if n > 10 {
		//				n--
		//				counterChan <- n
		//				continue
		//			}
		//
		//			break counter
		//		}
		//	}
	}()

counter:
	for {
		select {
		case n := <-counterChan:
			log.Println("counter ->", n)
			if n > 10 {
				n--
				counterChan <- n
				continue
			}

			break counter
		}
	}

	// close(counterChan)
	wg.Wait()
	log.Println("finished")
}
