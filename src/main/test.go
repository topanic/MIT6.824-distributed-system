package main

import (
	"fmt"

	"6.5840/tools"
)

type A struct {
	queue tools.Queue
}

func main() {
	a := A{}
	a.queue = make(tools.Queue, 0)
	
	for i := 0; i < 10; i++ {
		a.queue.Push(i)
	}

	fmt.Println(a.queue.Pop())
	fmt.Println(a.queue.Pop())
	fmt.Println(a.queue.Pop())
	fmt.Println(a.queue.Pop())

}