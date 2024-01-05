package tools



type Queue []int 

func (q *Queue) Push(i int) {
	*q = append(*q, i)
}

func (q *Queue) Pop() int {
	head := (*q)[0]
	*q = (*q)[1:]
	return head
}

func (q *Queue) IsEmpty() bool {
	return len(*q) == 0
}

