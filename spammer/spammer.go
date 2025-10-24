package main

import (
	"fmt"
	"log"
	"sort"
	"sync"
)

const (
	countWorkers = 5
	chanBuf      = 8
)

func RunPipeline(cmds ...cmd) {
	var wg sync.WaitGroup
	wg.Add(len(cmds))
	var in chan interface{}

	for _, f := range cmds {
		out := make(chan interface{}, chanBuf)
		curF := f
		curIn := in
		curOut := out

		go func(f cmd, in chan interface{}, out chan interface{}) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					log.Printf("panic in pipeline stage: %v", r)
				}
				close(out)
			}()
			f(in, out)
		}(curF, curIn, curOut)

		in = out
	}

	wg.Wait()
}

func SelectUsers(in, out chan interface{}) {
	var mp sync.Map
	var wg sync.WaitGroup

	for val := range in {
		curVal, ok := val.(string)
		if !ok {
			log.Printf("SelectUsers: unexpected type %T", val)
			continue
		}
		wg.Add(1)

		go func(email string) {
			defer wg.Done()
			user := GetUser(email)
			_, loaded := mp.LoadOrStore(user.ID, struct{}{})
			if !loaded {
				out <- user
			}
		}(curVal)
	}

	wg.Wait()
}

func SelectMessages(in, out chan interface{}) {

	var wg sync.WaitGroup

	arr := make([]User, 0, GetMessagesMaxUsersBatch)

	for val := range in {
		user, ok := val.(User)
		if !ok {
			log.Printf("SelectMessages: unexpected type %T", val)
			continue
		}
		arr = append(arr, user)
		if len(arr) == GetMessagesMaxUsersBatch {
			batch := make([]User, len(arr))
			copy(batch, arr)
			wg.Add(1)

			go func(batch []User) {
				defer wg.Done()
				res, err := GetMessages(batch...)
				if err != nil {
					log.Println("Error while getting MsgID from GetMessages")
				}

				for _, val := range res {
					out <- val
				}

			}(batch)

			arr = arr[:0]
		}
	}

	if len(arr) > 0 {
		batch := make([]User, len(arr))
		copy(batch, arr)
		wg.Add(1)
		go func(batch []User) {
			defer wg.Done()
			res, err := GetMessages(batch...)
			if err != nil {
				log.Println("Error while getting MsgID from GetMessages")
			}
			for _, val := range res {
				out <- val
			}
		}(batch)
	}

	wg.Wait()
}

func work(in, out chan interface{}) {
	for v := range in {
		id, ok := v.(MsgID)
		if !ok {
			log.Printf("work: unexpected type %T\n", v)
			continue
		}
		result, err := HasSpam(id)
		if err != nil {
			log.Printf("Error while getting value from HasSpam err: %v", err)
		}
		out <- MsgData{ID: id, HasSpam: result}
	}
}

func CheckSpam(in, out chan interface{}) {
	var wg sync.WaitGroup
	wg.Add(countWorkers)
	for i := 0; i < countWorkers; i++ {
		go func() {
			defer wg.Done()
			work(in, out)
		}()

	}
	wg.Wait()

}

func CombineResults(in, out chan interface{}) {
	arr := make([]MsgData, 0)
	for v := range in {
		md, ok := v.(MsgData)
		if !ok {
			log.Printf("CombineResults: unexpected type %T", v)
			continue
		}
		arr = append(arr, md)
	}

	sort.Slice(arr, func(i, j int) bool {
		if arr[i].HasSpam != arr[j].HasSpam {
			return arr[i].HasSpam
		}
		return arr[i].ID < arr[j].ID
	})

	for _, val := range arr {
		str := fmt.Sprintf("%t %d", val.HasSpam, val.ID)
		out <- str
	}
}
