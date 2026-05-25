package main
import (
	"bytes"
	"text/template"
	"sync"
)

type Recipient struct{
	Name string
	Email string
}

// main goroutine 
func main() {
	recipientChannel := make(chan Recipient , 50)
var wg sync.WaitGroup

	//non blocking go routines 
	go func()  {
	loadRecipient("dummy_emails.csv" , recipientChannel)
		close(recipientChannel)
	}()
    
workerCount := 5
for i:= 1 ;i<= workerCount ; i++ {
	wg.Add(1)
	go emailWorker(i , recipientChannel , &wg)
}

wg.Wait()

	
}

func Template(r Recipient) (string , error){
	t , err := template.ParseFiles("email.tmpl")
	if err != nil{
		return "" , err
	}
	var tpl bytes.Buffer
	err = t.Execute(&tpl , r)
	if err != nil {
		return "",err
	}
	return tpl.String() , nil
}