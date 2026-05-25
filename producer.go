package main

import (
	"encoding/csv"
	"fmt"
	"os"
)

func loadRecipient(filePath string , ch chan Recipient ) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	r := csv.NewReader(f)

	records, err := r.ReadAll()
	if err != nil {
		return err
	}

	for _, record := range records[1:] {
		fmt.Println("Name:", record[0], "Email:", record[1])
		ch <- Recipient{
			Name : record[0],
			Email : record[1],
		}
	}

	return nil
}


