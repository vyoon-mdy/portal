package main

import (
	"log"

	"github.com/vyoon-mdy/portal/ECOM/cmd/api"
)

func main() {
	server := api.NewAPIServer(":8888", nil)
	if err := server.Run(); err != nil {
		log.Fatal(err)
	}
}