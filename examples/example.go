package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/davidnarayan/go-splunk/splunk"
)

var (
	hostname = flag.String("hostname", "localhost", "Splunk hostname")
	username = flag.String("username", "admin", "Splunk username")
	password = flag.String("password", "changeme", "Splunk password")
)

func main() {
	flag.Parse()

	// Create Splunk Client
	c := splunk.NewClient(*hostname, *username, *password)

	// Get server info
	resp, err := c.Get("/server/info")

	if err != nil {
		log.Fatal(err)
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(body))
}
