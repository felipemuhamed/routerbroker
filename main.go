package main

import (
	"flag"
	"log"
	"net/http"
)

var addr = flag.String("addr", ":3000", "http service address")

func main() {
	flag.Parse()
	kafkaDojot := NewKafka("192.168.15.24", "test")

	go kafkaDojot.Run()
	go kafkaDojot.KafkaMessageRead()

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		ServeWs(kafkaDojot, w, r)
	})
	err := http.ListenAndServeTLS(*addr, "./cert/vernemq-k8s-0.crt", "./cert/vernemq-k8s-0.key", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}

}
