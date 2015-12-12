package main

import (
	"fmt"
	"github.com/cmu440-F15/paxosapp/controllers"
	"net/http"
)

const (
	serverPort = 3030
)

func main() {
	uc, _ := controllers.NewUserController()
	http.HandleFunc("/propose", uc.Propose)
	http.HandleFunc("/getround", uc.GetRound)
	http.HandleFunc("/getvalue", uc.GetValue)
	http.ListenAndServe(fmt.Sprintf(":%d", serverPort), nil)
}
