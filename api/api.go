package api

import (
	"fmt"
	"net/http"
)

// Handler for API endpoints
func Handler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Welcome to Gronicle API!")
}
