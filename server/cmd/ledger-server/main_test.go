package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDefaultAddr(t *testing.T) {
	t.Setenv("PORT", "")
	if got := defaultAddr(); got != "127.0.0.1:8700" {
		t.Errorf("no $PORT: got %q", got)
	}
	t.Setenv("PORT", "9123")
	if got := defaultAddr(); got != "0.0.0.0:9123" {
		t.Errorf("$PORT=9123: got %q", got)
	}
}

func TestUIIsReadOnly(t *testing.T) {
	h := Handler(nil) // the UI routes never touch the store
	for method, want := range map[string]int{
		http.MethodGet:    http.StatusOK,
		http.MethodHead:   http.StatusOK,
		http.MethodPost:   http.StatusMethodNotAllowed,
		http.MethodPut:    http.StatusMethodNotAllowed,
		http.MethodDelete: http.StatusMethodNotAllowed,
	} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(method, "/runs/abc", nil))
		if rec.Code != want {
			t.Errorf("%s /runs/abc: got %d, want %d", method, rec.Code, want)
		}
	}
}
