package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Streaming is opt-in. Wildcards retain the existing JSON response contract.
func wantsEventStream(r *http.Request) bool {
	streamQuality, jsonQuality := 0.0, 0.0
	for _, field := range r.Header.Values("Accept") {
		for _, item := range strings.Split(field, ",") {
			media, params, err := mime.ParseMediaType(strings.TrimSpace(item))
			if err != nil {
				continue
			}
			quality := 1.0
			if value, exists := params["q"]; exists {
				quality, err = strconv.ParseFloat(value, 64)
				if err != nil || quality < 0 || quality > 1 {
					continue
				}
			}
			switch media {
			case "text/event-stream":
				streamQuality = max(streamQuality, quality)
			case "application/json":
				jsonQuality = max(jsonQuality, quality)
			}
		}
	}
	return streamQuality > 0 && streamQuality >= jsonQuality
}

func canFlush(w http.ResponseWriter) bool {
	for {
		switch writer := w.(type) {
		case interface{ FlushError() error }:
			return true
		case http.Flusher:
			return true
		case interface{ Unwrap() http.ResponseWriter }:
			w = writer.Unwrap()
		default:
			return false
		}
	}
}

type eventWriter struct {
	w          http.ResponseWriter
	controller *http.ResponseController
	timeout    time.Duration
}

func (s *eventWriter) write(frame string) error {
	if err := s.controller.SetWriteDeadline(time.Now().Add(s.timeout)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	// An idle connection must not retain an expired per-write deadline between heartbeats.
	defer s.controller.SetWriteDeadline(time.Time{})
	if _, err := fmt.Fprint(s.w, frame); err != nil {
		return err
	}
	return s.controller.Flush()
}

func (s *eventWriter) event(name string, data any) error {
	encoded, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return s.write("event: " + name + "\ndata: " + string(encoded) + "\n\n")
}
