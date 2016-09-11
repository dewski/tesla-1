package tesla

import (
	"bufio"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/cenkalti/backoff"
)

var (
	ErrStreamClosed       = errors.New("HTTP stream closed")
	ErrStreamEventInvalid = errors.New("Bad message from Tesla API stream")
	StreamParams          = "speed,odometer,soc,elevation,est_heading,est_lat,est_lng,power,shift_state,range,est_range,heading"
	StreamingURL          = "https://streaming.vn.teslamotors.com"
)

type Stream struct {
	client   *http.Client
	Messages chan StreamEvent
	Errors   chan error
	done     chan struct{}
	group    *sync.WaitGroup
	body     io.Closer
}

// The event returned by the vehicle by the Tesla API
type StreamEvent struct {
	Timestamp  time.Time `json:"timestamp"`
	Speed      int       `json:"speed"`
	Odometer   float64   `json:"odometer"`
	Soc        int       `json:"soc"`
	Elevation  int       `json:"elevation"`
	EstHeading int       `json:"est_heading"`
	EstLat     float64   `json:"est_lat"`
	EstLng     float64   `json:"est_lng"`
	Power      int       `json:"power"`
	ShiftState string    `json:"shift_state"`
	Range      int       `json:"range"`
	EstRange   int       `json:"est_range"`
	Heading    int       `json:"heading"`
}

// Requests a stream from the vehicle and returns a Go channel
func (v Vehicle) Stream() (*Stream, error) {
	url := StreamingURL + "/stream/" + strconv.Itoa(v.VehicleID) + "/?values=" + StreamParams
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(ActiveClient.Auth.Email, v.Tokens[0])

	s := &Stream{
		client:   ActiveClient.HTTP,
		Messages: make(chan StreamEvent),
		Errors:   make(chan error),
		done:     make(chan struct{}),
		group:    &sync.WaitGroup{},
	}
	s.group.Add(1)
	go s.retry(req, newExponentialBackOff())
	return s, nil
}

// Stop signals retry and receiver to stop, closes the Messages channel, and
// blocks until done.
func (s *Stream) Stop() {
	close(s.done)
	// Scanner does not have a Stop() or take a done channel, so for low volume
	// streams Scan() blocks until the next keep-alive. Close the resp.Body to
	// escape and stop the stream in a timely fashion.
	if s.body != nil {
		s.body.Close()
	}
	// block until the retry goroutine stops
	s.group.Wait()
}

// retry retries making the given http.Request and receiving the response
// according to the Twitter backoff policies. Callers should invoke in a
// goroutine since backoffs sleep between retries.
// https://dev.twitter.com/streaming/overview/connecting
func (s *Stream) retry(req *http.Request, expBackOff backoff.BackOff) {
	// close Messages channel and decrement the wait group counter
	defer close(s.Messages)
	defer close(s.Errors)
	defer s.group.Done()

	var wait time.Duration
	for !stopped(s.done) {
		resp, err := s.client.Do(req)
		if err != nil {
			// stop retrying for HTTP protocol errors
			s.Errors <- err
			return
		}
		// when err is nil, resp contains a non-nil Body which must be closed
		defer resp.Body.Close()
		s.body = resp.Body
		switch resp.StatusCode {
		case 200:
			log.Debug("Received 200 for s.receive")
			// receive stream response Body, handles closing
			s.receive(resp.Body)
			log.Debug("Returned from s.receive")
		case 420, 429, 503:
			log.Debugf("Received %d from s.retry, exponential backoff", resp.StatusCode)
			// exponential backoff
			wait = expBackOff.NextBackOff()
		default:
			// stop retrying for other response codes
			log.Debug("stop retrying for other response codes")
			resp.Body.Close()
			return
		}
		// close response before each retry
		log.Debug("close response before each retry")
		resp.Body.Close()
		if wait == backoff.Stop {
			log.Debug("Backing off...")
			return
		}
		sleepOrDone(wait, s.done)
	}
}

// sleepOrDone pauses the current goroutine until the done channel receives
// or until at least the duration d has elapsed, whichever comes first. This
// is similar to time.Sleep(d), except it can be interrupted.
func sleepOrDone(d time.Duration, done <-chan struct{}) {
	select {
	case <-time.After(d):
		log.Debugf("Backed off for %+v before continuing stream", d)
		return
	case <-done:
		return
	}
}

// receive scans a stream response body, JSON decodes tokens to messages, and
// sends messages to the Messages channel. Receiving continues until an EOF,
// scan error, or the done channel is closed.
func (s *Stream) receive(body io.ReadCloser) {
	defer body.Close()
	log.Debug("Reached receive")
	// A bufio.Scanner steps through 'tokens' of data on each Scan() using a
	// SplitFunc. SplitFunc tokenizes input bytes to return the number of bytes
	// to advance, the token slice of bytes, and any errors.
	scanner := bufio.NewScanner(body)
	// default ScanLines SplitFunc is incorrect for Twitter Streams, set custom
	scanner.Split(bufio.ScanLines)
	for !stopped(s.done) && scanner.Scan() {
		log.Debug("Reached inner receive")
		log.Infof("new scan message received %+v", scanner.Text())
		token := scanner.Bytes()
		if len(token) == 0 {
			// empty keep-alive
			continue
		}

		s.parseStreamEvent(scanner.Text())

		select {
		case <-s.done:
			log.Debug("allow client to Stop(), even if not receiving")
			return
		}
	}
}

// Parses the stream event, setting all of the appropriate data types
func (s *Stream) parseStreamEvent(event string) {
	data := strings.Split(event, ",")
	if len(data) != 13 {
		s.Errors <- ErrStreamEventInvalid
		return
	}

	se := StreamEvent{}
	timestamp, _ := strconv.ParseInt(data[0], 10, 64)
	se.Timestamp = time.Unix(0, timestamp*int64(time.Millisecond))
	se.Speed, _ = strconv.Atoi(data[1])
	se.Odometer, _ = strconv.ParseFloat(data[2], 64)
	se.Soc, _ = strconv.Atoi(data[3])
	se.Elevation, _ = strconv.Atoi(data[4])
	se.EstHeading, _ = strconv.Atoi(data[5])
	se.EstLat, _ = strconv.ParseFloat(data[6], 64)
	se.EstLng, _ = strconv.ParseFloat(data[7], 64)
	se.Power, _ = strconv.Atoi(data[8])
	se.ShiftState = data[9]
	se.Range, _ = strconv.Atoi(data[10])
	se.EstRange, _ = strconv.Atoi(data[11])
	se.Heading, _ = strconv.Atoi(data[12])

	s.Messages <- se
}

func stopped(done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	default:
		return false
	}
}
