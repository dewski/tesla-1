package tesla

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
)

var (
	retryTimeout          time.Duration = time.Second * 10
	ErrStreamEventInvalid               = errors.New("Bad message from Tesla API stream")
	ErrStreamClosed                     = errors.New("HTTP stream closed")
)

type streamConn struct {
	client   *http.Client
	resp     *http.Response
	url      *url.URL
	authData string
	stale    bool
	closed   bool
	mu       sync.Mutex
	// wait time before trying to reconnect, this will be
	// exponentially moved up until reaching maxWait, when
	// it will exit
	wait    int
	maxWait int
	connect func() (*http.Response, error)
}

func NewStreamConn(max int) streamConn {
	return streamConn{wait: 1, maxWait: max}
}

//type StreamHandler func([]byte)

func (conn *streamConn) Close() {
	// Just mark the connection as stale, and let the connect() handler close after a read
	conn.mu.Lock()
	defer conn.mu.Unlock()
	conn.stale = true
	conn.closed = true
	if conn.resp != nil {
		conn.resp.Body.Close()
	}
}

func (conn *streamConn) Wait() {
	time.Sleep(time.Second * time.Duration(conn.wait))
}

func basicauthConnect(conn *streamConn) (*http.Response, error) {
	if conn.isStale() {
		return nil, errors.New("Stale connection")
	}

	conn.client = &http.Client{}

	var req http.Request
	req.URL = conn.url
	req.Method = "GET"
	req.Header = http.Header{}
	req.Header.Set("Authorization", conn.authData)
	log.Debug()

	// log.Debug(req.Header)
	resp, err := conn.client.Do(&req)

	if err != nil {
		log.Debugf("Could not connect to stream %+v with %s", err, conn.authData)
		log.Error(err)
		return nil, err
	} else {
		log.Debugf("connected to %s \n\thttp status = %v\n\tauth=%s", conn.url, resp.Status, conn.authData)

		if resp.StatusCode == 401 {
			log.Debug(resp.Header)
			for n, v := range resp.Header {
				log.Debug(n, v[0])
			}
		}
	}

	return resp, nil
}

func (conn *streamConn) isStale() bool {
	conn.mu.Lock()
	r := conn.stale
	conn.mu.Unlock()
	return r
}

func (conn *streamConn) readStream(resp *http.Response, handler func([]byte), done chan bool) {
	defer close(done)
	var reader *bufio.Reader
	reader = bufio.NewReader(resp.Body)
	conn.resp = resp

	for {
		//we've been closed
		if conn.isStale() {
			conn.Close()
			log.Debug("Connection closed, shutting down ")
			done <- true
			break
		}

		line, err := reader.ReadBytes('\n')
		if err != nil {
			if conn.isStale() {
				log.Debug("conn stale, continue")
				continue
			}
			conn.Wait()
			//try reconnecting, but exponentially back off until MaxWait is reached then exit?
			resp, err := conn.connect()
			if err != nil || resp == nil {
				log.Debugf("Could not reconnect to source? sleeping and will retry %+v", err)
				if conn.wait < conn.maxWait {
					conn.wait = conn.wait * 2
				} else {
					log.Infof("exiting, max wait of %d reached", conn.maxWait)
					done <- true
					return
				}
				continue
			}
			if resp.StatusCode != 200 {
				if conn.wait < conn.maxWait {
					conn.wait = conn.wait * 2
				}
				continue
			}

			reader = bufio.NewReader(resp.Body)
			continue
		} else if conn.wait != 1 {
			conn.wait = 1
		}
		line = bytes.TrimSpace(line)

		if len(line) == 0 {
			log.Debugf("Received empty response %+v", line)
			continue
		}
		handler(line)
	}
}

func encodedAuth(user, pwd string) string {
	var buf bytes.Buffer
	encoder := base64.NewEncoder(base64.StdEncoding, &buf)
	encoder.Write([]byte(user + ":" + pwd))
	encoder.Close()
	log.Debugf("username=%s password=%s auth=%s", user, pwd, buf.String())
	return buf.String()
}

// Client for connecting
type Stream struct {
	Username string
	Password string
	conn     *streamConn
	MaxWait  int
	Handler  func([]byte)
}

func NewBasicAuthClient(username, password string, handler func([]byte)) *Stream {
	return &Stream{
		Username: username,
		Password: password,
		Handler:  handler,
		MaxWait:  300,
	}
}

// Create a new basic Auth Channel Handler
func NewChannelClient(username, password string, bc chan []byte) *Stream {
	return &Stream{
		Username: username,
		Password: password,
		Handler:  func(b []byte) { bc <- b },
		MaxWait:  300,
	}
}

func (s *Stream) SetMaxWait(max int) {
	s.MaxWait = max
	if s.conn != nil {
		s.conn.maxWait = s.MaxWait
	}
}

// Connect to an http stream
// @url = http address
// @params = http params to be added
func (s *Stream) Connect(url_ *url.URL, done chan bool) (err error) {
	var resp *http.Response
	sc := NewStreamConn(s.MaxWait)

	sc.url = url_
	sc.authData = "Basic " + encodedAuth(s.Username, s.Password)
	sc.connect = func() (*http.Response, error) {
		return basicauthConnect(&sc)
	}
	resp, err = sc.connect()
	if err != nil {
		log.Error(err)
		goto Return
	} else if resp == nil {
		err = errors.New("No response on connection, invalid connect")
		log.Error(err)
		goto Return
	}

	if resp.StatusCode != 200 {
		log.Debug("not http 200")
		err = errors.New("stream HTTP Error: " + resp.Status + "\n" + url_.Path)
		log.Error(err)
		goto Return
	}

	//close the current connection
	if s.conn != nil {
		s.conn.Close()
	}
	s.conn = &sc

	go sc.readStream(resp, s.Handler, done)

	return
Return:
	log.Info("exiting")
	done <- true
	return
}

// Close the client
func (s *Stream) Close() {
	log.Info("Attempting to close client")
	if s.conn == nil || s.conn.isStale() {
		return
	}
	log.Info("Successfuly closed client")
	s.conn.Close()
}
