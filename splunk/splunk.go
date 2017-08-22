// Package splunk is a Go client library for the Splunk API
package splunk

import (
	"crypto/tls"
	"encoding/xml"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	// TODO - just use the standard library?
	"github.com/davidnarayan/go-hashmap"
	"github.com/davidnarayan/go-logging"
)

//-----------------------------------------------------------------------------
// Globals

var (
	// batch size for splunk results
	MinBatchSize = 100
	MaxBatchSize = 1000
)

var (
	// TODO: make this configurable
	ConnectionTimeout = 10 * time.Second
)

//-----------------------------------------------------------------------------

// TimeoutDialer creates a Dialer that can timeout after a connection timeout
// or a read timeout
func TimeoutDialer(connTimeout, rwTimeout time.Duration) func(netw, addr string) (c net.Conn, err error) {
	return func(netw, addr string) (net.Conn, error) {
		conn, err := net.DialTimeout(netw, addr, connTimeout)

		if err != nil {
			return nil, err
		}

		conn.SetDeadline(time.Now().Add(rwTimeout))

		return conn, nil
	}
}

//-----------------------------------------------------------------------------

// Client is a Splunk client
type Client struct {
	HttpClient *http.Client
	Host       string // Splunk hostname(s)
	Scheme     string // Connection scheme (http or https)
	Username   string // Splunk username
	Password   string // Splunk password
	App        string // Splunk app
}

// NewClient creates a new Splunk Client
func NewClient(host, username, password string) (c *Client) {
	// Don't verify SSL certificates since Splunk uses self-signed certs
	// by default
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		Dial:            TimeoutDialer(ConnectionTimeout, ConnectionTimeout),
	}

	return &Client{
		Host:       host,
		Username:   username,
		Password:   password,
		Scheme:     "https",
		App:        "search",
		HttpClient: &http.Client{Transport: tr},
	}
}

// Get makes an HTTP GET request to an API endpoint
func (c *Client) Get(endpoint string) (resp *http.Response, err error) {
	return c.do("GET", endpoint, nil)
}

// Get makes an HTTP POST request to an API endpoint
func (c *Client) Post(endpoint string, data url.Values) (resp *http.Response, err error) {
	return c.do("POST", endpoint, data)
}

// do makes an HTTP request to an API endpoint
func (c *Client) do(method, endpoint string, data url.Values) (resp *http.Response, err error) {
	host := c.PickHost()
	url := fmt.Sprintf("%s://%s/services/%s", c.Scheme, host, endpoint)

	var req *http.Request

	switch method {
	case "POST":
		req, err = http.NewRequest(method, url,
			strings.NewReader(data.Encode()))

		if err != nil {
			return nil, err
		}
	default:
		req, err = http.NewRequest(method, url, nil)
		if err != nil {
			return nil, err
		}
	}

	req.SetBasicAuth(c.Username, c.Password)
	logging.Trace(fmt.Sprintf("HTTP REQUEST: %+v", req))
	resp, err = c.HttpClient.Do(req)

	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		hm := hashmap.NewHashMap()
		hm.Set("status", resp.Status)
		hm.Set("hostname", host)
		hm.Set("username", c.Username)
		hm.Set("url", url)
		return nil, errors.New(fmt.Sprintf("HTTP Error: %s", hm))
	}

	logging.Trace(fmt.Sprintf("HTTP RESPONSE: %+v", resp))
	return resp, err
}

// SetApp sets the Splunk app to search against
func (c *Client) SetApp(app string) {
	c.App = app
}

// Pick a random host from the pool
func (c *Client) PickHost() string {
	h := strings.Split(c.Host, ",")
	n := len(h)

	if n < 2 {
		return h[0]
	}

	rand.Seed(time.Now().UnixNano())

	return h[rand.Intn(n)]
}

//-----------------------------------------------------------------------------
// Structs for Splunk XML Responses

type AtomFeedEntry struct {
	Title      string     `xml:"entry>title"`
	Properties []Property `xml:"entry>content>dict>key"`
}

type AtomEntry struct {
	Title      string     `xml:"title"`
	Properties []Property `xml:"content>dict>key"`
}

type Property struct {
	Name  string `xml:"name,attr"`
	Value string `xml:",chardata"`
}

// Get the value of a property for an entry
func (entry *AtomFeedEntry) Property(name string) (value string) {
	for _, p := range entry.Properties {
		if p.Name == name {
			value = p.Value
			break
		}
	}

	return value
}

//-----------------------------------------------------------------------------
// Splunk Saved Search

type SavedSearch struct {
	Name string
	TTL  time.Duration
}

// Get the properties for a saved search
func (c *Client) SavedSearch(name string) (ss *SavedSearch, err error) {
	logging.Debug("Fetching data for savedsearch=%s", name)
	resp, err := c.Get(fmt.Sprintf("saved/searches/%s", name))

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return nil, err
	}

	entry := &AtomFeedEntry{}
	err = xml.Unmarshal(body, entry)

	if err != nil {
		return nil, err
	}

	/*
	 * TODO: The default dispatch.ttl is "2p" which isn't a valid Go duration.
	 * The "p" indicates that the ttl is a multiple of the execution period of
	 * the saved search. Need to figure out the best way to parse "Splunk"
	 * TTLs as a time.Duration
	 */

	//dispatch_ttl := entry.Property("dispatch.ttl")
	//ttl, err := time.ParseDuration(fmt.Sprintf("%ss", dispatch_ttl))
	//
	//if err != nil {
	//	hm := hashmap.NewHashMap()
	//	hm.Set("savedsearch", name)
	//	hm.Set("dispatch.ttl", dispatch_ttl)
	//	hm.Set("error", err.Error())
	//	errmsg := errors.New(fmt.Sprintf("Unable to parse duration: %s", hm))
	//	return nil, errmsg
	//}

	ss = &SavedSearch{
		Name: entry.Title,
		//TTL:  ttl,
	}

	return ss, nil
}

//-----------------------------------------------------------------------------
// Splunk Job

type SearchJob struct {
	Id           string       `xml:"sid"`
	Messages     []JobMessage `xml:"messages>msg"`
	Client       *Client
	PollInterval time.Duration
	Properties   []Property
}

type JobMessage struct {
	Level  string `xml:"type,attr"`
	String string `xml:",chardata"`
}

type Event map[string]string

type SearchResults struct {
	InitOffset int     `xml:"initOffset" json:"init_offset"`
	Events     []Event `xml:"results" json:"results"`
}

// Submit a new search job. Since this endpoint is non-blocking, the expected
// response is an XML message with a <sid> tag which has the job ID. This ID
// will then be used to periodically poll the job for its status and fetch its
// results when the job indicates that it has completed.
func (c *Client) NewSearchJob(query string) (job *SearchJob, err error) {
	job = &SearchJob{
		Client:       c,
		PollInterval: time.Duration(time.Second * 2),
	}

	// Send the request to Splunk
	resp, err := job.Client.Post("search/jobs", url.Values{"search": {query}})

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return nil, err
	}

	// A proper response should look like this:
	// <?xml>
	// <response>
	//   <sid>123456789</sid>
	// </response>
	//
	err = xml.Unmarshal(body, &job)

	if err != nil {
		return nil, err
	}

	logging.Trace("XML UNMARSHAL: %+v", job)

	// Job Id must be set!
	if job.Id == "" {
		return nil, errors.New("Unable to find job ID in XML response")
	}

	// If the response has error messages, they will be in the <messages> tag
	var errorMessages []string

	for _, m := range job.Messages {
		errorMessages = append(errorMessages,
			fmt.Sprintf("%s %s", m.Level, m.String))
	}

	if len(errorMessages) > 0 {
		err = errors.New(strings.Join(errorMessages, "\n"))
		return nil, err
	}

	return job, nil
}

func (job *SearchJob) SetPollInterval(duration time.Duration) {
	job.PollInterval = duration
}

// Check the latest status of the job
func (job *SearchJob) Refresh() (err error) {
	resp, err := job.Client.Get(fmt.Sprintf("search/jobs/%s", job.Id))

	if err != nil {
		return err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return err
	}

	entry := &AtomEntry{}
	err = xml.Unmarshal(body, &entry)

	if err != nil {
		return err
	}

	job.Properties = entry.Properties

	return nil
}

// Get the value of a property for a job
// TODO - this is a dupe of func(entry *AtomFeedEntry) Property()
func (job *SearchJob) Property(name string) (value string) {
	for _, p := range job.Properties {
		if p.Name == name {
			value = p.Value
			logging.Trace("Fetched property: job=%s name=%s value=%s", job.Id, name, value)
			break
		}
	}

	return value
}

// Check if a job has completed
func (job *SearchJob) IsDone() (bool, error) {
	err := job.Refresh()

	if err != nil {
		return false, err
	}

	return strconv.ParseBool(job.Property("isDone"))
}

// Get the number of results for a job
func (job *SearchJob) ResultCount() (int, error) {
	rc, err := strconv.Atoi(job.Property("resultCount"))

	if err != nil {
		e := errors.New(
			fmt.Sprintf("Error getting resultCount for job: %s", err))
		return -1, e
	}

	return rc, nil
}

// Wait for a job to complete by polling for status until the job is finished
func (job *SearchJob) Wait() (err error) {
	for {
		done, err := job.IsDone()

		if err != nil {
			return err
		}

		if done {
			break
		}

		time.Sleep(job.PollInterval)
	}

	return
}

// Fetch raw results from a job
func (job *SearchJob) RawResults(offset, count int) ([]byte, error) {
	v := url.Values{
		"output_mode": {"json"},
		"offset":      {strconv.Itoa(offset)},
		"count":       {strconv.Itoa(count)},
	}

	uri := fmt.Sprintf("search/jobs/%s/results?%s", job.Id, v.Encode())
	resp, err := job.Client.Get(uri)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	return ioutil.ReadAll(resp.Body)
}

// Calculate batch size (count). The idea here is to attempt to balance the
// number of events per request (the batchsize) with the number of requests
// made to Splunk.  The algorithm adjusts the count upwards by a factor of 2
// until either the max events per request is reached or the batch size is
// half the total requests (which would result in two HTTP requests).
func (job *SearchJob) CalculateBatchSize(resultCount int) int {
	min := uint(MinBatchSize)
	max := uint(MaxBatchSize)
	count := uint(resultCount)

	if count <= min {
		return int(min)
	}

	var size uint = min
	var i uint = 0

	for {
		if size >= max {
			return int(max)
		}

		if size >= count>>1 {
			return int(size)
		}

		size += min << i
		i++
	}

	return int(max)
}
