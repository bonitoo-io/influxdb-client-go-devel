package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
)

// Setup sets up a new InfluxDB server.
// It requires a client be set up with a username and password.
// If successful will add a token to the client.
// RetentionPeriodHrs of zero will result in infinite retention.
func (c *client) Setup(username, password, org, bucket string) (*SetupResponse, error) {
	if username == "" || password == "" {
		return nil, errors.New("a username and password is required for a setup")
	}
	var setupResult *SetupResponse
	inputData, err := json.Marshal(SetupRequest{
		Username: username,
		Password: password,
		Org:      org,
		Bucket:   bucket,
	})
	if err != nil {
		return nil, err
	}
	if c.options.Debug > 2 {
		log.Printf("D! Request:\n%s\n", string(inputData))
	}
	err = c.postRequest(c.serverUrl+"/api/v2/setup", bytes.NewReader(inputData), func(req *http.Request) {
		req.Header.Add("Content-Type", "application/json; charset=utf-8")
	},
		func(resp *http.Response) error {
			defer resp.Body.Close()
			setupResponse := &SetupResponse{}
			if c.options.Debug > 2 {
				body, _ := ioutil.ReadAll(resp.Body)
				log.Printf("D! Response:\n%s\n", string(body))
				if err := json.NewDecoder(bytes.NewReader(body)).Decode(setupResponse); err != nil {
					return err
				}
			} else {
				if err := json.NewDecoder(resp.Body).Decode(setupResponse); err != nil {
					return err
				}
			}
			setupResult = setupResponse
			if setupResponse.Code != "conflict" && resp.StatusCode == http.StatusCreated && setupResponse.Auth.Token != "" {
				c.authorization = "Token " + setupResponse.Auth.Token
			}
			return nil
		},
	)
	return setupResult, err
}

// SetupRequest is a request to setup a new influx instance.
type SetupRequest struct {
	Username           string `json:"username"`
	Password           string `json:"password"`
	Org                string `json:"org"`
	Bucket             string `json:"bucket"`
	RetentionPeriodHrs int    `json:"retentionPeriodHrs"`
}

// SetupResult is the result of setting up a new influx instance.
type SetupResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	User    struct {
		Links struct {
			Logs string `json:"logs"`
			Self string `json:"self"`
		} `json:"links"`
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"user"`
	Bucket struct {
		ID             string `json:"id"`
		OrganizationID string `json:"organizationID"`
		Organization   string `json:"organization"`
		Name           string `json:"name"`
		RetentionRules []struct {
			Type         string `json:"type"`
			EverySeconds int    `json:"everySeconds"`
		} `json:"retentionRules"`
		Links struct {
			Labels  string `json:"labels"`
			Logs    string `json:"logs"`
			Members string `json:"members"`
			Org     string `json:"org"`
			Owners  string `json:"owners"`
			Self    string `json:"self"`
			Write   string `json:"write"`
		} `json:"links"`
	} `json:"bucket"`
	Org struct {
		Links struct {
			Buckets    string `json:"buckets"`
			Dashboards string `json:"dashboards"`
			Labels     string `json:"labels"`
			Logs       string `json:"logs"`
			Members    string `json:"members"`
			Owners     string `json:"owners"`
			Secrets    string `json:"secrets"`
			Self       string `json:"self"`
			Tasks      string `json:"tasks"`
		} `json:"links"`
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"org"`
	Auth struct {
		ID          string `json:"id"`
		Token       string `json:"token"`
		Status      string `json:"status"`
		Description string `json:"description"`
		OrgID       string `json:"orgID"`
		Org         string `json:"org"`
		UserID      string `json:"userID"`
		User        string `json:"user"`
		Permissions []struct {
			Action   string `json:"action"`
			Resource struct {
				Type string `json:"type"`
			} `json:"resource"`
		} `json:"permissions"`
		Links struct {
			Self string `json:"self"`
			User string `json:"user"`
		} `json:"links"`
	} `json:"auth"`
}
