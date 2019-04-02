package consul_session_lock

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

type (
	app struct {
		TTL          string
		UpdatePeriod int
	}

	ConsultParam struct {
		LockDelay string   `json:",omitempty"`
		Node      string   `json:",omitempty"`
		Name      string   `json:",omitempty"`
		Checks    []string `json:",omitempty"`
		TTL       string   `json:",omitempty"`
	}

	Session struct {
		ID          string
		Name        string
		Node        string
		Checks      []string
		LockDelay   int64
		Behavior    string
		TTL         string
		CreateIndex int64
		ModifyIndex int64
	}
)

func getUrl(path string) string {
	return fmt.Sprintf("http://localhost:8500/v1/%s", path)
}

func New() *app {
	return &app{}
}

func (a *app) CheckAccess(sessionName string, availableCount int) (bool, error) {
	//
	data := ConsultParam{
		Name: sessionName,
		TTL:  a.TTL,
	}

	session, err := a.createSession(data)
	if err != nil {
		return false, err
	}

	sessionCount, err := a.sessionCount(sessionName)
	if err != nil {
		return false, err
	}

	if sessionCount <= availableCount {
		return true, nil
	}

	err = a.deleteSession(session.ID)
	if err != nil {
		return false, err
	}

	return false, nil
}

func (a *app) SessionHold(session Session, ctx context.Context) {
	duration := time.Duration(a.UpdatePeriod)
	ticker := time.NewTicker(duration * time.Second)
	for {
		select {
		case <-ticker.C:
			_, _ = a.reNewSession(session)
		case <-ctx.Done():
			return
		}
	}
}

func (a *app) reNewSession(session Session) (Session, error) {

	answer, err := a.makeRequest(http.MethodPut, getUrl("session/renew/"+session.ID), nil)
	if err != nil {
		return session, err
	}

	err = json.Unmarshal(answer, &session)
	if err != nil {
		return session, err
	}

	return session, nil
}

// delete session by ID
func (a *app) deleteSession(sID string) error {
	_, err := a.makeRequest(http.MethodPut, getUrl("session/destroy/"+sID), nil)
	if err != nil {
		return err
	}

	return nil
}

func (a *app) makeRequest(method, url string, body *bytes.Buffer) ([]byte, error) {
	var request *http.Request
	var err error

	if body == nil {
		request, err = http.NewRequest(method, url, nil)
	} else {
		request, err = http.NewRequest(method, url, body)
	}

	if err != nil {
		return nil, err
	}

	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return nil, err
	}

	defer func() { _ = response.Body.Close() }()
	return ioutil.ReadAll(response.Body)
}

// create session with name
func (a *app) createSession(data ConsultParam) (Session, error) {
	session := Session{}

	params, _ := json.Marshal(data)
	answer, err := a.makeRequest(http.MethodPut, getUrl("session/create"), bytes.NewBuffer(params))
	if err != nil {
		return session, err
	}

	err = json.Unmarshal(answer, &session)
	if err != nil {
		return session, err
	}

	return session, nil
}

// returns a list of sessions
func (a *app) sessionList() ([]byte, error) {
	answer, err := a.makeRequest(http.MethodGet, getUrl("session/list"), nil)
	if err != nil {
		return answer, err
	}

	return answer, nil
}

// returns the number of sessions with this name
func (a *app) sessionCount(sessionName string) (int, error) {
	data, err := a.sessionList()
	if err != nil {
		return 0, err
	}

	session := make([]Session, 0)
	err = json.Unmarshal(data, &session)
	if err != nil {
		return 0, err
	}

	sessionCount := 0
	for _, s := range session {
		if s.Name == sessionName {
			sessionCount++
		}
	}

	return sessionCount, nil
}
