package user

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// SMS forwards a challenge to the operator's configured SMS delivery service.
type SMS struct{ URL, Token string }

func (s SMS) Send(ctx context.Context, number, code string) error {
	data, e := json.Marshal(map[string]string{"phone": number, "code": code})
	if e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	req, e := http.NewRequestWithContext(ctx, http.MethodPost, s.URL, bytes.NewReader(data))
	if e != nil {
		return e
	}
	req.Header.Set("Content-Type", "application/json")
	if s.Token != "" {
		req.Header.Set("Authorization", "Bearer "+s.Token)
	}
	client := http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	res, e := client.Do(req)
	if e != nil {
		return e
	}
	defer res.Body.Close()
	if res.StatusCode/100 != 2 {
		return fmt.Errorf("SMS delivery returned HTTP %d", res.StatusCode)
	}
	return nil
}
