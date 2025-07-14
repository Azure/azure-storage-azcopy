package azcopy

import "fmt"

type LogoutOptions struct {
}

type LogoutResponse struct {
}

func (c Client) Logout(_ LogoutOptions) (LogoutResponse, error) {
	uotm := GetUserOAuthTokenManagerInstance()
	if err := uotm.RemoveCachedToken(); err != nil {
		return LogoutResponse{}, fmt.Errorf("failed to perform logout command, %v", err)
	}
	return LogoutResponse{}, nil
}
