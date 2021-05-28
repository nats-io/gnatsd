package server

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestErrorsDataSource(t *testing.T) {
	dat, err := ErrorsDataSource()
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	if len(dat) == 0 {
		t.Fatalf("Expected > 0 errors")
	}

	errorsDataSource = "{"
	_, err = ErrorsDataSource()
	if err == nil {
		t.Fatalf("expected decoding error")
	}
}

func TestIsNatsErr(t *testing.T) {
	if !IsNatsErr(ApiErrors[JSNotEnabledForAccountErr], JSNotEnabledForAccountErr) {
		t.Fatalf("Expected error match")
	}

	if IsNatsErr(ApiErrors[JSNotEnabledForAccountErr], JSClusterNotActiveErr) {
		t.Fatalf("Expected error mismatch")
	}

	if IsNatsErr(ApiErrors[JSNotEnabledForAccountErr], JSClusterNotActiveErr, JSClusterNotAvailErr) {
		t.Fatalf("Expected error mismatch")
	}

	if !IsNatsErr(ApiErrors[JSNotEnabledForAccountErr], JSClusterNotActiveErr, JSNotEnabledForAccountErr) {
		t.Fatalf("Expected error match")
	}

	if !IsNatsErr(&ApiError{ErrCode: 10039}, 1, JSClusterNotActiveErr, JSNotEnabledForAccountErr) {
		t.Fatalf("Expected error match")
	}

	if IsNatsErr(&ApiError{ErrCode: 10039}, 1, 2, JSClusterNotActiveErr) {
		t.Fatalf("Expected error mismatch")
	}

	if IsNatsErr(nil, JSClusterNotActiveErr) {
		t.Fatalf("Expected error mismatch")
	}

	if IsNatsErr(errors.New("x"), JSClusterNotActiveErr) {
		t.Fatalf("Expected error mismatch")
	}
}

func TestApiError_Error(t *testing.T) {
	if es := ApiErrors[JSClusterNotActiveErr].Error(); es != "JetStream not in clustered mode (10006)" {
		t.Fatalf("Expected 'JetStream not in clustered mode (10006)', got %q", es)
	}
}

func TestApiError_NewF(t *testing.T) {
	ne := ApiErrors[JSRestoreSubscribeFailedErrF].NewT("{subject}", "the.subject", "{err}", errors.New("failed error"))
	if ne.Description != "JetStream unable to subscribe to restore snapshot the.subject: failed error" {
		t.Fatalf("Expected 'JetStream unable to subscribe to restore snapshot the.subject: failed error' got %q", ne.Description)
	}

	if ne == ApiErrors[JSRestoreSubscribeFailedErrF] {
		t.Fatalf("Expected a new instance")
	}
}

func TestApiError_ErrOrNewF(t *testing.T) {
	if ne := ApiErrors[JSStreamRestoreErrF].ErrOrNewT(ApiErrors[JSNotEnabledForAccountErr], "{err}", errors.New("failed error")); !IsNatsErr(ne, JSNotEnabledForAccountErr) {
		t.Fatalf("Expected JSNotEnabledForAccountErr got %s", ne)
	}

	if ne := ApiErrors[JSStreamRestoreErrF].ErrOrNewT(nil, "{err}", errors.New("failed error")); !IsNatsErr(ne, JSStreamRestoreErrF) {
		t.Fatalf("Expected JSStreamRestoreErrF got %s", ne)
	}

	if ne := ApiErrors[JSStreamRestoreErrF].ErrOrNewT(errors.New("other error"), "{err}", errors.New("failed error")); !IsNatsErr(ne, JSStreamRestoreErrF) {
		t.Fatalf("Expected JSStreamRestoreErrF got %s", ne)
	}
}

func TestApiError_ErrOr(t *testing.T) {
	if ne := ApiErrors[JSPeerRemapErr].ErrOr(ApiErrors[JSNotEnabledForAccountErr]); !IsNatsErr(ne, JSNotEnabledForAccountErr) {
		t.Fatalf("Expected JSNotEnabledForAccountErr got %s", ne)
	}

	if ne := ApiErrors[JSPeerRemapErr].ErrOr(nil); !IsNatsErr(ne, JSPeerRemapErr) {
		t.Fatalf("Expected JSPeerRemapErr got %s", ne)
	}

	if ne := ApiErrors[JSPeerRemapErr].ErrOr(errors.New("other error")); !IsNatsErr(ne, JSPeerRemapErr) {
		t.Fatalf("Expected JSPeerRemapErr got %s", ne)
	}
}

func TestApiError_NewT(t *testing.T) {
	aerr := ApiError{
		Code:        999,
		Description: "thing {string} failed on attempt {int} after {duration} with {float}: {err}",
	}

	if ne := aerr.NewT("{float}", 1.1, "{err}", fmt.Errorf("simulated error"), "{string}", "hello world", "{int}", 10, "{duration}", 456*time.Millisecond); ne.Description != "thing hello world failed on attempt 10 after 456ms with 1.1: simulated error" {
		t.Fatalf("Expected formatted error, got: %q", ne.Description)
	}
}
