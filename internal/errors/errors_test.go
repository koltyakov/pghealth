package errors

import (
	"errors"
	"testing"
)

func TestCollectionError(t *testing.T) {
	underlying := errors.New("connection refused")
	err := NewCollectionError("query tables", underlying, true)

	if err.Op != "query tables" {
		t.Errorf("expected Op 'query tables', got %q", err.Op)
	}

	if !err.Partial {
		t.Error("expected Partial to be true")
	}

	if !errors.Is(err, underlying) {
		t.Error("expected errors.Is to match underlying error")
	}

	expected := "partial collection error in query tables: connection refused"
	if err.Error() != expected {
		t.Errorf("expected error %q, got %q", expected, err.Error())
	}
}

func TestCollectionErrorNonPartial(t *testing.T) {
	err := NewCollectionError("connect", errors.New("timeout"), false)
	expected := "collection error in connect: timeout"
	if err.Error() != expected {
		t.Errorf("expected error %q, got %q", expected, err.Error())
	}
}

func TestValidationError(t *testing.T) {
	err := NewValidationError("timeout", "-5s", "must be positive")

	if !errors.Is(err, ErrInvalidConfig) {
		t.Error("ValidationError should match ErrInvalidConfig")
	}

	expected := `invalid timeout "-5s": must be positive`
	if err.Error() != expected {
		t.Errorf("expected error %q, got %q", expected, err.Error())
	}
}

func TestValidationErrorNoValue(t *testing.T) {
	err := NewValidationError("url", "", "required")
	expected := "invalid url: required"
	if err.Error() != expected {
		t.Errorf("expected error %q, got %q", expected, err.Error())
	}
}

func TestQueryError(t *testing.T) {
	err := NewQueryError("SELECT * FROM users", errors.New("relation does not exist"))

	expected := "query failed [SELECT * FROM users]: relation does not exist"
	if err.Error() != expected {
		t.Errorf("expected error %q, got %q", expected, err.Error())
	}
}

func TestQueryErrorLongQuery(t *testing.T) {
	longQuery := "SELECT " + string(make([]byte, 200))
	err := NewQueryError(longQuery, errors.New("error"))

	// Query should be truncated with ...
	if len(err.Query) != 103 { // 100 + "..."
		t.Errorf("expected truncated query length 103, got %d", len(err.Query))
	}
	if err.Query[len(err.Query)-3:] != "..." {
		t.Error("expected truncated query to end with ...")
	}
}

func TestReportError(t *testing.T) {
	err := NewReportError("template", "/tmp/report.html", errors.New("parse error"))

	expected := "report template error for /tmp/report.html: parse error"
	if err.Error() != expected {
		t.Errorf("expected error %q, got %q", expected, err.Error())
	}
}

func TestReportErrorNoPath(t *testing.T) {
	err := NewReportError("render", "", errors.New("data error"))
	expected := "report render error: data error"
	if err.Error() != expected {
		t.Errorf("expected error %q, got %q", expected, err.Error())
	}
}

func TestMultiError(t *testing.T) {
	me := &MultiError{}

	if me.ErrorOrNil() != nil {
		t.Error("empty MultiError should return nil")
	}

	me.Add(nil) // Should be ignored
	if me.ErrorOrNil() != nil {
		t.Error("MultiError with only nil should return nil")
	}

	err1 := errors.New("error 1")
	err2 := errors.New("error 2")

	me.Add(err1)
	me.Add(err2)

	if len(me.Errors) != 2 {
		t.Errorf("expected 2 errors, got %d", len(me.Errors))
	}

	if !errors.Is(me, err1) {
		t.Error("MultiError should match first error")
	}

	expected := "2 errors occurred; first: error 1"
	if me.Error() != expected {
		t.Errorf("expected error %q, got %q", expected, me.Error())
	}
}

func TestMultiErrorSingle(t *testing.T) {
	me := &MultiError{}
	err := errors.New("single error")
	me.Add(err)

	if me.Error() != "single error" {
		t.Errorf("single error should return just the error message")
	}
}

func TestSentinelErrors(t *testing.T) {
	// Verify sentinel errors are distinct
	sentinels := []error{
		ErrTimeout,
		ErrConnectionFailed,
		ErrInvalidConfig,
		ErrNoData,
		ErrPermissionDenied,
		ErrExtensionMissing,
	}

	for i, err1 := range sentinels {
		for j, err2 := range sentinels {
			if i != j && errors.Is(err1, err2) {
				t.Errorf("sentinel errors should be distinct: %v == %v", err1, err2)
			}
		}
	}
}

func TestMultiErrorEmpty(t *testing.T) {
	me := &MultiError{}
	if me.Error() != "no errors" {
		t.Errorf("empty MultiError.Error() should return 'no errors'")
	}
	if me.Unwrap() != nil {
		t.Error("empty MultiError.Unwrap() should return nil")
	}
}
