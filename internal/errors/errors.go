// Package errors provides typed errors for pghealth operations.
//
// This package defines sentinel errors and error types that allow callers
// to handle specific error conditions programmatically using errors.Is()
// and errors.As().
//
// Sentinel Errors:
//   - ErrTimeout: operation timed out
//   - ErrConnectionFailed: database connection failed
//   - ErrInvalidConfig: configuration validation failed
//   - ErrNoData: no data available for analysis
//   - ErrPermissionDenied: insufficient database privileges
//   - ErrExtensionMissing: required PostgreSQL extension not installed
//
// Typed Errors:
//   - CollectionError: wraps errors during data collection
//   - ValidationError: wraps configuration/input validation errors
//   - QueryError: wraps database query errors
//   - ReportError: wraps report generation errors
//   - MultiError: aggregates multiple errors
package errors

import (
	"errors"
	"fmt"
)

// Sentinel errors for common error conditions.
// Use errors.Is() to check for these conditions.
var (
	// ErrTimeout indicates an operation exceeded its time limit.
	ErrTimeout = errors.New("operation timed out")

	// ErrConnectionFailed indicates the database connection could not be established.
	ErrConnectionFailed = errors.New("database connection failed")

	// ErrInvalidConfig indicates configuration validation failed.
	ErrInvalidConfig = errors.New("invalid configuration")

	// ErrNoData indicates no data was available for the requested operation.
	ErrNoData = errors.New("no data available")

	// ErrPermissionDenied indicates insufficient database privileges.
	ErrPermissionDenied = errors.New("permission denied")

	// ErrExtensionMissing indicates a required PostgreSQL extension is not installed.
	ErrExtensionMissing = errors.New("required extension missing")
)

// CollectionError represents an error during data collection.
// It includes the operation that failed and whether partial results are available.
type CollectionError struct {
	Op      string // Operation that failed (e.g., "query tables", "fetch indexes")
	Err     error  // Underlying error
	Partial bool   // True if partial results were collected before error
}

// NewCollectionError creates a new CollectionError.
func NewCollectionError(op string, err error, partial bool) *CollectionError {
	return &CollectionError{Op: op, Err: err, Partial: partial}
}

// Error implements the error interface.
func (e *CollectionError) Error() string {
	prefix := "collection error"
	if e.Partial {
		prefix = "partial collection error"
	}
	return fmt.Sprintf("%s in %s: %v", prefix, e.Op, e.Err)
}

// Unwrap returns the underlying error for errors.Is/As support.
func (e *CollectionError) Unwrap() error {
	return e.Err
}

// Is reports whether target matches this error type.
func (e *CollectionError) Is(target error) bool {
	_, ok := target.(*CollectionError)
	return ok
}

// ValidationError represents a configuration or input validation error.
type ValidationError struct {
	Field   string // Field that failed validation
	Value   string // Value that was invalid (may be redacted for sensitive fields)
	Message string // Human-readable validation message
}

// NewValidationError creates a new ValidationError.
func NewValidationError(field, value, message string) *ValidationError {
	return &ValidationError{Field: field, Value: value, Message: message}
}

// Error implements the error interface.
func (e *ValidationError) Error() string {
	if e.Value == "" {
		return fmt.Sprintf("invalid %s: %s", e.Field, e.Message)
	}
	return fmt.Sprintf("invalid %s %q: %s", e.Field, e.Value, e.Message)
}

// Unwrap returns ErrInvalidConfig for errors.Is support.
func (e *ValidationError) Unwrap() error {
	return ErrInvalidConfig
}

// Is reports whether target matches this error type.
func (e *ValidationError) Is(target error) bool {
	_, ok := target.(*ValidationError)
	return ok
}

// QueryError represents a database query error.
type QueryError struct {
	Query string // SQL query (may be truncated for long queries)
	Err   error  // Underlying database error
}

// queryMaxLen is the maximum length of a query string in error messages.
const queryMaxLen = 100

// NewQueryError creates a new QueryError.
// Long queries are automatically truncated.
func NewQueryError(query string, err error) *QueryError {
	if len(query) > queryMaxLen {
		query = query[:queryMaxLen] + "..."
	}
	return &QueryError{Query: query, Err: err}
}

// Error implements the error interface.
func (e *QueryError) Error() string {
	return fmt.Sprintf("query failed [%s]: %v", e.Query, e.Err)
}

// Unwrap returns the underlying error for errors.Is/As support.
func (e *QueryError) Unwrap() error {
	return e.Err
}

// Is reports whether target matches this error type.
func (e *QueryError) Is(target error) bool {
	_, ok := target.(*QueryError)
	return ok
}

// ReportError represents an error during report generation.
type ReportError struct {
	Phase string // Phase that failed (e.g., "template", "render", "write")
	Path  string // Output path (if applicable)
	Err   error  // Underlying error
}

// NewReportError creates a new ReportError.
func NewReportError(phase, path string, err error) *ReportError {
	return &ReportError{Phase: phase, Path: path, Err: err}
}

// Error implements the error interface.
func (e *ReportError) Error() string {
	if e.Path == "" {
		return fmt.Sprintf("report %s error: %v", e.Phase, e.Err)
	}
	return fmt.Sprintf("report %s error for %s: %v", e.Phase, e.Path, e.Err)
}

// Unwrap returns the underlying error for errors.Is/As support.
func (e *ReportError) Unwrap() error {
	return e.Err
}

// Is reports whether target matches this error type.
func (e *ReportError) Is(target error) bool {
	_, ok := target.(*ReportError)
	return ok
}

// MultiError aggregates multiple errors into a single error.
// This is useful when multiple operations can fail independently.
type MultiError struct {
	Errors []error
}

// Add appends an error to the collection. Nil errors are ignored.
func (me *MultiError) Add(err error) {
	if err != nil {
		me.Errors = append(me.Errors, err)
	}
}

// Error implements the error interface.
func (me *MultiError) Error() string {
	switch len(me.Errors) {
	case 0:
		return "no errors"
	case 1:
		return me.Errors[0].Error()
	default:
		return fmt.Sprintf("%d errors occurred; first: %v", len(me.Errors), me.Errors[0])
	}
}

// Unwrap returns the first error for errors.Is/As support.
func (me *MultiError) Unwrap() error {
	if len(me.Errors) == 0 {
		return nil
	}
	return me.Errors[0]
}

// ErrorOrNil returns nil if no errors were added, otherwise returns the MultiError.
func (me *MultiError) ErrorOrNil() error {
	if len(me.Errors) == 0 {
		return nil
	}
	return me
}
