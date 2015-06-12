package operations

// The Operation interface represents a REST operation.
type Operation interface {
	// Params supplies an interface that will be populated by restapi.Unmarshal()
	// prior to calling Call()
	Params() interface{}

	// Call returns the result of the REST operation.
	Call() (interface{}, error)
}
