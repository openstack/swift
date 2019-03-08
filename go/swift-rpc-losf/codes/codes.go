package codes

type StatusCode int

//go:generate stringer -type=StatusCode
const (
	Ok                 StatusCode = 200
	Cancelled          StatusCode = 299
	InvalidArgument    StatusCode = 400
	NotFound           StatusCode = 404
	AlreadyExists      StatusCode = 409
	PermissionDenied   StatusCode = 403
	FailedPrecondition StatusCode = 412
	Unimplemented      StatusCode = 501
	Internal           StatusCode = 500
	Unavailable        StatusCode = 503
)
