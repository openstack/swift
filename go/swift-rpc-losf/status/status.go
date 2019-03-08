package status

import (
	"fmt"
	"github.com/openstack/swift-rpc-losf/codes"
)

type RpcError struct {
	code codes.StatusCode
	msg  string
}

func (e *RpcError) Error() string {
	return fmt.Sprintf("rpc error: %d. %s", e.code, e.msg)
}

func Error(code codes.StatusCode, msg string) error {
	return &RpcError{code, msg}
}

func Errorf(code codes.StatusCode, format string, a ...interface{}) error {
	return Error(code, fmt.Sprintf(format, a...))
}

func (e *RpcError) Code() codes.StatusCode {
	return e.code
}
