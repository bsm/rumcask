package rumcask

import "strconv"

const (
	// DB errors
	ERROR_DB_LOCKED Error = -100

	// Page errors
	ERROR_PAGE_INVALID    Error = -200
	ERROR_PAGE_BAD_HEADER Error = -201

	// KV errors
	ERROR_NOT_FOUND      Error = -300
	ERROR_BAD_OFFSET     Error = -301
	ERROR_BAD_CHECKSUM   Error = -302
	ERROR_KEY_BLANK      Error = -303
	ERROR_KEY_TOO_LONG   Error = -304
	ERROR_VALUE_BLANK    Error = -305
	ERROR_VALUE_TOO_LONG Error = -306
)

type Error int

// Error implements the error interface
func (e Error) Error() string {
	code := int(e)
	if msg, ok := errorMessages[code]; ok {
		return "rumcask: " + msg
	}
	return "rumcask: unknown error (" + strconv.Itoa(code) + ")"
}

var errorMessages = map[int]string{
	-100: "database directory is locked by another process",

	-200: "invalid page",
	-201: "invalid page header",

	-300: "not found",
	-301: "invalid offset",
	-302: "invalid checksum",
	-303: "key cannot be blank",
	-304: "key length exceeds limit",
	-305: "value cannot be blank",
	-306: "value length exceeds limit",
}
