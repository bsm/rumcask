package rumcask

import "strconv"

var (
	ERROR_DB_LOCKED        Error = -100
	ERROR_INVALID_OFFSET   Error = -101
	ERROR_INVALID_CHECKSUM Error = -102
	ERROR_KEY_TOO_LONG     Error = -103
	ERROR_VALUE_TOO_LONG   Error = -103

	ERROR_PAGE_INVALID    Error = -200
	ERROR_PAGE_BAD_HEADER Error = -201

	NOT_FOUND Error = -900
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
	-101: "invalid offset",
	-102: "invalid checksum",
	-103: "key too long",
	-104: "value too long",

	-200: "invalid page",
	-201: "invalid page header",

	-900: "not found",
}
