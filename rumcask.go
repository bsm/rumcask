package rumcask

import "encoding/binary"

var _MAGIC = []byte{'R', 'U', 'M', 'C', 'A', 'S', 'K'}

const VERSION uint8 = 1

// Size helper constants
const (
	KiB = 1024
	MiB = 1024 * KiB
	GiB = 1024 * MiB
)

// Limits
const (
	// Maximum number of pages: 65535
	MAX_PAGE_COUNT = (1 << 16) - 1
	// Maximum size of each file: <512M
	MAX_PAGE_SIZE = 512*MiB - 1
	// Maximum key length: 511 bytes
	MAX_KEY_LEN = 511
	// Maximum value length: 64M
	MAX_VALUE_LEN = 64*MiB - 1
	// Page header length
	PAGE_HEADER_LEN = 128
)

var binLE = binary.LittleEndian
