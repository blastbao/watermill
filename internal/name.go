package internal

import (
	"fmt"
	"strings"
)

func StructName(v interface{}) string {

	// if v has a member function of 'String()', call it directly
	if s, ok := v.(fmt.Stringer); ok {
		return s.String()
	}

	// else, use "%T" format it
	s := fmt.Sprintf("%T", v)

	// trim the pointer marker, if any
	return strings.TrimLeft(s, "*")
}
