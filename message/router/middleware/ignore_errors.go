package middleware

import (
	"github.com/blastbao/watermill/message"
	"github.com/pkg/errors"
)

// IgnoreErrors provides a middleware that makes the handler ignore some explicitly whitelisted errors.
type IgnoreErrors struct {
	ignoredErrors map[string]struct{}
}

// NewIgnoreErrors creates a new IgnoreErrors middleware.
func NewIgnoreErrors(errs []error) IgnoreErrors {
	errsMap := make(map[string]struct{}, len(errs))

	for _, err := range errs {
		errsMap[err.Error()] = struct{}{}
	}

	return IgnoreErrors{errsMap}
}

func (i IgnoreErrors) Middleware(h message.HandlerFunc) message.HandlerFunc {
	return func(msg *message.Message) ([]*message.Message, error) {
		msgs, err := h(msg)
		if err != nil {
			if _, ok := i.ignoredErrors[errors.Cause(err).Error()]; ok {
				return msgs, nil
			}
			return msgs, err
		}
		return msgs, nil
	}
}
