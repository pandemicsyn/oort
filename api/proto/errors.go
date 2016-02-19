package proto

import (
	"errors"

	"github.com/gholt/store"
)

func TranslateError(err error) string {
	switch err {
	case store.ErrDisabled:
		return "::github.com/gholt/store/ErrDisabled::"
	case store.ErrNotFound:
		return "::github.com/gholt/store/ErrNotFound::"
	}
	return err.Error()
}

func TranslateErrorString(errstr string) error {
	switch errstr {
	case "::github.com/gholt/store/ErrDisabled::":
		return store.ErrDisabled
	case "::github.com/gholt/store/ErrNotFound::":
		return store.ErrNotFound
	}
	return errors.New(errstr)
}
