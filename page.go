package mysql

import (
	"database/sql"
	"errors"
	"reflect"

	"github.com/jmoiron/sqlx"
)

// ================================================================
//
// ================================================================
const (
	KeyLimit  = "l"
	KeyOffset = "o"
)

var (
	ErrInvalidContainer = errors.New("Invalid container")
	ErrInvalidLimit     = errors.New("Invalid limit")
	ErrInvalidOffset    = errors.New("Invalid offset")
	ErrBOF              = errors.New("BOF")
	ErrEOF              = errors.New("EOF")
)

// ================================================================
type PageHandler interface {
	Select(rows *[]any, args map[string]any) error
	HasPrevious() bool
	GetPrevious() (int, int, error)
	SelectPrevious(rows *[]any) error
	HasNext() bool
	GetNext() (int, int, error)
	SelectNext(rows *[]any) error
	Close()
}

type Page struct {
	stmt        *sqlx.NamedStmt
	args        map[string]any
	limit       int
	previous    int
	next        int
	hasPrevious bool
	hasNext     bool
}

func NewPageHandler(db *sqlx.DB, query string) (*Page, error) {
	stmt, err := db.PrepareNamed(query)
	if err != nil {
		return nil, err
	}

	return &Page{
		stmt:        stmt,
		args:        nil,
		limit:       0,
		previous:    0,
		next:        0,
		hasPrevious: false,
		hasNext:     false,
	}, nil
}

func (h *Page) Select(rows any, args map[string]any) error {
	val, ok := args[KeyLimit]
	if ok {
		if limit, ok := val.(int); !ok {
			return ErrInvalidLimit
		} else if limit < 1 {
			return ErrInvalidLimit
		} else {
			if h.args == nil {
				h.limit = limit
				args[KeyLimit] = limit + 1
			}
		}

		if val, ok := args[KeyOffset]; ok {
			if offset, ok := val.(int); !ok {
				return ErrInvalidOffset
			} else if offset < 0 {
				return ErrInvalidOffset
			}
		} else {
			args[KeyOffset] = 0
		}

		h.args = args
	}

	if err := h.stmt.Select(rows, args); err != nil {
		return err
	}

	if h.args != nil {
		offset := h.args[KeyOffset].(int)
		// calc previous
		if offset > 0 {
			if h.previous = offset - h.limit; h.previous < 0 {
				h.previous = 0
			}
			h.hasPrevious = true
		} else {
			h.previous = 0
			h.hasPrevious = false
		}

		container := reflect.ValueOf(rows)
		if container.Kind() != reflect.Ptr {
			return ErrInvalidContainer
		}
		container = container.Elem()
		if container.Kind() != reflect.Slice {
			return ErrInvalidContainer
		}

		// calc next
		numRows := container.Len()
		if numRows > h.limit {
			container.SetLen(h.limit)
			h.next = offset + h.limit
			h.hasNext = true
		} else {
			container.SetLen(numRows)
			h.next = offset + numRows
			h.hasNext = false
		}
	}

	return nil
}

// ================================================================
func (h Page) HasPrevious() bool {
	return h.hasPrevious
}

func (h Page) GetPrevious() (int, int, error) {
	var err error
	if !h.hasPrevious {
		err = ErrBOF
	}
	return h.limit, h.previous, err
}

func (h *Page) SelectPrevious(rows *[]any) error {
	if h.hasPrevious {
		h.args[KeyOffset] = h.previous
		return h.Select(rows, h.args)
	}

	return sql.ErrNoRows
}

// ================================================================
func (h Page) HasNext() bool {
	return h.hasNext
}

func (h Page) GetNext() (int, int, error) {
	var err error
	if !h.hasNext {
		err = ErrEOF
	}
	return h.limit, h.next, err
}

func (h *Page) SelectNext(rows *[]any) error {
	if h.hasNext {
		h.args[KeyOffset] = h.next
		return h.Select(rows, h.args)
	}

	return sql.ErrNoRows
}

// ================================================================
func (h *Page) Close() {
	if h.stmt != nil {
		h.stmt.Close()
	}
}

// ================================================================
type Paging struct {
	Previous *string `json:"previous,omitempty"`
	Next     *string `json:"next,omitempty"`
}
