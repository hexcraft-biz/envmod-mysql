package mysql

import (
	"errors"
	"net/url"
	"reflect"
	"strconv"

	"github.com/jmoiron/sqlx"
)

// ================================================================
//
// ================================================================
var (
	ErrInvalidContainer = errors.New("Invalid container")
	ErrInvalidLimit     = errors.New("Invalid limit")
	ErrInvalidOffset    = errors.New("Invalid offset")
	ErrBOF              = errors.New("BOF")
	ErrEOF              = errors.New("EOF")
)

const (
	DefaultLimit = 64
)

// ================================================================
type ListArgsInterface interface {
	Subset() (int, int)
	SetLimit(int)
	SetOffset(int)
	GetNamedArgs() map[string]any
}

type ListQueryParams struct {
	Limit  int `form:"l" binding:"number" db:"l"`
	Offset int `form:"o" binding:"number" db:"o"`
}

func (qp ListQueryParams) Subset() (int, int) {
	return qp.Limit, qp.Offset
}

func (qp *ListQueryParams) SetLimit(limit int) {
	qp.Limit = limit
}

func (qp *ListQueryParams) SetOffset(offset int) {
	qp.Offset = offset
}

func (qp ListQueryParams) GetNamedArgs() map[string]any {
	return map[string]any{
		"l": qp.Limit,
		"o": qp.Offset,
	}
}

func (qp ListQueryParams) SubsetKeys() (string, string) {
	return "l", "o"
}

func (qp ListQueryParams) Filters() map[string]string {
	return map[string]string{
		"l": strconv.Itoa(qp.Limit),
		"o": strconv.Itoa(qp.Offset),
	}
}

// ================================================================
type SubsetInterface interface {
	Select(rows any, args ListArgsInterface) error
	GetPrevious() (int, int, error)
	GetNext() (int, int, error)
	MoveBackward() error
	MoveForward() error
	Close()
}

type Subset struct {
	stmt        *sqlx.NamedStmt
	args        ListArgsInterface
	limit       int
	previous    int
	next        int
	hasPrevious bool
	hasNext     bool
}

func NewSubset(db *sqlx.DB, query string) (*Subset, error) {
	stmt, err := db.PrepareNamed(query)
	if err != nil {
		return nil, err
	}

	return &Subset{
		stmt:        stmt,
		args:        nil,
		limit:       0,
		previous:    0,
		next:        0,
		hasPrevious: false,
		hasNext:     false,
	}, nil
}

func (h *Subset) Select(rows any, args ListArgsInterface) error {
	if args != nil && h.args == nil {
		l, o := args.Subset()
		if l < 1 {
			h.limit = DefaultLimit
		} else {
			h.limit = l
		}
		args.SetLimit(h.limit + 1)

		if o < 0 {
			args.SetOffset(0)
		}

		h.args = args
	}

	if err := h.stmt.Select(rows, args.GetNamedArgs()); err != nil {
		return err
	}

	if h.args != nil {
		// calc previous
		_, o := h.args.Subset()
		if o > 0 {
			if h.previous = o - h.limit; h.previous < 0 {
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
			h.next = o + h.limit
			h.hasNext = true
		} else {
			container.SetLen(numRows)
			h.next = o + numRows
			h.hasNext = false
		}
	}

	return nil
}

// ================================================================
func (h Subset) GetPrevious() (int, int, error) {
	var err error
	if !h.hasPrevious {
		err = ErrBOF
	}
	return h.limit, h.previous, err
}

func (h Subset) GetNext() (int, int, error) {
	var err error
	if !h.hasNext {
		err = ErrEOF
	}
	return h.limit, h.next, err
}

func (h *Subset) MoveBackward() error {
	if h.hasPrevious {
		h.args.SetOffset(h.previous)
		return nil
	}

	return ErrBOF
}

func (h *Subset) MoveForward() error {
	if h.hasNext {
		h.args.SetOffset(h.next)
		return nil
	}

	return ErrEOF
}

// ================================================================
func (h *Subset) Close() {
	if h.stmt != nil {
		h.stmt.Close()
	}
}

// ================================================================
type PagingQueryParamInterface interface {
	SubsetKeys() (string, string)
	Filters() map[string]string
}

type Paging struct {
	*Subset
	Previous    *string                   `json:"previous,omitempty" example:"https://myhostname/foo/bar?l=32&o=0"`
	Next        *string                   `json:"next,omitempty" example:"https://myhostname/foo/bar?l=32&o=64"`
	endpoint    *url.URL                  `json:"-"`
	queryParams PagingQueryParamInterface `json:"-"`
}

func NewPaging(db *sqlx.DB, query string, endpoint *url.URL, params PagingQueryParamInterface) (*Paging, error) {
	subset, err := NewSubset(db, query)
	if err != nil {
		return nil, err
	}

	endpoint.RawQuery = ""

	return &Paging{
		Subset:      subset,
		endpoint:    endpoint,
		queryParams: params,
	}, nil
}

func (p *Paging) Select(rows any, args ListArgsInterface) error {
	defer p.Subset.Close()
	if err := p.Subset.Select(rows, args); err != nil {
		return err
	}

	q := p.endpoint.Query()
	filters := p.queryParams.Filters()
	keyLimit, keyOffset := p.queryParams.SubsetKeys()
	delete(filters, keyLimit)
	delete(filters, keyOffset)

	for k, v := range filters {
		q.Set(k, v)
	}

	if limit, offset, err := p.GetPrevious(); err == nil {
		q.Set(keyLimit, strconv.Itoa(limit))
		q.Set(keyOffset, strconv.Itoa(offset))
		p.endpoint.RawQuery = q.Encode()
		urlstring := p.endpoint.String()
		p.Previous = &urlstring
	}

	if limit, offset, err := p.GetNext(); err == nil {
		q.Set(keyLimit, strconv.Itoa(limit))
		q.Set(keyOffset, strconv.Itoa(offset))
		p.endpoint.RawQuery = q.Encode()
		urlstring := p.endpoint.String()
		p.Next = &urlstring
	}

	return nil
}
