package mysql

import (
	"database/sql"
	"errors"
	"fmt"
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

type ListQueryParamInterface interface {
	KeyLimit() string
	KeyOffset() string
}

type Page struct {
	stmt        *sqlx.NamedStmt
	args        map[string]any
	limit       int
	previous    int
	next        int
	hasPrevious bool
	hasNext     bool
	KeyLimit    string
	KeyOffset   string
}

func NewPageHandler(db *sqlx.DB, query string, params ListQueryParamInterface) (*Page, error) {
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
		KeyLimit:    params.KeyLimit(),
		KeyOffset:   params.KeyOffset(),
	}, nil
}

func (h *Page) Select(rows any, args map[string]any) error {
	val, ok := args[h.KeyLimit]
	if ok {
		if limit, ok := val.(int); !ok {
			return ErrInvalidLimit
		} else if limit < 1 {
			return ErrInvalidLimit
		} else {
			if h.args == nil {
				h.limit = limit
				args[h.KeyLimit] = limit + 1
			}
		}

		if val, ok := args[h.KeyOffset]; ok {
			if offset, ok := val.(int); !ok {
				return ErrInvalidOffset
			} else if offset < 0 {
				return ErrInvalidOffset
			}
		} else {
			args[h.KeyOffset] = 0
		}

		h.args = args
	}

	if err := h.stmt.Select(rows, args); err != nil {
		return err
	}

	if h.args != nil {
		offset := h.args[h.KeyOffset].(int)
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
		h.args[h.KeyOffset] = h.previous
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
		h.args[h.KeyOffset] = h.next
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
type PagingQueryParamInterface interface {
	ListQueryParamInterface
	Filters() map[string]any
}

type Paging struct {
	*Page
	Previous *string        `json:"previous,omitempty"`
	Next     *string        `json:"next,omitempty"`
	endpoint *url.URL       `json:"-"`
	filters  map[string]any `json:"-"`
}

func NewPaging(db *sqlx.DB, query string, endpoint *url.URL, params PagingQueryParamInterface) (*Paging, error) {
	page, err := NewPageHandler(db, query, params)
	if err != nil {
		return nil, err
	}

	filters := params.Filters()

	delete(filters, page.KeyLimit)
	delete(filters, page.KeyOffset)

	return &Paging{
		Page:     page,
		endpoint: endpoint,
		filters:  filters,
	}, nil
}

func (p *Paging) Select(rows *[]any, args map[string]any) error {
	if err := p.Page.Select(rows, args); err != nil {
		return err
	}
	defer p.Page.Close()
	p.setPagingUrl()

	return nil
}

func ToStringDeep(a any) string {
	val := reflect.ValueOf(a)
	for val.Kind() == reflect.Ptr && !val.IsNil() {
		val = val.Elem()
	}

	if val.Kind() == reflect.Ptr && val.IsNil() {
		return ""
	}

	return fmt.Sprintf("%v", val.Interface())
}

func (p *Paging) setPagingUrl() {
	q := p.endpoint.Query()
	for k, v := range p.filters {
		q.Set(k, ToStringDeep(v))
	}

	if limit, offset, err := p.GetPrevious(); err == nil {
		q.Set(p.Page.KeyLimit, strconv.Itoa(limit))
		q.Set(p.Page.KeyOffset, strconv.Itoa(offset))
		p.endpoint.RawQuery = q.Encode()
		urlstring := p.endpoint.String()
		p.Previous = &urlstring
	}

	if limit, offset, err := p.GetNext(); err == nil {
		q.Set(p.Page.KeyLimit, strconv.Itoa(limit))
		q.Set(p.Page.KeyOffset, strconv.Itoa(offset))
		p.endpoint.RawQuery = q.Encode()
		urlstring := p.endpoint.String()
		p.Next = &urlstring
	}
}
