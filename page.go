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

// ================================================================
type ListQueryParamsInterface interface {
	KeyLimit() string
	KeyOffset() string
}

// ================================================================
type ListArgsInterface interface {
	Subset() (int, int)
	SetLimit(int)
	SetOffset(int)
}

type ListQueryParams struct {
	Limit  int `form:"l" binding:"number,min=1" db:"l"`
	Offset int `form:"o" binding:"number,min=0" db:"o"`
}

func (qp ListQueryParams) KeyLimit() string {
	return "l"
}

func (qp ListQueryParams) KeyOffset() string {
	return "o"
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

func (qp ListQueryParams) Filters() map[string]string {
	return map[string]string{
		"l": strconv.Itoa(qp.Limit),
		"o": strconv.Itoa(qp.Offset),
	}
}

// ================================================================
type PageHandler interface {
	Select(rows *[]any, args map[string]any) error
	GetPrevious() (int, int, error)
	SelectPrevious(rows *[]any) error
	GetNext() (int, int, error)
	SelectNext(rows *[]any) error
	Close()
}

type Page struct {
	stmt        *sqlx.NamedStmt
	args        ListArgsInterface
	limit       int
	previous    int
	next        int
	hasPrevious bool
	hasNext     bool
	KeyLimit    string
	KeyOffset   string
}

func NewPageHandler(db *sqlx.DB, query string, params ListQueryParamsInterface) (*Page, error) {
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

func (h *Page) Select(rows any, args ListArgsInterface) error {
	if args != nil {
		l, o := args.Subset()
		if h.args == nil {
			if l < 1 {
				h.limit = 1
			} else {
				h.limit = l
			}
			args.SetLimit(h.limit + 1)

			if o < 0 {
				args.SetOffset(0)
			}

			h.args = args
		}
	}

	if err := h.stmt.Select(rows, args); err != nil {
		return err
	}

	if h.args != nil {
		// calc previous
		_, o := args.Subset()
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
func (h Page) GetPrevious() (int, int, error) {
	var err error
	if !h.hasPrevious {
		err = ErrBOF
	}
	return h.limit, h.previous, err
}

func (h *Page) SelectPrevious(rows *[]any) error {
	if h.hasPrevious {
		h.args.SetOffset(h.previous)
		return h.Select(rows, h.args)
	}

	return ErrBOF
}

// ================================================================
func (h Page) GetNext() (int, int, error) {
	var err error
	if !h.hasNext {
		err = ErrEOF
	}
	return h.limit, h.next, err
}

func (h *Page) SelectNext(rows *[]any) error {
	if h.hasNext {
		h.args.SetOffset(h.next)
		return h.Select(rows, h.args)
	}

	return ErrEOF
}

// ================================================================
func (h *Page) Close() {
	if h.stmt != nil {
		h.stmt.Close()
	}
}

// ================================================================
type PagingQueryParamInterface interface {
	ListQueryParamsInterface
	Filters() map[string]string
}

type Paging struct {
	*Page
	Previous    *string                   `json:"previous,omitempty"`
	Next        *string                   `json:"next,omitempty"`
	endpoint    *url.URL                  `json:"-"`
	queryParams PagingQueryParamInterface `json:"-"`
}

func NewPaging(db *sqlx.DB, query string, endpoint *url.URL, params PagingQueryParamInterface) (*Paging, error) {
	page, err := NewPageHandler(db, query, params)
	if err != nil {
		return nil, err
	}

	return &Paging{
		Page:        page,
		endpoint:    endpoint,
		queryParams: params,
	}, nil
}

func (p *Paging) Select(rows *[]any, args ListArgsInterface) error {
	defer p.Page.Close()
	if err := p.Page.Select(rows, args); err != nil {
		return err
	}
	p.setPagingUrl()

	return nil
}

func (p *Paging) setPagingUrl() {
	q := p.endpoint.Query()
	filters := p.queryParams.Filters()
	delete(filters, p.queryParams.KeyLimit())
	delete(filters, p.queryParams.KeyOffset())

	for k, v := range filters {
		q.Set(k, v)
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
