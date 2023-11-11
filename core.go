package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

// ================================================================
//
// ================================================================
type Mysql struct {
	*sqlx.DB
	AutoCreateDBSchema bool
	Type               string
	Host               string
	Port               string
	ModeInit           *MysqlModeSettings
	ModeDefault        *MysqlModeSettings
}

type MysqlModeSettings struct {
	User     string
	Password string
	Name     string
	Params   string
	MaxOpen  int
	MaxIdle  int
	LifeTime int
	IdleTime int
}

// ================================================================
//
// ================================================================
func New() (*Mysql, error) {
	autoCreateDBSchema, err := strconv.ParseBool(os.Getenv("AUTO_CREATE_DB_SCHEMA"))
	if err != nil {
		return nil, err
	}

	maxOpen, err := strconv.Atoi(os.Getenv("DB_MAX_OPEN"))
	if err != nil {
		return nil, err
	}

	maxIdle, err := strconv.Atoi(os.Getenv("DB_MAX_IDLE"))
	if err != nil {
		return nil, err
	}

	lifeTime, err := strconv.Atoi(os.Getenv("DB_LIFE_TIME"))
	if err != nil {
		return nil, err
	}

	idleTime, err := strconv.Atoi(os.Getenv("DB_IDLE_TIME"))
	if err != nil {
		return nil, err
	}

	return &Mysql{
		AutoCreateDBSchema: autoCreateDBSchema,
		Type:               os.Getenv("DB_TYPE"),
		Host:               os.Getenv("DB_HOST"),
		Port:               os.Getenv("DB_PORT"),
		ModeInit: &MysqlModeSettings{
			User:     os.Getenv("DB_INIT_USER"),
			Password: os.Getenv("DB_INIT_PASSWORD"),
			Name:     "",
			Params:   os.Getenv("DB_INIT_PARAMS"),
			MaxOpen:  1,
			MaxIdle:  1,
			LifeTime: 30,
			IdleTime: 30,
		},
		ModeDefault: &MysqlModeSettings{
			User:     os.Getenv("DB_USER"),
			Password: os.Getenv("DB_PASSWORD"),
			Name:     os.Getenv("DB_NAME"),
			Params:   os.Getenv("DB_PARAMS"),
			MaxOpen:  maxOpen,
			MaxIdle:  maxIdle,
			LifeTime: lifeTime,
			IdleTime: idleTime,
		},
	}, nil
}

// ================================================================
//
// ================================================================
func (e *Mysql) Open() error {
	var err error
	e.Close()
	e.DB, err = e.connectWithMode(false)
	return err
}

func (e *Mysql) Close() {
	if e.DB != nil {
		e.DB.Close()
	}
}

// ================================================================
//
// ================================================================
func (e Mysql) DBInit(sqlDir string, sortedFiles []string) error {
	db, err := e.connectWithMode(true)
	if err != nil {
		return err
	}
	defer db.Close()

	hasDB := false
	if err := db.Get(&hasDB, `SELECT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ?);`, e.ModeDefault.Name); err != nil {
		return err
	} else if hasDB {
		return nil
	}

	if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS `" + e.ModeDefault.Name + "` COLLATE 'utf8mb4_unicode_ci' CHARACTER SET 'utf8mb4';"); err != nil {
		return err
	} else {
		db.Exec("USE `" + e.ModeDefault.Name + "`;")
	}

	if len(sortedFiles) > 0 {
		for i := range sortedFiles {
			if _, err = sqlx.LoadFile(db, filepath.Join(sqlDir, sortedFiles[i])); err != nil {
				break
			}
		}
	} else {
		err = filepath.Walk(sqlDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			} else if info.IsDir() || filepath.Ext(path) != ".sql" {
				return nil
			}

			_, err = sqlx.LoadFile(db, path)
			return err
		})
	}

	return err
}

// ================================================================
//
// ================================================================
func (e Mysql) connectWithMode(isInit bool) (*sqlx.DB, error) {
	var ms *MysqlModeSettings
	switch isInit {
	case true:
		ms = e.ModeInit
	default:
		ms = e.ModeDefault
	}

	if db, err := sqlx.Open(e.Type, fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?%s", ms.User, ms.Password, e.Host, e.Port, ms.Name, ms.Params)); err != nil {
		return nil, err
	} else {
		db.SetMaxOpenConns(ms.MaxOpen)
		db.SetMaxIdleConns(ms.MaxIdle)
		db.SetConnMaxLifetime(time.Duration(ms.LifeTime) * time.Second)
		db.SetConnMaxIdleTime(time.Duration(ms.IdleTime) * time.Second)
		return db, nil
	}
}

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
	h.stmt.Close()
}
