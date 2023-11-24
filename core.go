package mysql

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
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
	init        *bool
	Type        string
	Host        string
	Port        string
	ModeInit    *MysqlModeSettings
	ModeDefault *MysqlModeSettings
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

const (
	FlagInit            = "initmysql"
	FlagInitDescription = "To initialize database"
)

// ================================================================
//
// ================================================================
func New() (*Mysql, error) {
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
		init: flag.Bool(FlagInit, false, FlagInitDescription),
		Type: os.Getenv("DB_TYPE"),
		Host: os.Getenv("DB_HOST"),
		Port: os.Getenv("DB_PORT"),
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
func (e Mysql) IsInit() bool {
	return *e.init
}

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
