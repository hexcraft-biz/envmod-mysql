package mysql

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
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
	init          *bool
	Type          string
	Host          string
	Port          string
	Name          string
	DirSqls       string
	FlywayBinPath string
	ModeInit      *MysqlModeSettings
	ModeDefault   *MysqlModeSettings
}

type MysqlModeSettings struct {
	User     string
	Password string
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
		init:          flag.Bool(FlagInit, false, FlagInitDescription),
		Type:          os.Getenv("DB_TYPE"),
		Host:          os.Getenv("DB_HOST"),
		Port:          os.Getenv("DB_PORT"),
		Name:          os.Getenv("DB_NAME"),
		DirSqls:       os.Getenv("DIR_SQLS"),
		FlywayBinPath: os.Getenv("FLYWAY_BIN_PATH"),
		ModeInit: &MysqlModeSettings{
			User:     os.Getenv("DB_INIT_USER"),
			Password: os.Getenv("DB_INIT_PASSWORD"),
			Params:   os.Getenv("DB_INIT_PARAMS"),
			MaxOpen:  1,
			MaxIdle:  1,
			LifeTime: 30,
			IdleTime: 30,
		},
		ModeDefault: &MysqlModeSettings{
			User:     os.Getenv("DB_USER"),
			Password: os.Getenv("DB_PASSWORD"),
			Params:   os.Getenv("DB_PARAMS"),
			MaxOpen:  maxOpen,
			MaxIdle:  maxIdle,
			LifeTime: lifeTime,
			IdleTime: idleTime,
		},
	}, nil
}

func (e Mysql) FlywayMigrate() error {
	cmd := exec.Command(
		e.FlywayBinPath,
		"-url=jdbc:"+fmt.Sprintf("mysql://%s:%s/%s", e.Host, e.Port, e.Name),
		"-user="+e.ModeInit.User,
		"-password="+e.ModeInit.Password,
		"-locations=filesystem:"+e.DirSqls,
		"migrate",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func (e Mysql) FlywayClean() error {
	cmd := exec.Command(
		e.FlywayBinPath,
		"-url=jdbc:"+fmt.Sprintf("mysql://%s:%s/%s", e.Host, e.Port, e.Name),
		"-user="+e.ModeInit.User,
		"-password="+e.ModeInit.Password,
		"-locations=filesystem:"+e.DirSqls,
		"clean",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
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
func (e Mysql) DBInit(sqlDir string, sortedFiles []string) {
	if !*e.init {
		panic("not init mode")
	}

	db, err := e.connectWithMode(true)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	hasDB := false
	if err := db.Get(&hasDB, `SELECT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = ?);`, e.Name); err != nil {
		panic(err)
	} else if hasDB {
		return
	}

	if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS `" + e.Name + "` COLLATE 'utf8mb4_unicode_ci' CHARACTER SET 'utf8mb4';"); err != nil {
		panic(err)
	} else {
		db.Exec("USE `" + e.Name + "`;")
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

	if err != nil {
		panic(err)
	}
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

	connectionString := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?%s", ms.User, ms.Password, e.Host, e.Port, e.Name, ms.Params)
	db, err := sqlx.Open(e.Type, connectionString)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(ms.MaxOpen)
	db.SetMaxIdleConns(ms.MaxIdle)
	db.SetConnMaxLifetime(time.Duration(ms.LifeTime) * time.Second)
	db.SetConnMaxIdleTime(time.Duration(ms.IdleTime) * time.Second)

	err = db.Ping()
	if err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}
