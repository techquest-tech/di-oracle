package ora

import (
	"context"
	"database/sql"
	"time"

	ora "github.com/sijms/go-ora/v2"
	"github.com/spf13/viper"
	"github.com/techquest-tech/gin-shared/pkg/core"
	"go.uber.org/zap"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type OracleSetting struct {
	Host          string
	Port          int
	Service       string
	User          string
	Password      string
	Mustconnected bool // should test oracle connection when bootup
}

func init() {
	core.GetContainer().Provide(func(logger *zap.Logger) (*sql.DB, error) {
		settings := viper.Sub("oracle")
		oracleSetting := &OracleSetting{
			Host:    "127.0.0.1",
			Port:    1521,
			Service: "xe",
		}
		if settings != nil {
			settings.Unmarshal(oracleSetting)
		}

		// fullurl := ora.BuildJDBC(oracleSetting.User, oracleSetting.Password, oracleSetting.URI, map[string]string{})
		fullurl := ora.BuildUrl(oracleSetting.Host, oracleSetting.Port, oracleSetting.Service,
			oracleSetting.User, oracleSetting.Password, map[string]string{})
		db, err := sql.Open("oracle", fullurl)

		if err != nil {
			logger.Error("connect to oracle failed.", zap.Error(err))
			return nil, err
		}
		logger.Info("connect to oracle", zap.String("URI", fullurl))

		if oracleSetting.Mustconnected {
			ctx, cancel := context.WithTimeout(context.TODO(), 2*time.Second)
			defer cancel()

			err = db.PingContext(ctx)
			if err != nil {
				logger.Error("ping oracle failed.", zap.Error(err))
				return nil, err
			} else {
				logger.Info("ping oracle done.")
			}
		}

		return db, nil
	})
	core.Container.Provide(ToGorm)
}

//make sql.DB to gorm.DB, but only support gorm.Raw, nothing else.
func ToGorm(db *sql.DB) (*gorm.DB, error) {
	return gorm.Open(mysql.New(mysql.Config{
		Conn: db,
	}))
}
