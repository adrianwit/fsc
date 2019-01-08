package fsc

import (
	"github.com/viant/dsc"
)

func register() {
	dsc.RegisterManagerFactory("fsc", newManagerFactory())
	dsc.RegisterDatastoreDialect("fsc", newDialect())
}

func init() {
	register()
}
