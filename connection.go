package fsc

import (
	"cloud.google.com/go/firestore"
	"context"
	"firebase.google.com/go"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"google.golang.org/api/option"
)

const (
	storageBucketKey = "storageBucket"
	projectIDKey     = "projectID"
	databaseURLKey   = "databaseURL"
	dbnameKey        = "dbname"
)

//AppPointer represents an app pointer context key
var AppPointerKey = (*firebase.App)(nil)

//ClientPointerKey represents an client pointer key
var ClientPointerKey = (*firestore.Client)(nil)

//ClientPointerKey represents an context pointer key
var ContextPointerKey = (*context.Context)(nil)

func asClient(connection dsc.Connection) (*firestore.Client, context.Context, error) {
	client := connection.Unwrap(ClientPointerKey).(*firestore.Client)
	ctx := connection.Unwrap(ContextPointerKey).(*context.Context)
	return client, *ctx, nil
}

type connection struct {
	*dsc.AbstractConnection
	client    *firestore.Client
	ctx       *context.Context
	cancelCtx context.CancelFunc
	dbName    string
}

func (c *connection) CloseNow() error {
	c.cancelCtx()
	return nil
}

func (c *connection) Unwrap(targetType interface{}) interface{} {
	if targetType == ClientPointerKey {
		return c.client
	} else if targetType == ContextPointerKey {
		return c.ctx
	}
	panic(fmt.Sprintf("unsupported targetType type %v", targetType))
}

type connectionProvider struct {
	*dsc.AbstractConnectionProvider
}

func (p *connectionProvider) NewConnection() (dsc.Connection, error) {
	config := p.ConnectionProvider.Config()
	var err error
	firebaseConfig := &firebase.Config{
		DatabaseURL:   config.Get(databaseURLKey),
		ProjectID:     config.Get(projectIDKey),
		StorageBucket: config.Get(storageBucketKey),
	}
	if firebaseConfig.ProjectID == "" {
		return nil, errors.New("projectID was empty")
	}
	//if firebaseConfig.StorageBucket == "" {
	//	return nil, errors.New("storageBucket was empty")
	//}

	var credentials option.ClientOption
	if config.Credentials != "" {
		credentials = option.WithCredentialsFile(config.Credentials)
	}
	ctx, cancel := context.WithCancel(context.Background())

	var conn = &connection{ctx: &ctx, cancelCtx: cancel}
	if firebaseConfig.DatabaseURL != "" {
		app, err := firebase.NewApp(ctx, firebaseConfig, credentials)
		if err != nil {
			return nil, err
		}
		conn.client, err = app.Firestore(ctx)
		if err != nil {
			return nil, err
		}
		_, conn.dbName = toolbox.URLSplit(firebaseConfig.DatabaseURL)
	} else {
		conn.client, err = firestore.NewClient(ctx, firebaseConfig.ProjectID, credentials)
		if err != nil {
			return nil, err
		}
		conn.dbName = config.Get(dbnameKey)
	}
	var super = dsc.NewAbstractConnection(config, p.ConnectionProvider.ConnectionPool(), conn)
	conn.AbstractConnection = super
	return conn, nil
}

func newConnectionProvider(config *dsc.Config) dsc.ConnectionProvider {
	if config.MaxPoolSize == 0 {
		config.MaxPoolSize = 1
	}
	aerospikeConnectionProvider := &connectionProvider{}
	var connectionProvider dsc.ConnectionProvider = aerospikeConnectionProvider
	var super = dsc.NewAbstractConnectionProvider(config, make(chan dsc.Connection, config.MaxPoolSize), connectionProvider)
	aerospikeConnectionProvider.AbstractConnectionProvider = super
	aerospikeConnectionProvider.AbstractConnectionProvider.ConnectionProvider = connectionProvider
	return aerospikeConnectionProvider
}
