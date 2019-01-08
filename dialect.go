package fsc

import (
	"github.com/viant/dsc"
	"google.golang.org/api/iterator"
	"reflect"
)

var maxRecordColumnScan = 20

type dialect struct{ dsc.DatastoreDialect }

func (d *dialect) GetKeyName(manager dsc.Manager, datastore, table string) string {
	config := manager.Config()
	if keyColumn := manager.Config().GetString(table+"."+pkColumnKey, ""); keyColumn != "" {
		return keyColumn
	}
	return config.GetString(pkColumnKey, "id")
}

//GetColumns returns estimated column for supplied table
func (d *dialect) GetColumns(manager dsc.Manager, datastore, table string) ([]dsc.Column, error) {
	var result = make([]dsc.Column, 0)
	connection, err := manager.ConnectionProvider().Get()
	if err != nil {
		return result, err
	}
	defer connection.Close()
	client, ctx, err := asClient(connection)
	if err != nil {
		return result, err
	}
	collection := client.Collection(table).Limit(maxRecordColumnScan)
	var columns = map[string]bool{}
	iter := collection.Documents(ctx)
	for i := 0; i < maxRecordColumnScan; i++ {
		doc, err := iter.Next()

		if err != nil {
			if err == iterator.Done {
				break
			}
			return nil, err
		}
		data := doc.Data()
		if len(data) == 0 {
			break
		}
		for k := range data {
			if val, has := columns[k]; !has {
				columns[k] = true
				typeName := reflect.TypeOf(val).Name()
				result = append(result, dsc.NewSimpleColumn(k, typeName))
			}
		}
	}
	return result, nil
}

//DropTable deletes table
func (d *dialect) DropTable(manager dsc.Manager, datastore string, table string) error {
	connection, err := manager.ConnectionProvider().Get()
	if err != nil {
		return err
	}
	defer connection.Close()
	client, ctx, err := asClient(connection)
	if err != nil {
		return err
	}
	_, err = client.Doc(table).Delete(ctx)
	return err
}

//GetDatastores returns data stores
func (d *dialect) GetDatastores(manager dsc.Manager) ([]string, error) {
	store, err := d.GetCurrentDatastore(manager)
	return []string{store}, err
}

//GetCurrentDatastore returns current datastore
func (d *dialect) GetCurrentDatastore(manager dsc.Manager) (string, error) {
	config := manager.Config()
	return config.Get(databaseURLKey), nil
}

//GetTables returns tables
func (d *dialect) GetTables(manager dsc.Manager, datastore string) ([]string, error) {
	var result = []string{}
	connection, err := manager.ConnectionProvider().Get()
	if err != nil {
		return result, err
	}
	defer connection.Close()
	store, ctx, err := asClient(connection)
	if err != nil {
		return result, err
	}
	iter := store.Collections(ctx)
	if err != nil {
		return result, err
	}
	references, err := iter.GetAll()
	if err != nil {
		return result, err
	}
	for _, item := range references {
		result = append(result, item.Path)
	}
	return result, nil
}

//CanPersistBatch returns false
func (d *dialect) CanPersistBatch() bool {
	return false
}

func newDialect() dsc.DatastoreDialect {
	var resut dsc.DatastoreDialect = &dialect{dsc.NewDefaultDialect()}
	return resut
}
