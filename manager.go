package fsc

import (
	"cloud.google.com/go/firestore"
	"database/sql"
	"fmt"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"strings"
)

const (
	pkColumnKey = "keyColumn"
)

type config struct {
	*dsc.Config
	keyColumnName string
	dbName        string
}

type manager struct {
	*dsc.AbstractManager
	config *config
}

func (m *manager) getKeyColumn(table string) string {
	if keyColumn := m.config.GetString(table+"."+pkColumnKey, ""); keyColumn != "" {
		return keyColumn
	}
	return m.config.keyColumnName
}

func (m *manager) insert(client *firestore.Client, ctx context.Context, statement *dsc.DmlStatement, sqlParameters []interface{}) (err error) {
	parameters := toolbox.NewSliceIterator(sqlParameters)
	var record map[string]interface{}

	if record, err = statement.ColumnValueMap(parameters); err != nil {
		return err
	}
	keyColumn := m.getKeyColumn(statement.Table)
	id, ok := record[keyColumn]
	if !ok {
		return fmt.Errorf("missing value for %v", keyColumn)
	}
	pathRef := statement.Table
	ref := client.Collection(pathRef).Doc(toolbox.AsString(id))
	_, err = ref.Set(ctx, record)
	return err
}

func (m *manager) update(client *firestore.Client, ctx context.Context, statement *dsc.DmlStatement, sqlParameters []interface{}) (err error) {
	parameters := toolbox.NewSliceIterator(sqlParameters)
	var record map[string]interface{}
	if record, err = statement.ColumnValueMap(parameters); err != nil {
		return err
	}
	criteriaMap, err := m.criteria(statement.BaseStatement, parameters)
	keyColumn := m.getKeyColumn(statement.Table)
	id, ok := criteriaMap[keyColumn]
	if !ok {
		return fmt.Errorf("missing value for %v", keyColumn)
	}
	for k, v := range criteriaMap {
		record[k] = v
	}
	pathRef := statement.Table //+ "/" + toolbox.AsString(id)
	ref := client.Collection(pathRef).Doc(toolbox.AsString(id))
	if len(record) > 0 {
		var updates = make([]firestore.Update, 0)
		for k, v := range record {
			updates = append(updates, firestore.Update{
				Path:  strings.Replace(k, ".", "/", len(k)),
				Value: v,
			})
		}
		_, err = ref.Update(ctx, updates)
	}
	return err
}

func (m *manager) criteria(statement *dsc.BaseStatement, parameters toolbox.Iterator) (map[string]interface{}, error) {
	return asCriteriaMap(statement.SQLCriteria, parameters)

}

func (m *manager) runDelete(client *firestore.Client, ctx context.Context, statement *dsc.DmlStatement, sqlParameters []interface{}) (affected int, err error) {
	parameters := toolbox.NewSliceIterator(sqlParameters)
	criteriaMap, err := asCriteriaMap(statement.SQLCriteria, parameters)
	if err != nil {
		return 0, err
	}
	if len(criteriaMap) == 0 {
		pathRef := statement.Table
		_, err = client.Doc(pathRef).Delete(ctx)
		return 0, err
	}

	keyColumn := m.getKeyColumn(statement.Table)
	value, ok := criteriaMap[keyColumn]
	if !ok {
		return 0, fmt.Errorf("missing value for %v", keyColumn)
	}
	var ids = []interface{}{
		value,
	}
	if toolbox.IsSlice(value) {
		ids = toolbox.AsSlice(value)
	}
	var rowCount = 0
	for _, id := range ids {
		_, err :=  client.Collection(statement.Table).Doc(toolbox.AsString(id)).Delete(ctx)
		if err != nil {
			return 0, err
		}
		rowCount++
	}
	return rowCount, nil
}

func (m *manager) ExecuteOnConnection(connection dsc.Connection, sql string, sqlParameters []interface{}) (result sql.Result, err error) {
	dsc.Logf("[%v]:%v, %v\n", m.config.dbName, sql, sqlParameters)
	client, ctx, err := asClient(connection)
	if err != nil {
		return nil, err
	}
	parser := dsc.NewDmlParser()
	statement, err := parser.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse %v due to %v", sql, err)
	}
	var affectedRecords = 1
	switch statement.Type {
	case "INSERT":
		err = m.insert(client, ctx, statement, sqlParameters)
	case "UPDATE":
		err = m.update(client, ctx, statement, sqlParameters)
	case "DELETE":
		affectedRecords, err = m.runDelete(client, ctx, statement, sqlParameters)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to modify %v, %v", statement.Table, err)
	}
	return dsc.NewSQLResult(int64(affectedRecords), 0), nil
}

func (m *manager) enrichRecordIfNeeded(statement *dsc.QueryStatement, record map[string]interface{}) []string {
	var columns = make([]string, 0)
	for _, column := range statement.Columns {
		var name = column.Name
		if column.Alias != "" {
			if value, ok := record[name]; ok {
				delete(record, name)
				record[column.Alias] = value
			}
			name = column.Alias
		}
		columns = append(columns, name)
	}

	return columns
}

func (m *manager) ReadAllOnWithHandlerOnConnection(connection dsc.Connection, SQL string, SQLParameters []interface{}, readingHandler func(scanner dsc.Scanner) (toContinue bool, err error)) error {
	dsc.Logf("[%v]:%v, %v\n", m.config.dbName, SQL, SQLParameters)
	parser := dsc.NewQueryParser()
	statement, err := parser.Parse(SQL)
	if err != nil {
		return fmt.Errorf("failed to parse statement %v, %v", SQL, err)
	}
	parameters := toolbox.NewSliceIterator(SQLParameters)
	criteriaMap, err := m.criteria(statement.BaseStatement, parameters)
	if err != nil {
		return err
	}
	scanner := dsc.NewSQLScanner(statement, m.Config(), nil)
	if len(criteriaMap) == 1 {
		client, ctx, err := asClient(connection)
		if err != nil {
			return err
		}

		keyColumn := m.getKeyColumn(statement.Table)
		value, ok := criteriaMap[keyColumn]
		if !ok {
			return fmt.Errorf("missing value for %v", keyColumn)
		}
		var ids = []interface{}{
			value,
		}
		if toolbox.IsSlice(value) {
			ids = toolbox.AsSlice(value)
		}
		for _, id := range ids {
			pathRef := statement.Table
			document := client.Collection(pathRef).Doc(toolbox.AsString(id))
			scanner.Values = map[string]interface{}{}
			snapshot, err := document.Get(ctx)
			if err != nil {
				if grpc.Code(err) == codes.NotFound {
					continue
				}
				return err
			}
			scanner.Values = snapshot.Data()
			if cont, err := readingHandler(scanner); err != nil || !cont {
				return err
			}
		}
	} else if len(criteriaMap) == 0 {
		store, ctx, err := asClient(connection)
		if err != nil {
			return err
		}
		iter := store.Collection(statement.Table).Documents(ctx)

		for {

			doc, err := iter.Next()
			if err != nil {
				if err == iterator.Done {
					break
				}
				return err
			}
			scanner.Values = doc.Data()
			if cont, err := readingHandler(scanner); err != nil || !cont {
				return err
			}
		}

	} else {
		return fmt.Errorf("unupported criteria: %v", criteriaMap)
	}
	return nil
}

func newConfig(conf *dsc.Config) (*config, error) {
	var keyColumnName = conf.GetString(pkColumnKey, "id")
	return &config{
		Config:        conf,
		keyColumnName: keyColumnName,
	}, nil
}
