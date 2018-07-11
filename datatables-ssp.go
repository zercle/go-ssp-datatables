package datatables

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
	// Import mysql driver
	_ "github.com/go-sql-driver/mysql"
)

type Request struct {
	Draw    int             `json:"draw"`
	Columns []RequestColumn `json:"columns"`
	Start   int             `json:"start"`
	Length  int             `json:"length"`
	Order   []RequestOrder  `json:"order"`
	Search  RequestSearch   `json:"search"`
}

type RequestColumn struct {
	Data       string
	Name       string
	Searchable bool
	Orderable  bool
	Search     RequestSearch
}

type RequestOrder struct {
	Column int    `json:"column"`
	Dir    string `json:"dir"`
}

type RequestSearch struct {
	Value string `json:"value"`
	Regex string `json:"regx"`
}

type ColumnInStruct map[string]string

type DataStruct struct {
	DataItem map[string]string
}

type ColumnOutStruct struct {
	Row map[string]string
}

type OutPutStruct struct {
	Draw            int               `json:"draw"`
	RecordsTotal    int               `json:"recordsTotal"`
	RecordsFiltered int               `json:"recordsFiltered"`
	Data            []ColumnOutStruct `json:"data"`
}

type DBConnOptions struct {
	Driver             string
	Username, Password string
	Host               string
	Port               int
	UnixSocket         string
	DatabaseName       string
}

var dbConn *sqlx.DB

func dataOutPut(comlumns []ColumnInStruct, data []DataStruct) (out []ColumnOutStruct) {
	for dataIndex, dataItem := range data {
		var row map[string]string
		for _, comlumnItem := range comlumns {
			row[comlumnItem["dt"]] = dataItem.DataItem[comlumnItem["db"]]
		}
		out[dataIndex].Row = row
	}
	return out
}

func db(conn *sqlx.DB, connOptions *DBConnOptions) {
	if conn == nil {
		sqlConnect(connOptions)
	} else {
		dbConn = conn
	}
}

func limit(request Request) (limit string) {
	if (request.Start != -1) && (request.Length != -1) {
		limit = "LIMIT " + strconv.Itoa(request.Start) + ", " + strconv.Itoa(request.Length)
	}
	return limit
}

func order(request Request, columns []ColumnInStruct) (order string) {
	if len(request.Order) > 0 {
		var orderBy []string
		dtColumns := pluck(columns, "dt")

		for _, orderItem := range request.Order {
			columnIdx := orderItem.Column
			requestColumn := request.Columns[columnIdx]

			columnIdx = indexOf(requestColumn.Data, dtColumns)
			column := columns[columnIdx]

			if requestColumn.Orderable {
				var dir string
				if orderItem.Dir == "asc" {
					dir = "ASC"
				} else {
					dir = "DESC"
				}
				orderBy = append(orderBy, "`"+column["db"]+"` "+dir)
			}
		}

		if len(orderBy) > 0 {
			order = strings.Join(orderBy, ", ")
		}
	}
	return order
}

func filter(request Request, columns []ColumnInStruct, bindings []string) (where string) {

	var globalSearch []string
	var columnsSearch []string
	dtColumns := pluck(columns, "dt")

	if request.Search.Value != "" {
		str := request.Search.Value

		for _, columnItem := range request.Columns {
			requestColumn := columnItem
			columnIdx := indexOf(columnItem.Data, dtColumns)
			column := columns[columnIdx]

			if requestColumn.Searchable {
				binding := bind(bindings, "%"+str+"%", "")
				globalSearch = append(globalSearch, "`"+column["db"]+"` LIKE "+binding)
			}
		}
	}

	// Individual column filtering
	if len(request.Columns) > 0 {
		for _, columnItem := range request.Columns {
			requestColumn := columnItem
			columnIdx := indexOf(columnItem.Data, dtColumns)
			column := columns[columnIdx]

			str := requestColumn.Search.Value

			if requestColumn.Searchable {
				binding := bind(bindings, "%"+str+"%", "")
				columnsSearch = append(columnsSearch, "`"+column["db"]+"` LIKE "+binding)
			}
		}
	}

	// Combine the filters into a single string
	if len(globalSearch) > 0 {
		where = "(" + strings.Join(globalSearch, " OR ") + ")"
	}
	if len(columnsSearch) > 0 {
		if len(where) > 0 {
			where = "(" + strings.Join(columnsSearch, " OR ") + ")"
		} else {
			where += " AND (" + strings.Join(columnsSearch, " OR ") + ")"
		}
	}
	if len(where) > 0 {
		where = "WHERE " + where
	}

	return where
}

func simple(request Request, conn *sqlx.DB, table string, primaryKey string, columns []ColumnInStruct) OutPutStruct {

	var bindings []string
	db(conn, nil)

	// Build the SQL query string from the request
	limit := limit(request)
	order := order(request, columns)
	where := filter(request, columns, bindings)

	// Main query to actually get the data
	var data []DataStruct
	err := dbConn.Select(&data, "SELECT `"+strings.Join(pluck(columns, "db"), "`, `")+"` FROM "+table+where+order+limit)
	if err != nil {

	}

	// Data set length after filtering
	recordsFiltered := 0
	err = dbConn.QueryRow("SELECT COUNT(`" + primaryKey + "`) FROM " + table + where).Scan(&recordsFiltered)
	if err != nil {

	}

	// Total data set length
	recordsTotal := 0
	err = dbConn.QueryRow("SELECT COUNT(`" + primaryKey + "`) FROM " + table + where).Scan(&recordsTotal)
	if err != nil {

	}

	// Output
	var output OutPutStruct
	if request.Draw > 0 {
		output.Draw = request.Draw
	} else {
		output.Draw = 0
	}
	output.RecordsTotal = recordsTotal
	output.RecordsFiltered = recordsFiltered
	output.Data = dataOutPut(columns, data)
	return output
}

func complex(request Request, conn *sqlx.DB, table string, primaryKey string, columns []ColumnInStruct, whereResult string, whereAll string) OutPutStruct {

	var bindings []string
	db(conn, nil)
	// var localWhereResult []string
	// var localWhereAll []string
	// var whereAllSql string

	// Build the SQL query string from the request
	limit := limit(request)
	order := order(request, columns)
	where := filter(request, columns, bindings)

	// whereResult = _flatten(localWhereResult, " AND ")
	// whereAll = _flatten(localWhereAll, " AND ")

	if len(whereResult) > 0 {
		if len(where) > 0 {
			where += " AND " + whereResult
		} else {
			where = "WHERE " + whereResult
		}
	}

	if len(whereAll) > 0 {
		if len(where) > 0 {
			where += " AND " + whereAll
		} else {
			where = "WHERE " + whereAll
		}
	}

	// Main query to actually get the data
	var data []DataStruct
	err := dbConn.Select(&data, "SELECT `"+strings.Join(pluck(columns, "db"), "`, `")+"` FROM "+table+where+order+limit)
	if err != nil {

	}

	// Data set length after filtering
	recordsFiltered := 0
	err = dbConn.QueryRow("SELECT COUNT(`" + primaryKey + "`) FROM " + table + where).Scan(&recordsFiltered)
	if err != nil {

	}

	// Total data set length
	recordsTotal := 0
	err = dbConn.QueryRow("SELECT COUNT(`" + primaryKey + "`) FROM " + table + where).Scan(&recordsTotal)
	if err != nil {

	}

	// Output
	var output OutPutStruct
	if request.Draw > 0 {
		output.Draw = request.Draw
	} else {
		output.Draw = 0
	}
	output.RecordsTotal = recordsTotal
	output.RecordsFiltered = recordsFiltered
	output.Data = dataOutPut(columns, data)
	return output
}

func sqlConnect(connOptions *DBConnOptions) error {
	if len(connOptions.Host) == 0 {
		return fmt.Errorf("sqlConnect: Database host needed")
	}
	connString := connOptions.dataStoreString()
	connString = connString + "?parseTime=true"

	// Open connection
	conn, err := sqlx.Connect("mysql", connString)
	if err != nil {
		return err
	}
	// Check connection
	if err := conn.Ping(); err != nil {
		conn.Close()
		return err
	}
	dbConn = conn
	return nil
}

func fatal(err error) ([]byte, error) {
	var errorResp struct {
		Error string `json:"error"`
	}
	errorResp.Error = fmt.Sprintf("%+v", err)
	return json.Marshal(errorResp)
}

func bind(a []string, val string, t string) (binding string) {
	return binding
}

func pluck(a []ColumnInStruct, prop string) (out []string) {
	for arrayIndex, arrayItem := range a {
		out[arrayIndex] = arrayItem[prop]
	}
	return out
}

func _flatten(a []string, join string) (out string) {
	if len(a) > 0 {
		return ""
	}
	out = strings.Join(a, join)
	return out
}

func indexOf(word string, data []string) int {
	for index, item := range data {
		if word == item {
			return index
		}
	}
	return -1
}

func (c DBConnOptions) dataStoreString() string {
	var cred string
	// [username[:password]@]
	if c.Username != "" {
		cred = c.Username
		if c.Password != "" {
			cred = cred + ":" + c.Password
		}
		cred = cred + "@"
	}

	if c.UnixSocket != "" {
		return fmt.Sprintf("%sunix(%s)/%s", cred, c.UnixSocket, c.DatabaseName)
	}
	return fmt.Sprintf("%stcp([%s]:%d)/%s", cred, c.Host, c.Port, c.DatabaseName)
}
