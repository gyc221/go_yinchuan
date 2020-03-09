package main

import (
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

func connectMysql(dbconfig *DbConfig) *sqlx.DB {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s",
		dbconfig.User, dbconfig.Pwd, dbconfig.IPAddr, dbconfig.Port, dbconfig.DBName, dbconfig.Charset)
	Db, err := sqlx.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("mysql connect failed, detail is [%v]", err.Error())
	}
	return Db
}

func queryData(Db *sqlx.DB) []string {
	rows, err := Db.Query(fmt.Sprintf("select COLUMN_NAME,DATA_TYPE from information_schema.COLUMNS where TABLE_SCHEMA='configdb'"))
	if err != nil {
		fmt.Printf("query faied, error:[%v]", err.Error())
		return nil
	}
	defer rows.Close()
	rets := make([]string, 0)
	for rows.Next() {
		var colName, dataType string
		err := rows.Scan(&colName, &dataType)
		if err != nil {
			fmt.Printf("get data failed, error:[%v]", err.Error())
		}
		rets = append(rets, colName)
	}
	return rets
}

func addRecord(Db *sqlx.DB) {
	for i := 0; i < 2; i++ {
		result, err := Db.Exec("insert into userinfo  values(?,?,?,?,?,?)", 0, "2019-07-06 11:45:20", "johny", "123456", "技术部", "123456@163.com")
		if err != nil {
			fmt.Printf("data insert faied, error:[%v]", err.Error())
			return
		}
		id, _ := result.LastInsertId()
		fmt.Printf("insert success, last id:[%d]\n", id)
	}
}

func updateRecord(Db *sqlx.DB) {
	//更新uid=1的username
	result, err := Db.Exec("update userinfo set username = 'anson' where uid = 1")
	if err != nil {
		fmt.Printf("update faied, error:[%v]", err.Error())
		return
	}
	num, _ := result.RowsAffected()
	fmt.Printf("update success, affected rows:[%d]\n", num)
}

func deleteRecord(Db *sqlx.DB) {
	//删除uid=2的数据
	result, err := Db.Exec("delete from userinfo where uid = 2")
	if err != nil {
		fmt.Printf("delete faied, error:[%v]", err.Error())
		return
	}
	num, _ := result.RowsAffected()
	fmt.Printf("delete success, affected rows:[%d]\n", num)
}
