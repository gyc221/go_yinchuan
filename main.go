package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/jmoiron/sqlx"
)

//DbConfig DbConfig
type DbConfig struct {
	IPAddr  string
	Port    uint16
	User    string
	Pwd     string
	DBName  string
	Charset string
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func getIDList(db *sqlx.DB, tagNameList string) (string, string, string, string) {
	if len(tagNameList) < 2 {
		return "", "", "", ""
	}
	sql := "select plc_id,point_id,description from datapointconfig where tag_name='%s'"
	aryTagName := make([]string, 0)
	aryPlcID := make([]string, 0)
	aryPointID := make([]string, 0)
	aryTagDesc := make([]string, 0)
	for _, v := range strings.Split(tagNameList, ",") {
		rows, err := db.Query(fmt.Sprintf(sql, v))
		checkError(err)
		defer rows.Close()

		for rows.Next() {
			var plcid, pointid, desc string
			rows.Scan(&plcid, &pointid, &desc)
			if len(plcid) < 1 || len(pointid) < 1 {
				fmt.Println("----------------------", v, " not find!------------------------")
				continue
			}
			aryTagName = append(aryTagName, v)
			aryPlcID = append(aryPlcID, plcid)
			aryPointID = append(aryPointID, pointid)
			aryTagDesc = append(aryTagDesc, desc)
		}
	}
	return strings.Join(aryTagName, ","), strings.Join(aryPlcID, ","), strings.Join(aryPointID, ","), strings.Join(aryTagDesc, ",")
}

func saveOneTagName(db *sqlx.DB, stationID, stationName, tagName, tagType, tagTypeType, desc string) {
	tagNameList, plcIDList, pointIDList, tagDescList := getIDList(db, tagName)
	if len(tagNameList) > 1 {
		db.Exec("insert into temporary_station_tag_name_final values(?,?,?,?,?,?,?,?,?)", stationID, stationName, tagType, tagTypeType, desc,
			tagNameList, tagDescList, plcIDList, pointIDList)
		fmt.Println(stationID, stationName, tagNameList, tagDescList, plcIDList, pointIDList)
	}
}

//从李袁星整理的表里面统计水耗 电耗 热耗 对应的TagName
func convertWaterHeatElec(db *sqlx.DB) {
	sql1 := `select 
	ifnull(a.aId,''),
	ifnull(b.NAM,''),
	ifnull(a.waterSupplyTags,''),
	ifnull(a.heatConsumeTags,''),
	ifnull(a.powerConsumption,'') 
	from orgtaginfo a left join basis_org b on a.aId=b.ID
	where b.NAM is not null`
	rows, err := db.Query(sql1)
	checkError(err)
	defer rows.Close()
	for rows.Next() {
		var stationID, stationName, water, heat, elec string
		rows.Scan(&stationID, &stationName, &water, &heat, &elec)
		if len(stationName) < 2 {
			continue
		}
		// water
		saveOneTagName(db, stationID, stationName, water, "water_consume", "cumulative", "水耗,累积值")
		//heat
		saveOneTagName(db, stationID, stationName, heat, "heat_consume", "cumulative", "热耗,累积值")
		// elec
		saveOneTagName(db, stationID, stationName, elec, "elec_consume", "cumulative", "电耗,累积值")
	}
}

//从我自己整理的表里面统计除水耗 电耗 热耗 以外的TagName
func convertElseType(db *sqlx.DB) {
	sql1 := `select 
	ifnull(station_id,''),
	ifnull(station_name,''),
	ifnull(buy_heat,''),
	ifnull(one_net_water_consume,''),
	ifnull(two_net_water_consume,''),
	ifnull(valve_open_degree,''),
	ifnull(sec_send_water_temp,''),
	ifnull(sec_ret_water_temp,''),
	ifnull(sec_net_flow,''),
	ifnull(three_net_send_temp,''),
	ifnull(three_net_ret_temp,'')
	from temporary_station_tag_name_xxxxxx order by station_id asc`
	rows, err := db.Query(sql1)
	checkError(err)
	defer rows.Close()
	for rows.Next() {
		var stationID, stationName, buyHeat, oneNetWaterConsume, towNetWaterConsume, valveOpenDegree, secSendWaterTemp, secRetWaterTemp, secNetFlow, threeNetSendTemp, threeNetRetTemp string
		rows.Scan(&stationID, &stationName, &buyHeat, &oneNetWaterConsume, &towNetWaterConsume, &valveOpenDegree, &secSendWaterTemp, &secRetWaterTemp, &secNetFlow, &threeNetSendTemp, &threeNetRetTemp)
		if len(stationName) < 2 {
			continue
		}
		// buy_heat
		saveOneTagName(db, stationID, stationName, buyHeat, "buy_heat", "cumulative", "购热量,累积值")

		// one_net_water_consume
		saveOneTagName(db, stationID, stationName, oneNetWaterConsume, "one_net_water_consume", "cumulative", "一网补水量,累积值")

		// two_net_water_consume
		saveOneTagName(db, stationID, stationName, towNetWaterConsume, "two_net_water_consume", "cumulative", "二网补水量,累积值")

		// valve_open_degree
		saveOneTagName(db, stationID, stationName, valveOpenDegree, "valve_open_degree", "runtime", "电调阀开度,实时值")

		// sec_send_water_temp
		saveOneTagName(db, stationID, stationName, secSendWaterTemp, "sec_send_water_temp", "runtime", "二次网供水温度,实时值")

		// sec_ret_water_temp
		saveOneTagName(db, stationID, stationName, secRetWaterTemp, "sec_ret_water_temp", "runtime", "二次网回水温度,实时值")

		// sec_net_flow
		saveOneTagName(db, stationID, stationName, secNetFlow, "sec_net_flow", "cumulative", "二网流量,累积值")

		// three_net_send_temp
		saveOneTagName(db, stationID, stationName, threeNetSendTemp, "three_net_send_temp", "runtime", "三次网供水温度,实时值")

		// three_net_ret_temp
		saveOneTagName(db, stationID, stationName, threeNetRetTemp, "three_net_ret_temp", "runtime", "三次网回水温度,实时值")
	}
}

func addFixedStationAndTagName(db *sqlx.DB) {
	// guan_sun
	stationID, stationName := "2", "首站"
	db.Exec("insert into temporary_station_tag_name_final values(?,?,?,?,?,?,?,?,?)", stationID, stationName, "guan_sun", "guansun", "管损,管损值", "", "", "", "")
	fmt.Println("添加管损成功!")
}

//插入虚拟的TagName
func createVirtualTagName(db *sqlx.DB) {
	sql1 := `insert into temporary_station_tag_name_final
	select 
	a.station_id,
	a.station_name,
	'heat_consume' as tag_type,
	'cumulative' as tag_type_type,
	'热耗,累积值' as tag_type_desc,
	concat('VIRTUAL_HEAT_CONSUME_',a.station_id) as tag_name_list,
	'热耗' as desc1,
	200000 as plc_id_list,
	100000000+a.station_id as point_list
	
	from 
	(
	select a.id as station_id,a.nam as station_name,b.tag_type from basis_org a left join temporary_station_tag_name_final b on a.id=b.station_id and b.tag_type='heat_consume' where  a.id >46 and b.tag_type is null
	) a
	
	union all
	
	select 
	a.station_id,
	a.station_name,
	'water_consume' as tag_type,
	'cumulative' as tag_type_type,
	'水耗,累积值' as tag_type_desc,
	concat('VIRTUAL_WATER_CONSUME_',a.station_id) as tag_name_list,
	'水耗' as desc1,
	200000 as plc_id_list,
	200000000+a.station_id as point_list
	
	from 
	(
	select a.id as station_id,a.nam as station_name,b.tag_type from basis_org a left join temporary_station_tag_name_final b on a.id=b.station_id and b.tag_type='water_consume' where  a.id >46 and b.tag_type is null
	) a
	
	union all
	
	select 
	a.station_id,
	a.station_name,
	'elec_consume' as tag_type,
	'cumulative' as tag_type_type,
	'电耗,累积值' as tag_type_desc,
	concat('VIRTUAL_ELEC_CONSUME_',a.station_id) as tag_name_list,
	'电耗' as desc1,
	200000 as plc_id_list,
	300000000+a.station_id as point_list
	
	from 
	(
	select a.id as station_id,a.nam as station_name,b.tag_type from basis_org a left join temporary_station_tag_name_final b on a.id=b.station_id and b.tag_type='elec_consume' where  a.id >46 and b.tag_type is null
	) a
	
	union all
	
	select 
	a.station_id,
	a.station_name,
	'salt_consume' as tag_type,
	'cumulative' as tag_type_type,
	'盐耗,累积值' as tag_type_desc,
	concat('VIRTUAL_SALT_CONSUME_',a.station_id) as tag_name_list,
	'盐耗' as desc1,
	200000 as plc_id_list,
	400000000+a.station_id as point_list
	
	from 
	(
	select a.id as station_id,a.nam as station_name,b.tag_type from basis_org a left join temporary_station_tag_name_final b on a.id=b.station_id and b.tag_type='salt_consume' where  a.id >46 and b.tag_type is null
	) a`
	_, err := db.Exec(sql1)
	fmt.Println("创建虚拟点", err)
}
func main() {
	file, err := os.Open("./dbconfig.json")
	checkError(err)
	defer file.Close()
	bs, err := ioutil.ReadAll(file)
	checkError(err)

	dbconfig := &DbConfig{}
	err = json.Unmarshal(bs, dbconfig)
	var db *sqlx.DB = connectMysql(dbconfig)
	defer db.Close()

	db.Exec("truncate table temporary_station_tag_name_final")
	convertWaterHeatElec(db)
	convertElseType(db)
	addFixedStationAndTagName(db)
	//createVirtualTagName(db)
}
