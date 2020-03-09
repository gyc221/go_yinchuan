package main

import (
	"bytes"
	"fmt"
)

var triggerTemplate = `
-- ----------------------------------------------------------[tableName]------------------------------------------------------------
DROP TRIGGER IF EXISTS tri_insert_[tableName];
DELIMITER ;;
CREATE TRIGGER tri_insert_[tableName] AFTER INSERT ON [tableName] FOR EACH ROW 
begin
    declare final_str varchar(2048);
	set final_str=CONCAT('[',[newConcat],']');
	insert into log(ts,pri,title,msg) values(now(),9999,'{"op":"insert","tb":"[tableName]"}',final_str);
end
;;
DELIMITER ;

DROP TRIGGER IF EXISTS tri_delete_[tableName];
DELIMITER ;;
CREATE TRIGGER tri_delete_[tableName] AFTER DELETE ON [tableName] FOR EACH ROW 
begin
    declare final_str varchar(2048);
	set final_str=CONCAT('[',[oldConcat],']');
	insert into log(ts,pri,title,msg) values(now(),9999,'{"op":"delete","tb":"[tableName]"}',final_str);
end
;;
DELIMITER ;

DROP TRIGGER IF EXISTS tri_update_[tableName];
DELIMITER ;;
CREATE TRIGGER tri_update_[tableName] AFTER UPDATE ON [tableName] FOR EACH ROW 
begin
    declare final_old varchar(2048);
	declare final_new varchar(2048);
	set final_old=CONCAT([oldConcat]);
	set final_new=CONCAT([newConcat]);				
	insert into log(ts,pri,title,msg) values(now(),9999,'{"op":"update","tb":"[tableName]"}',CONCAT('[',final_old,',',final_new,']'));
end
;;
DELIMITER ;
`

func getConcat(s []string, prefix string) string {
	var buffer bytes.Buffer
	buffer.WriteString(`'{'`)
	for i, v := range s {
		if i == 0 {
			buffer.WriteString(fmt.Sprintf(`,'"%s":"',%s.%s,'"'`, v, prefix, v))
		} else {
			buffer.WriteString(fmt.Sprintf(`,',"%s":"',%s.%s,'"'`, v, prefix, v))
		}
	}
	buffer.WriteString(`,'}'`)
	return buffer.String()
}
