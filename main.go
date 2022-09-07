package main

import "athena-example/service"

func main() {
	service.ExecuteQuery("SELECT * FROM your_database_name.your_table_name limit 5")
}
