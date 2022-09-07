package main

import "athena-example/service"

func main() {
	service.ExecuteQuery("SELECT * FROM log_center.es_fail limit 5")
}
