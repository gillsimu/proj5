package surfstore

import (
	"fmt"
)

var ERR_SERVER_CRASHED = fmt.Errorf("server is crashed.")
var ERR_NOT_LEADER = fmt.Errorf("server is not the leader")
var UNKOWN_ERROR = fmt.Errorf("unkown error: some issue with cluster")
var SERVER_REJECTED_ERROR = fmt.Errorf("error: Server rejected to process the commit")
