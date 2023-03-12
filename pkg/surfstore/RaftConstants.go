package surfstore

import (
	"fmt"
)

var ERR_SERVER_CRASHED = fmt.Errorf("Server is crashed.")
var ERR_NOT_LEADER = fmt.Errorf("Server is not the leader")
var UNKOWN_ERROR = fmt.Errorf("Unkown error: some issue with cluster")
