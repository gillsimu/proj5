package SurfTest

import (
	"cse224/proj5/pkg/surfstore"
	"testing"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

func TestRaftSetLeader(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	term := int64(1)
	var leader bool

	for idx, server := range test.Clients {
		if idx == leaderIdx {
			leader = bool(true)
		} else {
			leader = bool(false)
		}

		_, err := CheckInternalState(&leader, &term, nil, nil, server, test.Context)

		if err != nil {
			t.Fatalf("Error checking state for server %d: %s", idx, err.Error())
		}
	}

	leaderIdx = 2
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	term = int64(2)
	for idx, server := range test.Clients {
		if idx == leaderIdx {
			leader = bool(true)
		} else {
			leader = bool(false)
		}

		_, err := CheckInternalState(&leader, &term, nil, nil, server, test.Context)

		if err != nil {
			t.Fatalf("Error checking state for server %d: %s", idx, err.Error())
		}
	}
}

func TestRaftNewLeaderPushesUpdates(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenMeta := make(map[string]*surfstore.FileMetaData)

	

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	// for _, server := range test.Clients {
	// 	server.SendHeartbeat(test.Context, &emptypb.Empty{})
	// }

	
	test.Clients[1].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})

	// update a file on node 0
	filemeta := &surfstore.FileMetaData{
		Filename:      "testfile",
		Version:       1,
		BlockHashList: nil,
	}

	test.Clients[leaderIdx].UpdateFile(test.Context, filemeta)

	test.Clients[0].Crash(test.Context, &emptypb.Empty{})

	test.Clients[1].Restore(test.Context, &emptypb.Empty{})
	test.Clients[2].Restore(test.Context, &emptypb.Empty{})
	
	test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})

	term := int64(1)
	leader := bool(true)
	_, err := CheckInternalState(&leader, &term, goldenLog, goldenMeta, test.Clients[0], test.Context)

	if err != nil {
		t.Fatalf("Error checking state for server %d: %s", 0, err.Error())
	}
	
}

func TestRaftLogsConsistent(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenMeta := make(map[string]*surfstore.FileMetaData)

	

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	// for _, server := range test.Clients {
	// 	server.SendHeartbeat(test.Context, &emptypb.Empty{})
	// }

	
	test.Clients[1].Crash(test.Context, &emptypb.Empty{})

	// update a file on node 0
	filemeta := &surfstore.FileMetaData{
		Filename:      "testfile",
		Version:       1,
		BlockHashList: nil,
	}

	test.Clients[leaderIdx].UpdateFile(test.Context, filemeta)

	test.Clients[0].Crash(test.Context, &emptypb.Empty{})

	test.Clients[1].Restore(test.Context, &emptypb.Empty{})
	
	leaderIdx = 1
	test.Clients[1].SetLeader(test.Context, &emptypb.Empty{})
	// update a file on node 0
	filemeta2 := &surfstore.FileMetaData{
		Filename:      "testfile2",
		Version:       1,
		BlockHashList: nil,
	}

	test.Clients[leaderIdx].UpdateFile(test.Context, filemeta2)

	test.Clients[0].Restore(test.Context, &emptypb.Empty{})

	term := int64(1)
	leader := bool(true)
	_, err := CheckInternalState(&leader, &term, goldenLog, goldenMeta, test.Clients[0], test.Context)

	if err != nil {
		t.Fatalf("Error checking state for server %d: %s", 0, err.Error())
	}
	
}

func TestRaftRecoverable(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenMeta := make(map[string]*surfstore.FileMetaData)

	

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	// for _, server := range test.Clients {
	// 	server.SendHeartbeat(test.Context, &emptypb.Empty{})
	// }

	
	test.Clients[1].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})

	// update a file on node 0
	filemeta := &surfstore.FileMetaData{
		Filename:      "testfile",
		Version:       1,
		BlockHashList: nil,
	}

	test.Clients[leaderIdx].UpdateFile(test.Context, filemeta)

	test.Clients[2].Restore(test.Context, &emptypb.Empty{})

	test.Clients[1].Restore(test.Context, &emptypb.Empty{})

	term := int64(1)
	leader := bool(true)
	_, err := CheckInternalState(&leader, &term, goldenLog, goldenMeta, test.Clients[0], test.Context)

	if err != nil {
		t.Fatalf("Error checking state for server %d: %s", 0, err.Error())
	}
	
}

func TestRaftFollowersGetUpdates(t *testing.T) {
	t.Log("leader1 gets a request")
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// set node 0 to be the leader
	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// here node 0 should be the leader, all other nodes should be followers
	// all logs and metastores should be empty
	// ^ TODO check

	// update a file on node 0
	filemeta := &surfstore.FileMetaData{
		Filename:      "testfile",
		Version:       1,
		BlockHashList: nil,
	}

	test.Clients[leaderIdx].UpdateFile(test.Context, filemeta)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// one final call to sendheartbeat (from spec)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// check that all the nodes are in the right state
	// leader and all followers should have the entry in the log
	// everything should be term 1
	// entry should be applied to all metastores
	// only node 0 should be leader
	goldenMeta := make(map[string]*surfstore.FileMetaData)
	goldenMeta[filemeta.Filename] = filemeta

	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta,
	})

	term := int64(1)
	var leader bool
	for idx, server := range test.Clients {
		if idx == leaderIdx {
			leader = bool(true)
		} else {
			leader = bool(false)
		}

		_, err := CheckInternalState(&leader, &term, goldenLog, goldenMeta, server, test.Context)

		if err != nil {
			t.Fatalf("Error checking state for server %d: %s", idx, err.Error())
		}
	}
}
