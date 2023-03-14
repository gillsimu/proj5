package surfstore

import (
	context "context"
	"fmt"
	// "math"

	// "log"

	// "math"
	"sync"
	"time"

	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64              // latest term server has seen (initialized to 0) on first boot, increases monotonically
	log           []*UpdateOperation // og entries; each entry contains command for state machine, and term when entry was received by lead

	metaStore *MetaStore

	// Added for discussion
	id             int64
	peers          []string
	pendingCommits []*chan bool
	commitIndex    int64 // ndex of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied    int64 // index of highest log entry applied to state machine (initialized to 0, increases monotonically

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) CheckPreConditions(checkLeader bool, checkServerHealth bool) error {
	// check if server is leader or not
	s.isLeaderMutex.RLock()
	defer s.isLeaderMutex.RUnlock()
	isServerLeader := s.isLeader

	if checkLeader && !isServerLeader {
		return ERR_NOT_LEADER
	}

	// check if server is crashed
	s.isCrashedMutex.RLock()
	defer s.isCrashedMutex.RUnlock()
	isCrashed := s.isCrashed

	if checkServerHealth && isCrashed {
		return ERR_SERVER_CRASHED
	}

	return nil
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	if err := s.CheckPreConditions(true, true); err != nil {
		return nil, err
	}

	// if the leader can query a majority quorum of the nodes, it will reply back to the client with the correct answer.
	// As long as a majority of the nodes are up and not in a crashed state, the clients should be able to interact with the system successfully.
	// When a majority of nodes are in a crashed state, clients should block and not receive a response until a majority are restored.
	// Any clients that interact with a non-leader should get an error message and retry to find the leader.
	for {
		if success, _ := s.SendHeartbeat(ctx, empty); success.Flag {
			return s.metaStore.GetFileInfoMap(ctx, empty)
		}
	}

}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	if err := s.CheckPreConditions(true, true); err != nil {
		return nil, err
	}

	// if the leader can query a majority quorum of the nodes, it will reply back to the client with the correct answer.
	// As long as a majority of the nodes are up and not in a crashed state, the clients should be able to interact with the system successfully.
	// When a majority of nodes are in a crashed state, clients should block and not receive a response until a majority are restored.
	// Any clients that interact with a non-leader should get an error message and retry to find the leader.
	for {
		if success, _ := s.SendHeartbeat(ctx, &emptypb.Empty{}); success.Flag {
			return s.metaStore.GetBlockStoreMap(ctx, hashes)
		}
	}
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	if err := s.CheckPreConditions(true, true); err != nil {
		return nil, err
	}
	// if the leader can query a majority quorum of the nodes, it will reply back to the client with the correct answer.
	// As long as a majority of the nodes are up and not in a crashed state, the clients should be able to interact with the system successfully.
	// When a majority of nodes are in a crashed state, clients should block and not receive a response until a majority are restored.
	// Any clients that interact with a non-leader should get an error message and retry to find the leader.
	for {
		if success, _ := s.SendHeartbeat(ctx, empty); success.Flag {
			return s.metaStore.GetBlockStoreAddrs(ctx, empty)
		}
	}
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	fmt.Println("------------------------------------ Request received to update file:")
	if err := s.CheckPreConditions(true, true); err != nil {
		fmt.Println("server requested on:", s.id, " is:", err)
		return nil, err
	}

	// append entry to our log
	s.log = append(s.log, &UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	})
	commitChan := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, &commitChan)

	// send entry to all followers in parallel
	go s.sendToAllFollowersInParallel(ctx)

	fmt.Println("------------------------------------ Hello:")
	// commit the entry once majority of followers have it in their log
	commit := <-commitChan
	fmt.Println("------------------------------------ commit:", commit)
	// once committed, apply to the state machine
	if commit {
		fmt.Println("------------------------------------ Updating file in metastore for:", s.id)
		s.lastApplied = s.commitIndex
		fmt.Println("Leader new commitIndex:", s.commitIndex, " lastApplied:", s.lastApplied, " term:", s.term)
		return s.metaStore.UpdateFile(ctx, filemeta)
	}
	// log.Fatalf("UNKOWN_ERROR")
	return nil, UNKOWN_ERROR
}

func (s *RaftSurfstore) sendToAllFollowersInParallel(ctx context.Context) {
	// send entry to all my followers and count the replies
	responses := make(chan bool, len(s.peers)-1)

	// contact all the follower, send some AppendEntries call
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}
		fmt.Println("Leader:", s.id, " server request sent to:", idx)
		go s.sendToFollower(ctx, addr, responses)
	}

	totalResponses := 1
	totalAppends := 1

	// wait in loop for responses
	for {
		result := <-responses
		totalResponses++
		if result {
			totalAppends++
		}
		if totalResponses == len(s.peers) {
			break
		}
	}

	fmt.Println("totalAppends:", totalAppends, " totalResponses:", totalResponses)
	if totalAppends > len(s.peers)/2 {
		*s.pendingCommits[len(s.pendingCommits)-1] <- true
		s.commitIndex = s.commitIndex + 1
	} else {
		*s.pendingCommits[len(s.pendingCommits)-1] <- false
	}
}

func (s *RaftSurfstore) sendToFollower(ctx context.Context, addr string, responses chan bool) {
	dummyAppendEntriesInput := AppendEntryInput{
		Term:         s.term,
		PrevLogTerm:  s.GetPreviousLogTerm(s.commitIndex),
		PrevLogIndex: s.commitIndex,
		Entries:      s.log, //TODO check this
		LeaderCommit: s.commitIndex,
	}

	for {
		// TODO check all errors
		if err := s.CheckPreConditions(false, true); err != nil {
			responses <- false //TODO
			// return;
			fmt.Println("Pre Check failed")
			continue
		}

		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			fmt.Println("Dialing failed:", err)
			responses <- false
			return
			// continue
		}

		client := NewRaftSurfstoreClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		appendEntryOutput, _ := client.AppendEntries(ctx, &dummyAppendEntriesInput)
		// fmt.Println("appendEntryOutput:", err.Error())
		// if appendEntryOutput == nil {
		// 	continue
		// }

		if appendEntryOutput != nil && appendEntryOutput.Success {
			fmt.Println("Success to append entries for server address:" , addr)
			responses <- true
			return
		}
		// else {
		// 	fmt.Println("Failure to append entries for server, ", s.id, " err:", err)
		// 	responses <- false
		// }
		// return
	}
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex
// whose term matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index
// but different terms), delete the existing entry and all that
// follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {

	if err := s.CheckPreConditions(false, true); err != nil {
		// fmt.Println(s.id, "Pre condition check failed: ", err)
		return &AppendEntryOutput{
			Success: false,
		}, ERR_SERVER_CRASHED
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if s.term > input.Term {
		fmt.Println(s.id, "ERROR: s.term", s.term, " > input.Term:", input.Term)
		return &AppendEntryOutput{
			Success: false,
		}, UNKOWN_ERROR
	}

	// • If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if input.Term > s.term {
		s.isLeaderMutex.Lock()
		fmt.Println(s.id, "Resetting since s.term", s.term, " < input.Term:", input.Term)
		s.isLeader = false
		s.term = input.Term
		s.isLeaderMutex.Unlock()
	}

	// TODO actually check entries
	lastIndexMatchesLogs := int64(len(s.log) - 1)
	for id, log := range s.log {
		if id >= len(input.Entries) {
			break
		}

		if log == input.Entries[id] {
			s.lastApplied = int64(id)
			lastIndexMatchesLogs = int64(id)
		} else {
			break
		}
	}

	s.log = s.log[:lastIndexMatchesLogs+1]
	if lastIndexMatchesLogs < int64(len(input.Entries)) {
		s.log = append(s.log, input.Entries[lastIndexMatchesLogs+1:]...)
	} else {
		s.log = append(s.log, make([]*UpdateOperation, 0)...)
	}
	fmt.Println(s.id, "lastIndexMatchesLogs", lastIndexMatchesLogs, "s.commitIndex:", s.commitIndex, "input.LeaderCommit:", input.LeaderCommit, " s.term", s.term, " Leader.Term:", input.Term)

	if s.commitIndex < input.LeaderCommit {
		if input.LeaderCommit < int64(len(s.log)-1) {
			s.commitIndex = input.LeaderCommit
		} else {
			s.commitIndex = int64(len(s.log)-1)
		}
	}

	// • If commitIndex > lastApplied: increment lastApplied, apply
	// log[lastApplied] to state machine (§5.3)
	for s.lastApplied < s.commitIndex {
		entry := s.log[s.lastApplied+1]
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		s.lastApplied++
	}

	// for s.lastApplied < input.LeaderCommit {
	// 	entry := s.log[s.lastApplied+1]
	// 	s.metaStore.UpdateFile(ctx, entry.FileMetaData)
	// 	s.lastApplied++
	// }
	fmt.Println(s.id, "Append entry to server successfull:", s.id)

	return &AppendEntryOutput{
		Success: true,
	}, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	if err := s.CheckPreConditions(false, true); err != nil {
		fmt.Println(s.id, "Server crashed: ", err)
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}
	s.isLeaderMutex.Lock()
	defer s.isLeaderMutex.Unlock()
	s.isLeader = true
	s.term++

	// TODO update state as per paper
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetPreviousLogTerm(commitIndex int64) int64 {
	if commitIndex == -1 {
		return 0
	}
	return s.log[s.commitIndex].Term
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	fmt.Println("------------------------------------ Sending heartbeat from leader:", s.id)

	if err := s.CheckPreConditions(true, true); err != nil {
		return &Success{Flag: false}, err
	}
	fmt.Println("leader: id:", s.id, " commitIndex:", s.commitIndex, "last applied:", s.lastApplied, "term:", s.term)
	dummyAppendEntriesInput := AppendEntryInput{
		Term:         s.term,
		PrevLogTerm:  s.GetPreviousLogTerm(s.commitIndex),
		PrevLogIndex: s.commitIndex,
		Entries:      s.log,
		LeaderCommit: s.commitIndex,
	}

	noOfNodesAlive := 1
	countOfMajorityNodes := len(s.peers) / 2

	// contact all the follower, send some AppendEntries call
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}

		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			fmt.Println(s.id, "Server dialing failed: ", err)
			return &Success{Flag: false}, nil
		}
		client := NewRaftSurfstoreClient(conn)
		
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		appendEntryOutput, _ := client.AppendEntries(ctx, &dummyAppendEntriesInput)
		if appendEntryOutput != nil && appendEntryOutput.Success {
			noOfNodesAlive++
		}
	}

	fmt.Println(s.id, "Nodes alive: ", noOfNodesAlive, " countOfMajorityNodes:", countOfMajorityNodes+1)
	if noOfNodesAlive > countOfMajorityNodes {
		return &Success{Flag: true}, nil
	}
	// log.Fatal("Failure")
	return &Success{Flag: false}, nil
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
