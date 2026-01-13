package main

/*
imports we want but werent using yet:
	"os"
	"strings"

*/

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"

	//"os"
	"sync"
	"time"

	// "fmt"
	"log"
	pb "mapreduce/protos"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// The initial job request stuff to send to etcd
type JobRequest struct {
	ID              string `json:"id"`
	SourceContainer string `json:"container"` // the files we want mapReduced
	NumMappers      int    `json:"num_mappers"`
	NumReducers     int    `json:"num_reducers"`
}

// This will maintain what the task is and its status
type MapTaskStatus struct {
	ID       int  `json:"id"`
	Complete bool `json:"complete"`
}

type ReduceTaskStatus struct {
	ID       int  `json:"id"`
	Complete bool `json:"complete"`
}

var (
	addr             = flag.String("A", "localhost:50051", "the address to connect to")
	id               = flag.String("id", "", "etcd leader election id")
	fail_count       = flag.String("fail", "", "number of attempts till fail")
	failCounter      int
	failThreshold    int
	m_amount         = 10
	source_container string
	r_amount         = 10
	etcdClient       *clientv3.Client
	jobID            string
	jCount           = 0

	mapStates    = make(map[string][]MapTaskStatus)    // jobID → []MapTaskStatus
	reduceStates = make(map[string][]ReduceTaskStatus) // jobID → []ReduceTaskStatus
	ipToJobs     = make(map[string][]int)              //pod ip -> []string representing all job ids assigned to that pod
	jobToTime    = make(map[int]time.Time)             //jobID -> time that job was started

	mutex sync.RWMutex

	syncListenAddr = flag.String("syncAddr", ":5050", "gRPC listen addr for master sync")

	masterIPs []string

	minShardSize int64 = 4096 // Making it a minimum of 4k bytes so small files don't use unnessacy sharding
)

type syncServer struct {
	pb.UnimplementedMasterSyncServer
}

// Uses DNS lookup on the Kubernetes headless service to find all master pod IPs in the cluster. Returns them with port 5050 appended for gRPC communication.
// This runs in a background go thread so it can detect new masters being added or removed
func discoverMasterIPs() []string {
	serviceName := "master-headless.mr.svc.cluster.local"

	ips, err := net.LookupHost(serviceName)
	if err != nil {
		log.Printf("This service name: %s failed to get IPs of masters", serviceName)
		return nil
	}

	var masters []string
	for _, ip := range ips {
		// Append the grpc port the masters use (5050)
		masters = append(masters, fmt.Sprintf("%s:5050", ip))
	}

	log.Printf("Master IPs: %v", masters)
	return masters
}

// Background goroutine that refreshes the list of master IPs every 30 seconds to detect new masters or failures.
func startMasterLoop() {
	for {
		time.Sleep(30 * time.Second)
		masterIPs = discoverMasterIPs()
	}
}

// we start a server listening on port 5050, this gets run in main so all masters will do this
// This allows for all masters to be synced with the leader master
func startMasterServers() {
	lis, err := net.Listen("tcp", *syncListenAddr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", *syncListenAddr, err)
	}
	s := grpc.NewServer()
	pb.RegisterMasterSyncServer(s, &syncServer{})
	log.Printf("Master gRPC listening on %s", *syncListenAddr)

	if err := s.Serve(lis); err != nil {
		log.Fatalf("sync gRPC server failed: %v", err)
	}
}

// Creates a protobuf message containing the current state of all jobs (map/reduce task completion status) to send to follower masters.
func buildCurrentState() *pb.StateSyncRequest {

	mutex.RLock()
	defer mutex.RUnlock()

	req := &pb.StateSyncRequest{}

	for jobID, maps := range mapStates {
		// Creates the job as well as the map/reduce to send based on local map/reduce
		job := &pb.JobState{JobId: jobID}

		// .MapTasks and .ReduceTask are repeated in the protos, so it just appends the map states and reduce states
		for _, m := range maps {
			job.MapTasks = append(job.MapTasks, &pb.TaskInfo{
				Id:       int32(m.ID),
				Complete: m.Complete,
			})
		}
		for _, r := range reduceStates[jobID] {
			job.ReduceTasks = append(job.ReduceTasks, &pb.TaskInfo{
				Id:       int32(r.ID),
				Complete: r.Complete,
			})
		}
		// Update all job states
		req.Jobs = append(req.Jobs, job)
	}
	return req
}

// Sends the current state to all other master nodes (excluding self) via gRPC. Runs asynchronously for each master.
func broadcastStateToNonLeaders() {
	req := buildCurrentState()

	for _, addr := range masterIPs {
		// Need to make sure not to send a grpc to itself
		if strings.HasPrefix(addr, *id) {
			continue
		}

		go func(a string) {
			conn, err := grpc.NewClient(a, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				return
			}
			defer conn.Close()
			client := pb.NewMasterSyncClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			_, err = client.SyncState(ctx, req)
			if err != nil {
				log.Printf("Failed updating non masters")
			}
		}(addr)
	}
}

// gRPC handler that receives state updates from the leader and updates the local map/reduce state maps.
func (s *syncServer) SyncState(ctx context.Context, req *pb.StateSyncRequest) (*pb.Ack, error) {
	mutex.Lock()
	defer mutex.Unlock()

	for _, job := range req.Jobs {

		// Non leaders don't know M or R, so they get it by doing len()
		maps := make([]MapTaskStatus, len(job.MapTasks))
		for i, m := range job.MapTasks {
			maps[i] = MapTaskStatus{ID: int(m.Id), Complete: m.Complete}
		}
		reduces := make([]ReduceTaskStatus, len(job.ReduceTasks))
		for i, r := range job.ReduceTasks {
			reduces[i] = ReduceTaskStatus{ID: int(r.Id), Complete: r.Complete}
		}
		mapStates[job.JobId] = maps
		reduceStates[job.JobId] = reduces

	}
	log.Printf("Recieved Update from leader")
	return &pb.Ack{Ok: true}, nil
}

// Leader-only function that broadcasts state to followers every 5 seconds to keep them synchronized.
func startPeriodicSync() {
	for {
		broadcastStateToNonLeaders()
		time.Sleep(5 * time.Second)
	}
}

// Parses the -fail flag to set a failure threshold (for testing master failures).
func initFailCounter() {
	if *fail_count == "" {
		failThreshold = 0 // means never fail
	} else {
		n, err := strconv.Atoi(*fail_count)
		if err != nil {
			log.Fatalf("Invalid fail value: %v", err)
		}
		failThreshold = n
	}
}

// Entry point: parses flags, connects to etcd, starts background goroutines for master discovery and sync server, then begins leader election.
func main() {
	flag.Parse()
	initFailCounter()

	// Create a global etcd client
	var err error
	etcdClient, err = clientv3.New(clientv3.Config{Endpoints: []string{"etcd.mr.svc.cluster.local:2379"}})
	if err != nil {
		log.Fatal(err)
	}
	defer etcdClient.Close()

	go startMasterLoop()
	go startMasterServers()

	leader_election()
}

// simple helper to remove a job from a job list and return the new slice
func removeJob(jList []int, job int) []int {
	for i := 0; i < len(jList); i++ {
		if jList[i] == job {
			return append(jList[:i], jList[i+1:]...)
		}
	}
	return nil
}

// Uses DNS lookup to discover all worker pod IPs via the Kubernetes headless service. Returns them with port 80 appended.
func getPodIps() []string {

	time.Sleep(time.Second)
	serviceName := "worker-headless.mr.svc.cluster.local"

	ips, err := net.LookupHost(serviceName)
	if err != nil {
		log.Printf("Failed to lookup worker service %s: %v", serviceName, err)
		return nil
	}

	for i, ip := range ips {
		ips[i] = fmt.Sprintf("%s:80", ip)
	}

	log.Printf("Discovered %d worker IPs: %v", len(ips), ips)
	return ips
}

// Reassigns failed map tasks to available workers by round-robin distribution.
func redoJobs(jobList []int, shardInfo *[]pb.MapRequest, ch chan pb.MapReply, jobId string) {
	ips := getPodIps()
	for i := 0; i < len(jobList); i++ {
		/*loop through the jobs and reassign them to available workers*/
		ourIp := ips[i%len(ips)]
		mutex.Lock()
		delete(jobToTime, jobList[i])
		ipToJobs[ourIp] = append(ipToJobs[ourIp], jobList[i])
		log.Printf("REDO: %v going to %v", jobList[i], ourIp)
		mutex.Unlock()
		go sendMap(ourIp, ch, jobList[i], shardInfo, jobId)
	}
}

// Originally meant to remove worker IPs from etcd but changed design
func removePodIP(ip string) {
	/*
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		key := fmt.Sprintf("/mapreduce/pods/%s/ip", ip)
		//log.Printf("deleting: %s", key)
		_, err := etcdClient.Delete(ctx, key)
		if err != nil {
			log.Printf("Failed to delete %s from kv store", key)
			return
		}*/
	log.Printf("%v", ip)
	return
}

// Sends a single map task to a worker via gRPC
func sendMap(ip string, ch chan pb.MapReply, j int, shardInfo *[]pb.MapRequest, jobID string) {
	// Ensure safe access to global structure
	mutex.Lock()
	jobToTime[j] = time.Now()
	mutex.Unlock()

	// Connects to a worker to send the job to
	conn, err := grpc.NewClient(ip, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("did not connect: %v", err)
		mutex.Lock()
		jobToTime[j] = jobToTime[j].Add(time.Minute * -5)
		mutex.Unlock()
		return
	}
	defer conn.Close()

	c := pb.NewMapReduceClient(conn)
	temp, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	ctx := metadata.AppendToOutgoingContext(temp, "ip", *id)
	defer cancel()

	// The -5 minute addition is a clever hack to force immediate task reassignment when a connection or RPC fails. Explained in heartbeat section
	r, err := c.Map(ctx, &(*shardInfo)[j])
	if err != nil {
		log.Printf("could not greet: %v", err)
		mutex.Lock()
		log.Printf("JOB ID: %d", j)
		jobToTime[j] = jobToTime[j].Add(time.Minute * -5)
		mutex.Unlock()
		return
	}
	// Make updates thread safe
	mutex.Lock()
	ch <- *r
	mapStates[jobID][j].Complete = true
	jCount++
	failCounter++

	// Just to simulate failure for demonstration purposes
	if failThreshold > 0 && failCounter >= failThreshold {
		log.Printf("[FAIL] Master %s reached fail threshold (%d).", *id, failThreshold)
		os.Exit(1)
	}
	mutex.Unlock()

}

/*
Function which monitors heartbeat of tasks

Spawns a goroutine per worker to send periodic "pulse" checks every 5 seconds
If worker doesn't respond or connection fails: reassigns all its tasks
Monitors for task timeouts (>5 minutes) and reassigns them
Exits when all map tasks complete
*/
func trackBeat(shardInfo *[]pb.MapRequest, ch chan pb.MapReply, jobId string, ips []string) {

	var mapFinished int
	mapFinished = 0

	mutex.Lock()
	print("%v", ipToJobs)
	mutex.Unlock()

	for i := 0; i < len(ips); i++ {
		/*spawn go routine for each worker pod*/
		ip := ips[i]
		go func(ip string) {
			conn, err := grpc.NewClient(ip, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				// Cant connect to worker, so need to remove it from available list of IPs and resend its tasks
				log.Printf("did not connect to %s: %v", ip, err)

				mutex.Lock()
				removePodIP(ip)
				ips = getPodIps()
				log.Printf("deleted %s and got: %v", ip, ips)

				jobList := ipToJobs[ip]
				delete(ipToJobs, ip)
				mutex.Unlock()

				redoJobs(jobList, shardInfo, ch, jobId)
				return
			}
			defer conn.Close()

			c := pb.NewMapReduceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
			defer cancel()
			for {
				time.Sleep(time.Second * 5)
				if mapFinished == 1 {
					return
				}
				/*loop until all jobs are finished*/
				log.Printf("Send pulse to: %s", ip)
				mutex.Lock()
				if jCount == len(*shardInfo) {
					log.Printf("Finish mapper heartbeat")
					mutex.Unlock()
					return
				}
				mutex.Unlock()

				blood := pb.Pump{Oxygen: int32(1)}
				/*send pulse to check if worker is still alive*/
				_, err := c.Pulse(ctx, &blood)
				if err != nil {
					/*timeout*/

					log.Printf("failed heartbeat: %v", err)
					mutex.Lock()

					removePodIP(ip)
					jobList := ipToJobs[ip]
					delete(ipToJobs, ip)

					mutex.Unlock()

					redoJobs(jobList, shardInfo, ch, jobId)
					return
				}
				t := time.Now()
				mutex.Lock()
				jobList := ipToJobs[ip]
				mutex.Unlock()
				jlInd := 0
				for z := 0; z < len(jobList); z++ {
					//check for any jobs that have timedout
					mutex.Lock()
					if mapStates[jobID][jobList[jlInd]].Complete == true {
						mutex.Unlock()
						continue
					}
					tstamp := jobToTime[jobList[jlInd]]
					mutex.Unlock()
					tdiff := t.Sub(tstamp)
					//log.Printf("TDIFF: %d", tdiff)
					if tdiff > time.Minute*5 {
						/*redo job*/
						/*shouldnt really matter too much who we send to cause eventually all workers will be free to focus on the task*/
						log.Printf("redo job, timedout: %v", jobList[jlInd])
						newips := getPodIps()
						ipI := rand.Intn(len(newips))
						mutex.Lock()

						ourIp := newips[ipI]
						log.Printf("Our IP: %s", ourIp)
						log.Printf("Ip to jobs before append: %v", ipToJobs[ourIp])
						ipToJobs[ourIp] = append(ipToJobs[ourIp], jobList[jlInd])
						log.Printf("Ip to jobs after append: %v", ipToJobs[ourIp])

						log.Printf("Ip to jobs before remove job: %v", ipToJobs[ip])
						ipToJobs[ip] = removeJob(ipToJobs[ip], jobList[jlInd])
						log.Printf("Ip to jobs after remove job: %v", ipToJobs[ip])

						mutex.Unlock()
						sendMap(ourIp, ch, jobList[jlInd], shardInfo, jobID)
					}
					jlInd++
				}
			}
		}(ip)
	}
	// ---- End of go routines ----

	for {
		time.Sleep(5 * time.Second)
		mutex.Lock()
		log.Printf("current jcount: %v, current shard num: %v", jCount, len(*shardInfo))
		if jCount == len(*shardInfo) {
			log.Printf("Finish checking jobcount")
			mutex.Unlock()
			break
		}
		mutex.Unlock()
	}
	mutex.Lock()

	clear(ipToJobs)
	clear(jobToTime)
	mapFinished = 1
	mutex.Unlock()
}

// Starts HTTP server on port 80 to listen for job submissions at /submit endpoint
func startInterface() {
	http.HandleFunc("/submit", handleJob)
	log.Println("Running Master to listen for jobs")
	log.Fatal(http.ListenAndServe(":80", nil))
}

/*
HTTP handler for job submissions:

Validates POST request and JSON body
Generates unique 8-character job ID
Initializes map/reduce state tracking
Uploads job metadata to etcd
Launches master_work() asynchronously
*/
func handleJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	var req JobRequest
	body, _ := io.ReadAll(r.Body)
	err := json.Unmarshal(body, &req)
	if err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if req.SourceContainer == "" {
		http.Error(w, "No container provided", http.StatusBadRequest)
		return
	}

	// We can have multiple jobs so creating unique jobID, and only taking 8 chars to keep it short
	// Also funny fact when you alt click .NewString() about how random it is lmao
	jobID = uuid.NewString()[:8]
	meta := JobRequest{
		ID:              jobID,
		SourceContainer: req.SourceContainer,
		NumMappers:      req.NumMappers,
		NumReducers:     req.NumReducers,
	}

	m_amount = req.NumMappers
	r_amount = req.NumReducers
	source_container = req.SourceContainer

	mutex.Lock()
	maps := make([]MapTaskStatus, m_amount)
	for i := 0; i < m_amount; i++ {
		maps[i] = MapTaskStatus{
			ID:       i,
			Complete: false,
		}
	}
	mapStates[jobID] = maps

	reduces := make([]ReduceTaskStatus, r_amount)
	for i := 0; i < r_amount; i++ {
		reduces[i] = ReduceTaskStatus{
			ID:       i,
			Complete: false,
		}
	}
	reduceStates[jobID] = reduces
	mutex.Unlock()

	upload_err := uploadJobMeta(meta)
	if upload_err != nil {
		log.Printf("etcd upload failed: %v", err)
		return
	}
	log.Printf("Received new job: %d mappers", m_amount)

	// run async so the HTTP request returns fast
	go master_work()

	w.WriteHeader(http.StatusAccepted)
}

/*
* Main master logic.
*
* Creates the connection and data structures and sends requests.
 */
func master_work() {
	var shardInfo []pb.MapRequest
	get_shards(&shardInfo)

	MapFunc(&shardInfo)
	ReduceFunc()
}

/*
Main map phase coordinator:

Creates result channel
Checks for already-completed tasks (from previous master failure)
Assigns tasks to workers via round-robin
Launches sendMap() goroutines
Starts trackBeat() to monitor progress
Waits for all tasks to complete
*/
func MapFunc(shardInfo *[]pb.MapRequest) {
	//ref for async grpc in go: https://github.com/rupc/grpc-async/blob/master/client-async/client/main.go

	channels := make(chan pb.MapReply, len(*shardInfo))

	mutex.RLock()
	jobTasks := mapStates[jobID]
	mutex.RUnlock()

	launched := 0

	ips := getPodIps()
	mutex.Lock()
	jCount = 0
	mutex.Unlock()

	for i := 0; i < len(*shardInfo); i++ {
		if i < len(jobTasks) && jobTasks[i].Complete {
			log.Printf("[map] Skipping shard %d (already complete)", i)
			dummy := pb.MapReply{Stat: int32(1)}
			channels <- dummy
			mutex.Lock()
			jCount++
			mutex.Unlock()
			continue
		}
		log.Printf("sending id: %v", i)
		// Doing this because result is based on amount of shards actually sent, so when a master fails and another master sends half, it will not give error
		launched++

		// Needed to alter proto a little so the workers know the amount of reducers so it can split up files evenly

		(*shardInfo)[i].NumReducers = int32(r_amount)
		(*shardInfo)[i].MapId = int32(i)
		(*shardInfo)[i].ContainerName = source_container

		// Added this to the grpc so files can be named based on jobID by the worker since we call multiple jobs
		(*shardInfo)[i].JobID = jobID

		// Had to move connections inside the actual function so it can be load balanced by the ClusterIP
		ourIp := ips[i%len(ips)]
		mutex.Lock()
		ipToJobs[ourIp] = append(ipToJobs[ourIp], i)
		mutex.Unlock()
		go sendMap(ourIp, channels, i, shardInfo, jobID)
	}

	/*start sending out heart beat requests*/
	trackBeat(shardInfo, channels, jobID, ips)
}

/*
Main reduce phase coordinator:

Lists intermediate files from Azure blob storage
Groups files by reducer ID (parses filenames like jobID_m0-r1.txt)
Checks for already-completed reducers
Assigns reduce tasks to workers
Starts trackBeatReduce() to monitor
Waits for completion
*/
func ReduceFunc() {
	accountName := "sysprojectstorage"
	accountKey := os.Getenv("AZURE_STORAGE_KEY")
	if accountName == "" || accountKey == "" {
		log.Fatalf("Missing Azure credentials: AZURE_STORAGE_ACCOUNT or AZURE_STORAGE_KEY")
	}

	cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatalf("Failed to create shared key credential: %v", err)
	}

	client, err := azblob.NewClientWithSharedKeyCredential(
		fmt.Sprintf("https://%s.blob.core.windows.net/", accountName),
		cred,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to create Azure blob client: %v", err)
	}
	containerName := "intermediates"

	log.Println("Listing the intermediate files in the container:")

	var allFiles []string
	pager := client.NewListBlobsFlatPager(containerName, nil)
	for pager.More() {
		resp, err := pager.NextPage(context.TODO())
		if err != nil {
			log.Printf("Error listing intermediates: %v", err)
			continue
		}
		for _, blob := range resp.Segment.BlobItems {
			name := *blob.Name

			// Since we base the name off the job now, need to only get relevant files
			if strings.HasPrefix(name, jobID+"_") {
				allFiles = append(allFiles, name)
			}
		}
	}

	if len(allFiles) == 0 {
		log.Println("No intermediate files found.")
		return
	}

	// We are going to get all the files from the intermediate container, and find their actual map and reducer ID, and create a map of the id to files
	reducer_map := make(map[int][]string)

	for _, fname := range allFiles {
		var mapper_id int
		var reducer_id int

		// Need to split the string so it works with old code
		split_intermediate_file_by_jobID := strings.SplitN(fname, "_", 2)
		if len(split_intermediate_file_by_jobID) < 2 {
			log.Printf("bad filename: %s", fname)
			continue
		}

		// This is getting the actual file that corresponds to reducer
		_, err := fmt.Sscanf(split_intermediate_file_by_jobID[1], "m%d-r%d.txt", &mapper_id, &reducer_id)
		if err != nil {
			log.Printf("Filename is not expected %s", fname)
			continue
		}
		reducer_map[reducer_id] = append(reducer_map[reducer_id], fname)
	}

	log.Printf("Found %d intermediate files", len(allFiles))
	mutex.RLock()
	jobReduces := reduceStates[jobID]
	mutex.RUnlock()

	mutex.Lock()
	jCount = 0
	mutex.Unlock()
	ips := getPodIps()

	// We then send out R amount of reduce tasks, sending out the files that make up that ID
	channels := make(chan pb.ReduceReply, len(reducer_map))
	for reducer_id, _ := range reducer_map {

		mutex.RLock()
		if reducer_id < len(jobReduces) && jobReduces[reducer_id].Complete {
			mutex.RUnlock()
			log.Printf("[reduce] Skipping reducer %d (already complete)", reducer_id)
			dummy := pb.ReduceReply{Stat: int32(1)}
			channels <- dummy
			mutex.Lock()
			jCount++
			mutex.Unlock()
			continue
		}
		mutex.RUnlock()
		ourIp := ips[reducer_id%len(ips)]
		mutex.Lock()
		ipToJobs[ourIp] = append(ipToJobs[ourIp], reducer_id)
		mutex.Unlock()
		go sendReduce(ourIp, channels, reducer_id, reducer_map, jobID)
	}

	trackBeatReduce(reducer_map, channels, jobID)
	log.Println("All reducers finished.")
}

/*
Sends a single reduce task to a worker:

Records start time
Connects to worker
Sends list of intermediate files to process
On success: marks complete, increments counters
On failure: sets timestamp for retry
*/
func sendReduce(ip string, ch chan pb.ReduceReply, reducerIdx int, fileInfo map[int][]string, jobID string) {
	mutex.Lock()
	jobToTime[reducerIdx] = time.Now()
	mutex.Unlock()

	files := fileInfo[reducerIdx]
	conn, err := grpc.NewClient(ip, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Reducer %d: failed to connect to gRPC: %v", reducerIdx, err)
		mutex.Lock()
		jobToTime[reducerIdx] = jobToTime[reducerIdx].Add(time.Minute * -5)
		mutex.Unlock()
		return
	}
	defer conn.Close()

	// Sets up the job to send to worker and final name
	c := pb.NewMapReduceClient(conn)
	temp, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	ctx := metadata.AppendToOutgoingContext(temp, "ip", *id)
	out := fmt.Sprintf("final_reduced_job_%s_%d.txt", jobID, reducerIdx)
	defer cancel()

	// Construct the ReduceRequest for this reducer
	req := pb.ReduceRequest{
		Fname:   files,
		Outname: out,
	}

	// If failed, set heartbeat -5 minutes to make it get resent out to different worker
	log.Printf("Reducer %d: processing %d files → %s", reducerIdx, len(files), out)
	result, err := c.Reduce(ctx, &req)
	if err != nil {
		log.Printf("Reducer %d: Reduce RPC failed: %v", reducerIdx, err)
		mutex.Lock()
		jobToTime[reducerIdx] = jobToTime[reducerIdx].Add(time.Minute * -5)
		mutex.Unlock()
		return
	}

	log.Printf("Reducer %d: completed, Stat=%d", reducerIdx, result.GetStat())

	// Make updates thread safe
	mutex.Lock()
	reduceStates[jobID][reducerIdx].Complete = true
	jCount++
	failCounter++
	if failThreshold > 0 && failCounter >= failThreshold {
		mutex.Unlock()
		log.Printf("[FAIL] Master %s reached fail threshold (%d).", *id, failThreshold)
		os.Exit(1)
	}
	mutex.Unlock()
	ch <- *result
}

// Reassigns failed reduce tasks to available workers.
func redoJobsReduce(jobList []int, fileInfo map[int][]string, ch chan pb.ReduceReply, jobId string) {
	ips := getPodIps()
	for i := 0; i < len(jobList); i++ {
		/*loop through the jobs and reassign them to available workers*/
		ourIp := ips[i%len(ips)]

		mutex.Lock()

		delete(jobToTime, jobList[i])
		ipToJobs[ourIp] = append(ipToJobs[ourIp], i)
		log.Printf("REDO: %v going to %v", jobList[i], ourIp)

		mutex.Unlock()
		go sendReduce(ourIp, ch, i, fileInfo, jobId)
	}
}

/*
Heartbeat monitor for reduce phase:

Same logic as trackBeat() but for reduce tasks
Monitors worker health and task timeouts
Reassigns on failure
*/
func trackBeatReduce(fileInfo map[int][]string, ch chan pb.ReduceReply, jobId string) {
	ips := getPodIps()
	/*loop through ips spawning threads for each worker pod*/
	for i := 0; i < len(ips); i++ {
		ip := ips[i]
		go func(ip string) {
			conn, err := grpc.NewClient(ip, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				/*cant connect, assume worker down*/
				removePodIP(ip)
				log.Printf("did not connect: %v", err)

				mutex.Lock()

				removePodIP(ip)
				ips = getPodIps()
				log.Printf("deleted %s and got: %v", ip, ips)
				jobList := ipToJobs[ip]
				delete(ipToJobs, ip)

				mutex.Unlock()

				redoJobsReduce(jobList, fileInfo, ch, jobId)
				return
			}
			defer conn.Close()
			c := pb.NewMapReduceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
			defer cancel()
			for {
				time.Sleep(time.Second * 5)
				/*loop until all jobs are finished*/
				mutex.Lock()
				if jCount == len(fileInfo) {
					mutex.Unlock()
					return
				}
				mutex.Unlock()
				log.Printf("Send pulse to: %s", ip)
				blood := pb.Pump{Oxygen: int32(1)}
				/*send pulse to check if worker is still alive*/
				_, err := c.Pulse(ctx, &blood)
				if err != nil {
					/*timeout*/
					mutex.Lock()
					jobList := ipToJobs[ip]
					mutex.Unlock()

					removePodIP(ip)
					log.Printf("failed heartbeat: %v", err)
					redoJobsReduce(jobList, fileInfo, ch, jobId)
					return
				}
				t := time.Now()
				mutex.Lock()
				jobList := ipToJobs[ip]
				mutex.Unlock()
				jlInd := 0
				for z := 0; z < len(jobList); z++ {
					//check for any jobs that have timedout
					mutex.Lock()
					if mapStates[jobId][jobList[jlInd]].Complete == true {
						mutex.Unlock()
						continue
					}
					tstamp := jobToTime[jobList[jlInd]]
					mutex.Unlock()

					// This tdiff is why we do -5, since the order is reversed. When a worker is down, instead of waiting 5 minutes, it automatically restarts right away
					tdiff := tstamp.Sub(t)
					if tdiff > time.Minute*5 {
						/*redo job*/
						/*shouldnt really matter too much who we send to cause eventually all workers will be free to focus on the task*/

						newips := getPodIps()
						ipI := rand.Intn(len(newips))
						mutex.Lock()
						ourIp := newips[ipI]

						log.Printf("Our IP: %s", ourIp)
						log.Printf("Ip to jobs before append: %v", ipToJobs[ourIp])
						ipToJobs[ourIp] = append(ipToJobs[ourIp], jobList[jlInd])
						log.Printf("Ip to jobs after append: %v", ipToJobs[ourIp])

						log.Printf("Ip to jobs before remove job: %v", ipToJobs[ip])
						ipToJobs[ip] = removeJob(ipToJobs[ip], jobList[jlInd])
						log.Printf("Ip to jobs after remove job: %v", ipToJobs[ip])

						mutex.Unlock()
						sendReduce(ourIp, ch, jobList[jlInd], fileInfo, jobID)
					}
					jlInd++
				}
			}
		}(ip)
	}
	for {
		log.Printf("go to sleep")
		time.Sleep(5 * time.Second)
		log.Printf("check for completion")
		mutex.Lock()
		log.Printf("current jcount: %v, current shard num: %v", jCount, len(fileInfo))
		if jCount == len(fileInfo) {
			log.Printf("Finish checking jobcount")
			mutex.Unlock()
			break
		}
		mutex.Unlock()
	}
	mutex.Lock()
	clear(ipToJobs)
	clear(jobToTime)
	mutex.Unlock()
}

/*
Creates shard boundaries for map tasks:

Lists all blobs in the source Azure container
Calculates total size and desired shard size
Creates FileSegment protobuf messages defining byte ranges
Handles files spanning multiple shards or multiple files in one shard
Populates the shardInfo array with MapRequest objects
*/
func get_shards(shardInfo *[]pb.MapRequest) {
	//accountName := os.Getenv("AZURE_STORAGE_ACCOUNT")
	//accountKey := os.Getenv("AZURE_STORAGE_KEY")
	accountName := "sysprojectstorage"
	accountKey := os.Getenv("AZURE_STORAGE_KEY")
	containerName := source_container

	if accountName == "" || accountKey == "" {
		log.Fatalf("Missing Azure credentials: AZURE_STORAGE_ACCOUNT or AZURE_STORAGE_KEY")
	}

	cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatalf("Failed to create shared key credential: %v", err)
	}

	client, err := azblob.NewClientWithSharedKeyCredential(
		fmt.Sprintf("https://%s.blob.core.windows.net/", accountName),
		cred,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to create Azure blob client: %v", err)
	}

	log.Println("Listing the blobs in the container:")

	pager := client.NewListBlobsFlatPager(containerName, nil)

	type FileInfo struct {
		Name string
		Size int64
	}
	var files []FileInfo
	var totalSize int64

	for pager.More() {
		resp, err := pager.NextPage(context.TODO())

		if err != nil {
			log.Printf("Error")
		}

		// This gets all the files inside the blob container
		for _, blob := range resp.Segment.BlobItems {
			// log.Println(*blob.Name)

			// Sharding was very slow, but it was because i was calling a new client thousands of times for 1 GB
			// This is much much faster
			if blob.Properties.ContentLength == nil {
				continue
			}
			files = append(files, FileInfo{Name: *blob.Name, Size: *blob.Properties.ContentLength})
			totalSize += *blob.Properties.ContentLength

			// We need to store the file name and its size in an array, which is why we use the struct
			// This is because multiple files can be in one shard, so we need all the file information in the container
		}
	}

	// Create Shard estimates from blob info, we want min size of 4k for the shard to make it more efficeint

	// Round up the shard size to contain end of files
	shardSize := (totalSize + int64(m_amount) - 1) / int64(m_amount)
	if shardSize < minShardSize {
		shardSize = minShardSize
	}
	log.Printf("Creating %d shards, each maximum %d bytes", m_amount, shardSize)

	// Used to get current stats of current blob for tracking throughout shard creation
	var currentFileIdx = 0
	var fileOffset int64 = 0
	var bytes_left_in_file int64 = files[currentFileIdx].Size

	// Loop through M amount of shards until all blobs are processed
	// Essentially going to go through the blob file until the shard is full
	// With large files, its simple, but edge case where the file ends and there is another file that can fit inside
	for shardIdx := 0; shardIdx < m_amount && currentFileIdx < len(files); shardIdx++ {
		remaining_bytes_in_shard := shardSize

		// Added segments in the protos since we may need to have multiple files per shard, stores filename, and start/end of that file
		var segments []*pb.FileSegment

		// This will loop until the shard is full. Need to make sure to update what file we use once that file is completly used
		for remaining_bytes_in_shard > 0 && currentFileIdx < len(files) {
			take := remaining_bytes_in_shard

			// If shard is larger than the amount of bytes left in the file, then only use the remaining bytes
			// This also means we must move onto the next file and input in same shard
			if take > bytes_left_in_file {
				take = bytes_left_in_file
			}

			seg := &pb.FileSegment{
				Fname: files[currentFileIdx].Name,
				Begin: int32(fileOffset),
				End:   int32(fileOffset + take),
			}

			segments = append(segments, seg)

			// log.Printf("Segment added: %s; Begin: %d; End: %d, Max shard size: %d", files[currentFileIdx].Name, fileOffset, fileOffset+take, shardSize)

			// If shard gets full from one file, bytes_in_shard will be 0, otherwise the bytes left in file is 0, so we must use next file
			remaining_bytes_in_shard -= take
			fileOffset += take
			bytes_left_in_file -= take

			if bytes_left_in_file == 0 {
				currentFileIdx++
				if currentFileIdx < len(files) {
					fileOffset = 0
					bytes_left_in_file = files[currentFileIdx].Size
				}
			}
		}

		log.Printf("Shard %d has been added", shardIdx)
		*shardInfo = append(*shardInfo, pb.MapRequest{
			Segments: segments,
			Outname:  fmt.Sprintf("intermediate_%d.txt", shardIdx),
		})
	}

	log.Printf("Generated %d total shards", len(*shardInfo))

}

/*
Main leader election loop:

Creates etcd session with 10-second TTL
Attempts to become leader via etcd election
If elected: starts periodic sync, HTTP interface, and metadata recovery
If loses leadership: cleans up and tries again
*/
func leader_election() {
	for {

		s, err := concurrency.NewSession(etcdClient, concurrency.WithTTL(10))
		if err != nil {
			log.Fatal(err)
		}

		e := concurrency.NewElection(s, "/leader-election/")
		ctx := context.Background()

		// Elect a leader (or wait that the leader resign)
		log.Printf("%s is Attempting to be leader", *id)
		if err := e.Campaign(ctx, "e"); err != nil {
			time.Sleep(2 * time.Second)
			continue
		}

		log.Printf("%s Is the leader, Starting the work", *id)

		go startPeriodicSync()
		go startInterface()

		go func() {
			if err := GetAndSetMetaData(); err != nil {
				log.Printf("metadata recovery failed: %v", err)
			}
		}()

		<-s.Done()
		log.Printf("[%s] Lost leadership.", *id)
		s.Close()
	}
}

// Stores job metadata in etcd at /mapreduce/jobs/{jobID}/meta and updates the current job pointer. Broadcasts state to followers.
func uploadJobMeta(meta JobRequest) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("/mapreduce/jobs/%s/meta", meta.ID)

	// A put is a singular atomic operation so it is kinda expensive, will see example later how to make it better but in this case we only need to write once per job anyway
	_, err = etcdClient.Put(ctx, key, string(data))
	if err != nil {
		log.Fatal("failed to upload job meta: %w", err)
	}

	// Need a pointer for the actual job_id that gets deleted then reset whenever we get a job request
	job_id_key := "/mapreduce/job_id"

	if _, err := etcdClient.Delete(ctx, job_id_key); err != nil {
		log.Fatal("failed to delete previous job pointer")
	}

	if _, err := etcdClient.Put(ctx, job_id_key, meta.ID); err != nil {
		log.Fatal("failed to set new job pointer")
	}

	log.Printf("Uploaded job meta to etcd: %s", key)
	broadcastStateToNonLeaders()
	return nil
}

/*
Recovery function for new leaders:

Reads current job ID from etcd
Checks if job is already complete
If incomplete: recovers job parameters and restarts master_work()
Allows seamless continuation after master failure
*/
func GetAndSetMetaData() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// Gets job if it exists
	resp, err := etcdClient.Get(ctx, "/mapreduce/job_id")
	if err != nil {
		log.Print("failed to get job pointer: %w", err)
		return err
	}
	if len(resp.Kvs) == 0 {
		log.Print("No active job pointer found in etcd")
		return nil
	}

	currentJobID := string(resp.Kvs[0].Value)
	log.Printf("Found current job pointer: %s", currentJobID)

	mutex.RLock()
	mapTasks, mapExists := mapStates[currentJobID]
	reduceTasks := reduceStates[currentJobID]
	mutex.RUnlock()

	// If all masters go down, etcd breaks program without this check because the map wont exist
	if !mapExists || len(mapTasks) == 0 {
		log.Printf("No map state found for job %s, likely because all masters went down", currentJobID)
		return nil
	}

	// Checks to see if the mapping or reducing phase already complete
	all_maps_finished := true
	for _, task := range mapTasks {
		if !task.Complete {
			all_maps_finished = false
			break
		}
	}
	all_reduced_finished := true
	for _, task := range reduceTasks {
		if !task.Complete {
			all_reduced_finished = false
			break
		}
	}

	// Job was already finished so dont do anything
	if all_maps_finished && all_reduced_finished {
		log.Printf("Job %s already completed", currentJobID)
		return nil
	}

	// Job not finished so retrieve metadata (number of mappers, reducers, and source container for texts)
	metadata_for_current_job := fmt.Sprintf("/mapreduce/jobs/%s/meta", currentJobID)
	metadata, err := etcdClient.Get(ctx, metadata_for_current_job)
	if err != nil {
		log.Fatal("failed to get job metadata")
	}
	if len(metadata.Kvs) == 0 {
		log.Fatal("no metadata found for job")
	}

	var meta JobRequest
	if err := json.Unmarshal(metadata.Kvs[0].Value, &meta); err != nil {
		log.Fatalf("failed to unmarshal job metadata")
	}

	mutex.Lock()
	jobID = meta.ID
	m_amount = meta.NumMappers
	r_amount = meta.NumReducers
	source_container = meta.SourceContainer
	mutex.Unlock()

	log.Printf("Recovered job: ID=%s, Mappers=%d, Reducers=%d, Container=%s",
		jobID, m_amount, r_amount, source_container)

	go master_work()

	return nil
}
