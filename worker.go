package main

/*
Imports we want but havent used yet:
	"time"
	"os"
	"strings"
	"go.etcd.io/etcd/client/v3"
  	"go.etcd.io/etcd/client/v3/concurrency"
	"github.com/Azure/azure-storage-blob-go/azblob"
*/

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	pb "mapreduce/protos"
	"net"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"

	"strconv"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var (
	port         = flag.Int("port", 50051, "The server port")
	url          = "https://sysprojectstorage.blob.core.windows.net/"
	etcdClient   *clientv3.Client
	failVal      = 0
	currentCount = 0
	mutex        sync.RWMutex
)

type server struct {
	//pb.UnimplementedGreeterServer
	pb.UnimplementedMapReduceServer
	blobClient *azblob.Client
	container  string
}

/*
data: The blob data of that specific shard
start: The start given by the master

What we are doing is checking if start byte is a letter, if so, we are in middle of word so we must go to the next word
(This works because the findEnd will work out the edge case where the start byte of one shard=end byte of last shard)
*/
func findRealStart(data []byte, start int64) int64 {
	n := int64(len(data))

	// If prev byte is anything but space/newline
	if start > 0 && data[start] != ' ' && data[start] != '\n' && data[start] != '\r' {
		// move right until we hit a word separator
		for start < n && data[start] != ' ' && data[start] != '\n' && data[start] != '\r' {
			start++
		}

		// skip the separators (for loop in case multiple seperators)
		for start < n && (data[start] == ' ' || data[start] == '\n' || data[start] == '\r') {
			start++
		}
	} else {
		// if already at a boundary, skip any extra whitespace to reach the first letter
		for start < n && (data[start] == ' ' || data[start] == '\n' || data[start] == '\r') {
			start++
		}
	}
	return start
}

/*
data: The blob data of that specific shard
end: The end given by the master

What we are doing is checking if the current end is a letter, if so keep continuing till there is a newline/space
*/
func findRealEnd(data []byte, end int64) int64 {
	n := int64(len(data))

	if end >= n {
		end = n - 1
	}

	if data[end] != ' ' && data[end] != '\n' && data[end] != '\r' {
		for end < n && data[end] != ' ' && data[end] != '\n' && data[end] != '\r' {
			// fmt.Printf("%c", data[end])
			end++
		}
		// fmt.Println()
	}

	// Include spaces and such after to keep it clean
	for end < n && (data[end] == ' ' || data[end] == '\n' || data[end] == '\r' || data[end] == '\t') {
		end++
	}
	return end
}

// Processes a map task requested by the master
func (s *server) Map(ctx context.Context, in *pb.MapRequest) (*pb.MapReply, error) {
	md, exist := metadata.FromIncomingContext(ctx)
	if exist == false {
		log.Fatalf("no metadata")
	}
	ip := md["ip"]
	log.Printf("Received request from: %v", ip)

	mutex.Lock()
	currentCount++
	if currentCount >= failVal && failVal > 0 {
		log.Printf("Reached the target fail value. Exiting")
		os.Exit(1)
	}
	mutex.Unlock()

	// First need to read in the blob
	client := s.blobClient
	containerName := in.ContainerName
	var total_Data []byte

	// It is possible to have multiple segments in one shard, so when creating the shard, we need to append the data and combine it
	for _, seg := range in.GetSegments() {
		fileName := seg.GetFname()
		start := int64(seg.GetBegin())
		end := int64(seg.GetEnd())

		var offset int64 = 64
		extended_start := start - offset
		if extended_start < 0 {
			extended_start = 0
		}
		count := end - extended_start + offset

		get, err := client.DownloadStream(ctx, containerName, fileName, &azblob.DownloadStreamOptions{
			Range: azblob.HTTPRange{Offset: extended_start, Count: count},
		})
		if err != nil {
			log.Printf("Failed to download range from %s: %v", fileName, err)
			return nil, err
		}

		data, err := io.ReadAll(get.Body)
		get.Body.Close()
		if err != nil {
			log.Printf("Failed to read blob %s: %v", fileName, err)
			return nil, err
		}

		// Adjust for partial word boundaries
		var realStart int64
		var realEnd int64
		if start == 0 {
			realEnd = findRealEnd(data, end-start)
			realStart = 0
		} else {
			realEnd = findRealEnd(data, end-start+offset)
			realStart = findRealStart(data, offset)
		}

		real_data := data[realStart:realEnd]
		total_Data = append(total_Data, real_data...)
	}

	log.Printf("Finished downloading blobs")

	// Hard coded a mapper function to use
	cmd := exec.Command("python3", "mapper.py")
	cmd.Stdin = bytes.NewReader(total_Data)
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Fatalf("failed running mapper %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")

	numReducers := int(in.GetNumReducers())
	map_id := int(in.GetMapId())
	partitions := make(map[int][]byte, numReducers)

	// Added jobID here so we can write the proper files
	jobID := string(in.GetJobID())

	// We get the actual output from the mapper, and hash each word to figure out which index it should go to for the reducer id
	for _, line := range lines {
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) != 2 {
			continue
		}
		// Hashing word and getting idx
		word := parts[0]
		h := fnv.New32a()
		h.Write([]byte(word))
		reducerID := int(h.Sum32()) % numReducers

		partitions[reducerID] = append(partitions[reducerID], line...)
		partitions[reducerID] = append(partitions[reducerID], '\n')
	}

	// We then write R files per map task, with each having unique words based on the reducer index
	for r := 0; r < numReducers; r++ {
		// Skip if any partition is empty
		if len(partitions[r]) == 0 {
			continue
		}

		// sorting the partitions as requested by the pdf
		raw := strings.TrimSpace(string(partitions[r]))
		linesForReducer := strings.Split(raw, "\n")
		sort.Strings(linesForReducer)
		sortedData := []byte(strings.Join(linesForReducer, "\n") + "\n")

		outName := fmt.Sprintf("%s_m%d-r%d.txt", jobID, map_id, r)
		_, err = client.UploadBuffer(ctx, "intermediates", outName, sortedData, &azblob.UploadBufferOptions{})
		if err != nil {
			log.Printf("Failed to upload new blob: %v", err)
		}

		log.Printf("Uploaded new blob: %s with %d bytes", outName, len(partitions[r]))

	}
	return &pb.MapReply{Stat: int32(len(total_Data))}, nil
}

// Processes a reduce task - aggregates intermediate results.
func (s *server) Reduce(ctx context.Context, in *pb.ReduceRequest) (*pb.ReduceReply, error) {
	md, exist := metadata.FromIncomingContext(ctx)
	if exist == false {
		log.Fatalf("no metadata")
	}
	ip := md["ip"]
	log.Printf("Received request from: %v", ip)

	// First need to read in the blob
	client := s.blobClient
	containerName := "intermediates"
	files := in.GetFname()
	outName := in.GetOutname()

	var total_Data []byte

	for _, fname := range files {
		get, err := client.DownloadStream(ctx, containerName, fname, nil)
		if err != nil {
			log.Printf("Failed to download blob range: %v", err)
			return nil, err
		}

		data, err := io.ReadAll(get.Body)
		get.Body.Close()
		if err != nil {
			log.Printf("Failed to read blob data: %v", err)
			return nil, err
		}

		total_Data = append(total_Data, data...)

	}

	cmd := exec.Command("python3", "reducer.py")
	input := string(total_Data)

	cmd.Stdin = strings.NewReader(input)
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Fatalf("failed running mapper %v", err)
	}
	total_Data = []byte(output)

	_, err = client.UploadBuffer(ctx, "reduced", outName, total_Data, &azblob.UploadBufferOptions{})
	if err != nil {
		log.Printf("Failed to upload new blob: %v", err)
		return nil, err
	}

	log.Printf("Reducer processing %d files for %s", len(files), outName)

	return &pb.ReduceReply{Stat: int32(len(total_Data))}, nil
}

// Responds to heartbeat checks from the master.
func (s *server) Pulse(ctx context.Context, in *pb.Pump) (*pb.Pump, error) {
	log.Printf("Received pulse from master")
	return &pb.Pump{Oxygen: int32(1)}, nil
}

func main() {

	flag.Parse()

	/*Get pod ip and store it in etcd:*/
	podIp := os.Getenv("CONTAINER_IP")
	failVal, _ = strconv.Atoi(os.Getenv("FAIL"))
	log.Printf("ip address: %s", podIp)
	var err error
	etcdClient, err = clientv3.New(clientv3.Config{Endpoints: []string{"etcd.mr.svc.cluster.local:2379"}})
	if err != nil {
		log.Fatal(err)
	}
	defer etcdClient.Close()
	podIp = podIp + ":80"
	go func(ip string) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		key := fmt.Sprintf("/mapreduce/pods/%s/ip", ip)
		_, err := etcdClient.Put(ctx, key, ip)
		if err != nil {
			log.Fatal("failed to upload job meta: %w", err)
		}
	}(podIp)

	//accountName := os.Getenv("AZURE_STORAGE_ACCOUNT")
	accountName := "sysprojectstorage"
	accountKey := os.Getenv("AZURE_STORAGE_KEY")

	if accountName == "" || accountKey == "" {
		log.Fatalf("Missing Azure credentials: AZURE_STORAGE_ACCOUNT or AZURE_STORAGE_KEY")
	}

	cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		log.Fatalf("Failed to create shared key credential: %v", err)
	}

	blobClient, err := azblob.NewClientWithSharedKeyCredential(
		fmt.Sprintf("https://%s.blob.core.windows.net/", accountName),
		cred,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to create Azure blob client: %v", err)
	}

	/*Download mapper python file*/
	f, err := os.Create("/mapper.py")
	if err != nil {
		log.Fatalf("Failed to create python mapper file: %v", err)
	}
	_, err = blobClient.DownloadFile(context.TODO(), "python", "mapper.py", f, nil)
	if err != nil {
		_ = os.Remove("/mapper.py")
		log.Fatalf("Failed to download python mapper file: %v", err)
	}
	/*Download reducer python file*/
	f, err = os.Create("/reducer.py")
	if err != nil {
		log.Fatalf("Failed to create python reducer file: %v", err)
	}
	_, err = blobClient.DownloadFile(context.TODO(), "python", "reducer.py", f, nil)
	if err != nil {
		_ = os.Remove("/reducer.py")
		log.Fatalf("Failed to download python reducer file: %v", err)
	}

	s2 := &server{
		blobClient: blobClient,
	}

	log.Printf("Started application with following vals: port: %v", *port)
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s1 := grpc.NewServer()

	pb.RegisterMapReduceServer(s1, s2)
	log.Printf("server listening at %v", lis.Addr())
	if err := s1.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
