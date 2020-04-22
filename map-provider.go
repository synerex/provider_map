package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	gosocketio "github.com/mtfelian/golang-socketio"
	fleet "github.com/synerex/proto_fleet"
	pt "github.com/synerex/proto_ptransit"
	api "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"
)

// map provider provides map information to Web Service through socket.io.

var (
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	port            = flag.Int("port", 10080, "Map Provider Listening Port")
	mu              sync.Mutex
	version         = "0.01"
	assetsDir       http.FileSystem
	ioserv          *gosocketio.Server
	sxServerAddress string
)

// assetsFileHandler for static Data
func assetsFileHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return
	}

	file := r.URL.Path
	//	log.Printf("Open File '%s'",file)
	if file == "/" {
		file = "/index.html"
	}
	f, err := assetsDir.Open(file)
	if err != nil {
		log.Printf("can't open file %s: %v\n", file, err)
		return
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		log.Printf("can't open file %s: %v\n", file, err)
		return
	}
	http.ServeContent(w, r, file, fi.ModTime(), f)
}

func run_server() *gosocketio.Server {

	currentRoot, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	d := filepath.Join(currentRoot, "mclient", "build")

	assetsDir = http.Dir(d)
	log.Println("AssetDir:", assetsDir)

	assetsDir = http.Dir(d)
	server := gosocketio.NewServer()

	server.On(gosocketio.OnConnection, func(c *gosocketio.Channel) {
		log.Printf("Connected from %s as %s", c.IP(), c.Id())
		// do something.
	})

	server.On(gosocketio.OnDisconnection, func(c *gosocketio.Channel) {
		log.Printf("Disconnected from %s as %s", c.IP(), c.Id())
	})

	return server
}

type MapMarker struct {
	mtype int32   `json:"mtype"`
	id    int32   `json:"id"`
	lat   float32 `json:"lat"`
	lon   float32 `json:"lon"`
	angle float32 `json:"angle"`
	speed int32   `json:"speed"`
}

func (m *MapMarker) GetJson() string {
	s := fmt.Sprintf("{\"mtype\":%d,\"id\":%d,\"lat\":%f,\"lon\":%f,\"angle\":%f,\"speed\":%d}",
		m.mtype, m.id, m.lat, m.lon, m.angle, m.speed)
	return s
}

func supplyRideCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
	flt := &fleet.Fleet{}
	//	dt := sp.Cdata.Entity
	//	fmt.Printf("Got data %d: %v",len(dt), dt)
	err := proto.Unmarshal(sp.Cdata.Entity, flt)
	if err == nil {
		mm := &MapMarker{
			mtype: int32(0),
			id:    flt.VehicleId,
			lat:   flt.Coord.Lat,
			lon:   flt.Coord.Lon,
			angle: flt.Angle,
			speed: flt.Speed,
		}
		mu.Lock()
		ioserv.BroadcastToAll("event", mm.GetJson())
		mu.Unlock()
	} else { // err
		log.Printf("Err UnMarshal %v", err)
	}
}

func subscribeRideSupply(client *sxutil.SXServiceClient) {
	for {
		ctx := context.Background() //
		err := client.SubscribeSupply(ctx, supplyRideCallback)
		log.Printf("Error:Supply %s\n", err.Error())
		// we need to restart

		time.Sleep(5 * time.Second) // wait 5 seconds to reconnect
		newClt := sxutil.GrpcConnectServer(sxServerAddress)
		if newClt != nil {
			log.Printf("Reconnect server [%s]\n", sxServerAddress)
			client.Client = newClt
		}
	}
}

func supplyPTCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {
	pt := &pt.PTService{}
	err := proto.Unmarshal(sp.Cdata.Entity, pt)

	if err == nil { // get PT
		//		fmt.Printf("Receive PT: %#v", *pt)

		mm := &MapMarker{
			mtype: pt.VehicleType, // depends on type of GTFS: 1 for Subway, 2, for Rail, 3 for bus
			id:    pt.VehicleId,
			lat:   float32(pt.Lat),
			lon:   float32(pt.Lon),
			angle: pt.Angle,
			speed: pt.Speed,
		}
		mu.Lock()
		if mm.lat > 10 {
			ioserv.BroadcastToAll("event", mm.GetJson())
		}
		mu.Unlock()
	}
}

func subscribePTSupply(client *sxutil.SXServiceClient) {
	ctx := context.Background() //
	err := client.SubscribeSupply(ctx, supplyPTCallback)
	log.Printf("Error:Supply %s\n", err.Error())
}

// just for stat debug
func monitorStatus() {
	for {
		sxutil.SetNodeStatus(int32(runtime.NumGoroutine()), "MapGoroute")
		time.Sleep(time.Second * 3)
	}
}

func main() {
	log.Printf("MapProvider(%s) built %s sha1 %s", sxutil.GitVer, sxutil.BuildTime, sxutil.Sha1Ver)

	flag.Parse()
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)

	channelTypes := []uint32{pbase.RIDE_SHARE}
	srv, rerr := sxutil.RegisterNode(*nodesrv, "MapProvider", channelTypes, nil)
	if rerr != nil {
		log.Fatal("Can't register node ", rerr)
	}
	log.Printf("Connecting SynerexServer at [%s]\n", srv)
	wg := sync.WaitGroup{} // for syncing other goroutines
	ioserv = run_server()
	fmt.Printf("Running Map Server..\n")
	if ioserv == nil {
		fmt.Printf("Can't run websocket server.\n")
		os.Exit(1)
	}

	client := sxutil.GrpcConnectServer(srv)
	sxServerAddress = srv
	argJSON := fmt.Sprintf("{Client:Map:RIDE}")
	rideClient := sxutil.NewSXServiceClient(client, pbase.RIDE_SHARE, argJSON)

	argJSON2 := fmt.Sprintf("{Client:Map:PT}")
	pt_client := sxutil.NewSXServiceClient(client, pbase.PT_SERVICE, argJSON2)

	wg.Add(1)
	go subscribeRideSupply(rideClient)

	wg.Add(1)
	go subscribePTSupply(pt_client)

	go monitorStatus() // keep status

	serveMux := http.NewServeMux()

	serveMux.Handle("/socket.io/", ioserv)
	serveMux.HandleFunc("/", assetsFileHandler)

	log.Printf("Starting Map Provider %s  on port %d", version, *port)
	err := http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", *port), serveMux)
	if err != nil {
		log.Fatal(err)
	}

	wg.Wait()

}
