package controller

import (
	"github.com/emicklei/go-restful"
	"github.com/golang/glog"
	"github.com/yarntime/hybridjob/pkg/client"
	"github.com/yarntime/hybridjob/pkg/types"
	"net"
	"net/http"
	"strconv"
)

type RestServer struct {
	address         string
	port            int
	hybridJobClient *client.HybridJobClient
}

func NewRestServer(config *Config) *RestServer {
	return &RestServer{
		address:         config.ServeAddress,
		port:            config.ServePort,
		hybridJobClient: config.HybridJobClient,
	}
}

func (rs *RestServer) Register(container *restful.Container) {
	ws := new(restful.WebService)
	ws.
		Path("/api/v1").
		Consumes(restful.MIME_JSON).
		Produces(restful.MIME_JSON) // you can specify this per route as well

	ws.Route(ws.GET("/namespaces/{namespace}/hybridjob/{name}/status").To(rs.GetJobStatus).
		Doc("get hybrid job status").
		Operation("get hybrid job status").
		Param(ws.PathParameter("namespace", "the namespace of the target hybird job")).
		Param(ws.PathParameter("name", "the name of the target hybrid job")).
		Writes(types.HybridJobStatus{}))

	container.Add(ws)
}

func (rs *RestServer) GetJobStatus(req *restful.Request, res *restful.Response) {
	namespace := req.PathParameter("namespace")
	name := req.PathParameter("name")
	hybridJob, err := rs.hybridJobClient.Get(name, namespace)
	if err != nil {
		res.WriteErrorString(http.StatusInternalServerError, err.Error())
	}
	if err := res.WriteEntity(hybridJob.Status); err != nil {
		res.WriteErrorString(http.StatusInternalServerError, err.Error())
	}
}

func (rs *RestServer) Run(stop chan struct{}) {
	wsContainer := restful.NewContainer()
	wsContainer.Router(restful.CurlyRouter{})

	rs.Register(wsContainer)

	glog.Infof("start listening on %s:%d", rs.address, rs.port)
	server := &http.Server{Addr: net.JoinHostPort(rs.address, strconv.Itoa(rs.port)), Handler: wsContainer}
	glog.Fatal(server.ListenAndServe())
}
