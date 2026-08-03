package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"maps"
	"os"
	"slices"
	"time"

	"k8_scheduler/common"
	"k8_scheduler/scheduler"
	"k8_scheduler/visualizer"

	networkgraph "k8_scheduler/networkGraph"

	gograph "github.com/dominikbraun/graph"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "k8_scheduler/proto"

	k8 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	clientID = "scheduler"
)

func main() {
	if len(os.Args) < 2 {
		println("The scheduler expects two arguments.")
		println("arg1: /path/to/kubeconfig")
		println("		This will be used to connect to the K8 cluster")
		println("arg2: /path/to/schedulerconfig")
		println("		This will be used to configure the scheduler. Find and example config.yaml file here: https://github.com/kr-0-n/master_thesis/blob/main/k8_scheduler/common/config.yaml")
		panic("Incorrect Arguments")
	}

	common.LoadConfig(os.Args[2])

	var lvl slog.Level
	lvl.UnmarshalText([]byte(common.Cfg.LogLevel))
	timeFmt := map[string]string{
		"time":     "15:04:05",
		"datetime": "2006-01-02 15:04:05",
	}[common.Cfg.LogTimeFormat]
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: lvl,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if timeFmt != "" && a.Key == slog.TimeKey {
				if t, ok := a.Value.Any().(time.Time); ok {
					return slog.String(slog.TimeKey, t.Format(timeFmt))
				}
			}
			return a
		},
	})))

	slog.Info("penalty configuration",
		"move_pod", common.Cfg.Penalties.MovePod,
		"unconnected_pod", common.Cfg.Penalties.UnconnectedPod,
		"label", common.Cfg.Penalties.Label,
		"latency", common.Cfg.Penalties.Latency,
		"throughput", common.Cfg.Penalties.Throughput,
		"stability", common.Cfg.Penalties.Stability,
		"spread", common.Cfg.Penalties.Spread,
		"cpu", common.Cfg.Penalties.CPU,
		"memory", common.Cfg.Penalties.Memory,
	)

	ticker := time.NewTicker(10 * time.Second)
	quit := make(chan struct{})

	clientset := connectToK8s()

	networkLinksMap := make(map[string]common.Link)

	opts := mqtt.NewClientOptions()
	opts.AddBroker(common.Cfg.MQTT.Host)
	opts.SetClientID(clientID)
	opts.OnConnect = connectHandler
	opts.OnConnectionLost = connectLostHandler

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	client.Subscribe(common.Cfg.MQTT.Topic, 1, func(c mqtt.Client, m mqtt.Message) {
		// fmt.Println("Received MSG")
		var link common.Link
		json.Unmarshal(m.Payload(), &link)
		networkLinksMap[link.Source+";"+link.Target] = link
		// fmt.Println(networkLinksMap)
	})

	stop := make(chan struct{})
	defer close(stop)

	go func() {
		for {
			select {
			case <-ticker.C:
				k8knowledge := queryK8API(*clientset)
				networkgraph.SetK8Knowledge(k8knowledge)

				nodeOnline := map[string]bool{}
				for _, node := range networkgraph.GetK8Knowledge().Nodes {
					cpu := node.Status.Allocatable[v1.ResourceCPU]
					mem := node.Status.Allocatable[v1.ResourceMemory]
					online := common.IsNodeOnline(&node)
					nodeOnline[node.Name] = online
					args := []any{"name", node.Name, "online", online, "cpu", cpu.String(), "mem", mem.String()}
					for k, v := range node.Labels {
						if !common.IsSystemLabel(k) {
							args = append(args, k, v)
						}
					}
					slog.Debug("node", args...)
				}
				unscheduledPods := []k8.Pod{}
				terminatingPodsExist := false
				for _, pod := range networkgraph.GetK8Knowledge().Pods {
					var cpuReq, memReq resource.Quantity
					for _, c := range pod.Spec.Containers {
						cpuReq.Add(c.Resources.Requests[v1.ResourceCPU])
						memReq.Add(c.Resources.Requests[v1.ResourceMemory])
					}
					slog.Debug("pod", "name", pod.Name, "phase", pod.Status.Phase, "node", pod.Spec.NodeName, "terminating", pod.DeletionTimestamp != nil, "cpu", cpuReq.String(), "mem", memReq.String())

					// A pod stuck terminating on a node that's down will never
					// actually go away until the node comes back - the kubelet
					// has to ack the deletion. Don't let it block scheduling forever.
					if pod.DeletionTimestamp != nil {
						if nodeOnline[pod.Spec.NodeName] {
							terminatingPodsExist = true
						}
						continue
					}
					if pod.Status.Phase == "Pending" && pod.Spec.NodeName == "" && pod.Spec.SchedulerName == "custom-scheduler" {
						unscheduledPods = append(unscheduledPods, pod)
					}
				}

				networkgraph.SetNetworkKnowledge(slices.Collect(maps.Values(networkLinksMap)))
				currentGraph := networkgraph.GetGraph()
				if common.Cfg.Visualize {
					visualizer.DrawGraph(currentGraph, "pre")
				}
				if len(unscheduledPods) > 0 && !terminatingPodsExist {
					slog.Info("scheduling", "pods", len(unscheduledPods))
					newGraph := scheduler.SchedulePods(currentGraph, unscheduledPods, common.Cfg.Debug, false)
					realiseGraph(newGraph, clientset)
					if common.Cfg.Visualize {
						visualizer.DrawGraph(newGraph, "post-schedule")
					}
				} else if !terminatingPodsExist {
					k8k := networkgraph.GetK8Knowledge()
					slog.Info("optimizing", "pods", len(k8k.Pods), "nodes", len(k8k.Nodes))
					newGraph := scheduler.Optimize(currentGraph, common.Cfg.Debug, false)
					realiseGraph(newGraph, clientset)
					if common.Cfg.Visualize {
						visualizer.DrawGraph(newGraph, "post-optimize")
					}
				} else if terminatingPodsExist {
					slog.Info("skipping: terminating pods exist")
				}

			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()
	time.Sleep(10 * time.Second)
	select {}
}

func queryK8API(clientset kubernetes.Clientset) common.K8Knowledge {
	ctx := context.Background()

	podList, err := clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		slog.Warn("k8 api error", "err", err)
	}

	var defaultPods []k8.Pod
	reservedCPU := map[string]resource.Quantity{}
	reservedMem := map[string]resource.Quantity{}

	for _, pod := range podList.Items {

		if pod.Namespace == metav1.NamespaceDefault {
			defaultPods = append(defaultPods, pod)
			continue
		}

		if pod.Status.Phase != k8.PodRunning && pod.Status.Phase != k8.PodPending {
			continue
		}

		node := pod.Spec.NodeName
		if node == "" {
			continue
		}

		for _, c := range pod.Spec.Containers {
			cpuReq := c.Resources.Requests[v1.ResourceCPU]
			memReq := c.Resources.Requests[v1.ResourceMemory]

			if existing, ok := reservedCPU[node]; ok {
				existing.Add(cpuReq)
				reservedCPU[node] = existing
			} else {
				reservedCPU[node] = cpuReq.DeepCopy()
			}

			if existing, ok := reservedMem[node]; ok {
				existing.Add(memReq)
				reservedMem[node] = existing
			} else {
				reservedMem[node] = memReq.DeepCopy()
			}
		}
	}

	nodeList, err := clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		slog.Warn("k8 api error", "err", err)
	}

	nodes := nodeList.Items

	for i := range nodes {
		node := &nodes[i]

		if reserved, ok := reservedCPU[node.Name]; ok {
			alloc := node.Status.Allocatable[v1.ResourceCPU]
			alloc.Sub(reserved)
			node.Status.Allocatable[v1.ResourceCPU] = alloc
		}

		if reserved, ok := reservedMem[node.Name]; ok {
			alloc := node.Status.Allocatable[v1.ResourceMemory]
			alloc.Sub(reserved)
			node.Status.Allocatable[v1.ResourceMemory] = alloc
		}
	}

	return common.K8Knowledge{
		Pods:  defaultPods,
		Nodes: nodes,
	}
}

func queryLinkAPI() []common.Link {
	conn, err := grpc.Dial("127.0.0.1:50051", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		slog.Error("grpc connect failed", "err", err)
		panic(err)
	}
	defer conn.Close()

	client := pb.NewLinkServiceClient(conn)

	// Perform RPC call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	resp, err := client.GetAllLinks(ctx, &pb.EmptyMessage{})
	if err != nil {
		slog.Warn("grpc call failed", "err", err)
		return []common.Link{}
	}

	if resp == nil {
		slog.Error("grpc nil response")
		panic("received nil response from link service")
	}

	tempLinks := []common.Link{}

	for _, link := range resp.Links {
		tempLinks = append(tempLinks, common.Link{Source: link.From, Target: link.To, Latency: int(link.Latency), Throughput: float64(link.Throughput), Timestamp: int(*link.Timestamp)})
	}

	return tempLinks
}

func connectToK8s() *kubernetes.Clientset {
	config, err := clientcmd.BuildConfigFromFlags("", os.Args[1])
	if err != nil {
		// Try to use in-cluster configuration if the kubeconfig is not available
		config, err = rest.InClusterConfig()
		if err != nil {
			panic(err.Error())
		}
	}
	// Create the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	// fmt.Println("Successfully connected to the Kubernetes API")
	return clientset
}

func realiseGraph(graph gograph.Graph[string, *common.Node], clientset *kubernetes.Clientset) {
	// println("Realising graph")
	edges, _ := graph.Edges()
	for _, edge := range edges {
		if edge.Properties.Attributes["type"] == "assign" {
			// Is this pod already assigned to the right node?
			vertex, _ := graph.Vertex(edge.Source)
			// fmt.Printf("Pod %s with properties\n", edge.Source)
			// for key, value := range vertex.Properties {
			// 	// fmt.Printf("  %s: %s\n", key, value)
			// }
			if vertex.Properties["nodeName"] == edge.Target {
				// fmt.Println("Pod already assigned to node:", edge.Target)
				continue
			} else if vertex.Properties["nodeName"] == "" {
				// fmt.Println("Pod needs binding")
				// This pod is unscheduled, but assigned
				err := clientset.CoreV1().Pods("default").Bind(context.TODO(), &k8.Binding{
					ObjectMeta: metav1.ObjectMeta{
						Name:      edge.Source,
						Namespace: "default",
					},
					Target: k8.ObjectReference{
						Kind:      "Node",
						Name:      edge.Target,
						Namespace: "default",
					},
				}, metav1.CreateOptions{})
				if err != nil {
					slog.Warn("bind error", "pod", edge.Source, "node", edge.Target, "err", err)
				} else {
					slog.Info("pod bound", "pod", edge.Source, "node", edge.Target)
				}

			} else if vertex.Properties["nodeName"] != edge.Target {
				slog.Info("pod moved", "pod", edge.Source, "from", vertex.Properties["nodeName"], "to", edge.Target)
				err := clientset.CoreV1().Pods("default").Delete(context.TODO(), edge.Source, metav1.DeleteOptions{})
				if err != nil {
					slog.Warn("delete error", "pod", edge.Source, "err", err)
				} else {
					slog.Info("pod deleted for rescheduling", "pod", edge.Source)
				}
				continue
			}

		}
		// visualizer.DrawGraph(graph)
	}
}

var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
	slog.Info("mqtt connected")
}

var connectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
	slog.Warn("mqtt connection lost", "err", err)
}
