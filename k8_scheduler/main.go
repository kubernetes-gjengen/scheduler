package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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
				log.Println("==================================================")
				k8knowledge := queryK8API(*clientset)
				networkgraph.SetK8Knowledge(k8knowledge)

				for _, node := range networkgraph.GetK8Knowledge().Nodes {

					var nodeCondition k8.NodeCondition
					for _, cond := range node.Status.Conditions {
						if cond.Type == k8.NodeReady {
							nodeCondition = cond
						}
					}
					cpu := node.Status.Allocatable[v1.ResourceCPU]
					mem := node.Status.Allocatable[v1.ResourceMemory]
					log.Printf(
						"Found Node: %s, Online %t, Status %s, Available CPU: %s, Available Mem: %s\n",
						node.Name,
						common.IsNodeOnline(&node),
						&nodeCondition,
						cpu.String(),
						mem.String(),
					)
				}
				unscheduledPods := []k8.Pod{}
				terminatingPodsExist := false
				for _, pod := range networkgraph.GetK8Knowledge().Pods {
					log.Printf(
						"Found Pod: %s, Phase: %s, NodeName: %s, Scheduler: %s, DeletionTimestamp: %s, CPUReq: %s, MemReq: %s, Status: %s\n",
						pod.Name,
						pod.Status.Phase,
						pod.Spec.NodeName,
						pod.Spec.SchedulerName,
						func() string {
							if pod.DeletionTimestamp != nil {
								return pod.DeletionTimestamp.String()
							}
							return "<none>"
						}(),
						func() string {
							cpu, mem := resource.Quantity{}, resource.Quantity{}
							for _, c := range pod.Spec.Containers {
								cpu.Add(c.Resources.Requests[v1.ResourceCPU])
								mem.Add(c.Resources.Requests[v1.ResourceMemory])
							}
							return cpu.String()
						}(),
						func() string {
							cpu, mem := resource.Quantity{}, resource.Quantity{}
							for _, c := range pod.Spec.Containers {
								cpu.Add(c.Resources.Requests[v1.ResourceCPU])
								mem.Add(c.Resources.Requests[v1.ResourceMemory])
							}
							return mem.String()
						}(),
						&pod.Status.Conditions,
					)

					// Check if pod is terminating
					if pod.DeletionTimestamp != nil {
						terminatingPodsExist = true
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
					log.Println("Scheduling pods: ", len(unscheduledPods))
					newGraph := scheduler.SchedulePods(currentGraph, unscheduledPods, false, false)
					realiseGraph(newGraph, clientset)
					if common.Cfg.Visualize {
						visualizer.DrawGraph(newGraph, "post-schedule")
					}
				} else if !terminatingPodsExist {
					log.Println("Optimizing schedule", len(unscheduledPods))
					newGraph := scheduler.Optimize(currentGraph, false, false)
					realiseGraph(newGraph, clientset)
					if common.Cfg.Visualize {
						visualizer.DrawGraph(newGraph, "post-optimize")
					}
				} else if terminatingPodsExist {
					log.Println("Skipping scheduling: terminating pods still exist")
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
		log.Println("Error interacting with K8")
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
		log.Println("Error interacting with K8")
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
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewLinkServiceClient(conn)

	// Perform RPC call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	resp, err := client.GetAllLinks(ctx, &pb.EmptyMessage{})
	if err != nil {
		log.Printf("Error calling SendData: %v", err)
		return []common.Link{}
	}

	if resp == nil {
		log.Fatalf("Received nil response")
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
					log.Println(err)
				} else {
					log.Printf("Pod %s assigned to node %s\n", edge.Source, edge.Target)
				}

			} else if vertex.Properties["nodeName"] != edge.Target {
				log.Println("POD MOVEMENT: Pod is scheduled on", vertex.Properties["nodeName"], "and will be moved to", edge.Target)
				err := clientset.CoreV1().Pods("default").Delete(context.TODO(), edge.Source, metav1.DeleteOptions{})
				if err != nil {
					log.Println(err)
				} else {
					log.Printf("Pod %s Deleted. Waiting for K8s controller to reschedule\n", edge.Source)
				}
				continue
			}

		}
		// visualizer.DrawGraph(graph)
	}
}

var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
	fmt.Println("Connected to MQTT Broker")
}

var connectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
	fmt.Printf("Connection lost: %v", err)
}
