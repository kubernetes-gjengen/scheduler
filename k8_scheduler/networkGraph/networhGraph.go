// Package networkgraph contains the offline integrated networkgraph representation
package networkgraph

import (
	"encoding/json"
	"errors"
	"k8_scheduler/common"
	"log/slog"
	"time"

	k8 "k8s.io/api/core/v1"

	gograph "github.com/dominikbraun/graph"
)

var (
	networkKnowledge []common.Link
	k8Knowledge      common.K8Knowledge
)

// The graph which represents the current networkgraph
var graph gograph.Graph[string, *common.Node] = gograph.New(func(node *common.Node) string {
	return node.Name
}, gograph.Directed())

func GetGraph() gograph.Graph[string, *common.Node] {
	verifyEdges()
	return graph
}

// AddPod adds a Kubernetes Pod to the networkgraph
// If the pod exists already, the function will throw an error
// Only add Pods which are pending or running
// If the pod is assigned to a Node, add the edge as well
// Returns true if the operation was successful
func AddPod(pod *k8.Pod) (bool, error) {
	// Check if there is already a pod with the same name
	existingPod, err := graph.Vertex(pod.Name)
	if err != nil {
	} else if existingPod != nil {
		return false, errors.New("pod exists")
	}
	if pod.Status.Phase == k8.PodRunning || pod.Status.Phase == k8.PodPending {
		err := graph.AddVertex(common.PodToVertex(*pod), common.VertexAttributes("pod")...)
		if err != nil {
			panic(err)
		}
		verifyEdges()
		return true, nil
	} else {
		return false, nil
	}
}

// UpdatePod updates the pod by removing the old pod and adding a new one. It reapplies the edges after adding the new one
func UpdatePod(oldPod *k8.Pod, newPod *k8.Pod) {
	common.RemoveVertex(graph, oldPod.Name)
	AddPod(newPod)
}

func DeletePod(podName string) {
	graph = common.RemoveVertex(graph, podName)
}

// AddNode adds a node to the graph
func AddNode(node *k8.Node, isOnline bool) {
	if isOnline {
		graph.AddVertex(common.NodeToVertex(*node, "node"), common.VertexAttributes("node")...)
	} else {
		graph.AddVertex(common.NodeToVertex(*node, "node"), common.VertexAttributes("offline_node")...)
	}
	verifyEdges()
}

func UpdateNode(oldNode *k8.Node, newNode *k8.Node, newNodeIsOnline bool) {
	common.RemoveVertex(graph, oldNode.Name)
	AddNode(newNode, newNodeIsOnline)
	// verifyEdges is executed in AddNode function
}

func SetNetworkKnowledge(links []common.Link) {
	networkKnowledge = links
	applyNetworkKnowledge()
}

func GetK8Knowledge() common.K8Knowledge {
	return k8Knowledge
}

func applyNetworkKnowledge() {
	for _, link := range networkKnowledge {
		srcVertex, _ := graph.Vertex(link.Source)
		trgtVertex, _ := graph.Vertex(link.Target)
		if srcVertex == nil || trgtVertex == nil {
			continue
		}

		if srcVertex.Type == "offline_node" || trgtVertex.Type == "offline_node" || time.Since(time.Unix(int64(link.Timestamp), 0)) > (time.Second*time.Duration(common.Cfg.Stability.LinkTimeout)) {
			graph.AddEdge(link.Source, link.Target, common.EdgeAttributes("offline_connection", link)...)
		} else {
			graph.AddEdge(link.Source, link.Target, common.EdgeAttributes("connection", link)...)
		}
	}
}

func SetK8Knowledge(k8k common.K8Knowledge) {
	k8Knowledge = k8k
	applyK8Knowledge()
}

// applyK8Knowledge applies the knowledge we have about the existance of pods.
// IT DELETES ALL PODS AND SETS THEIR STATE TO THE KNOWELDGE WE HAVE OF THE CLUSTER.
// The function uses the local k8Knowledge variable for that
func applyK8Knowledge() {
	// First remove old pods
	adjacencyMap, _ := graph.AdjacencyMap()
	for vertexID := range adjacencyMap {
		vertex, _ := graph.Vertex(vertexID)
		if vertex.Type == "pod" || vertex.Type == "node" {
			graph = common.RemoveVertex(graph, vertexID)
		}
	}

	for _, pod := range k8Knowledge.Pods {
		if pod.DeletionTimestamp == nil && (pod.Status.Phase == k8.PodRunning || (pod.Status.Phase == k8.PodPending && (pod.Spec.SchedulerName == "custom-scheduler" || pod.Spec.NodeName != ""))) {
			slog.Debug("pod added to graph", "pod", pod.Name)
			AddPod(&pod)
		}
	}

	for _, node := range k8Knowledge.Nodes {
		AddNode(&node, common.IsNodeOnline(&node))
	}
}

func verifyEdges() {
	// Remove all Edges
	edges, _ := graph.Edges()
	for _, edge := range edges {
		err := graph.RemoveEdge(edge.Source, edge.Target)
		if err != nil {
			slog.Warn("remove edge error", "err", err)
		}
	}

	// Apply Network Knowledge
	applyNetworkKnowledge()

	adjacencyMap, _ := graph.AdjacencyMap()
	// fmt.Println("Adjacency Map:", adjacencyMap)

	for vertex := range adjacencyMap {
		// println(vertex)
		node, _ := graph.Vertex(vertex)
		if node.Type == "pod" {
			if node.Properties["nodeName"] != "" {
				adjNode, _ := graph.Vertex(node.Properties["nodeName"])
				if adjNode == nil {
					continue
				}
				graph.AddEdge(node.Name, adjNode.Name, common.EdgeAttributes("assign", common.Link{})...)

			}

			if node.Properties["networkComRequirements"] != "" {
				comReqStr := node.Properties["networkComRequirements"]
				comReq := []common.NetworkComRequirement{}
				json.Unmarshal([]byte(comReqStr), &comReq)
				for _, req := range comReq {
					destinations := common.NodesWithPrefix(graph, req.Target)
					for _, destination := range destinations {
						graph.AddEdge(node.Name, destination, common.EdgeAttributes("networkComRequirement", common.Link{Source: node.Name, Target: destination, Latency: req.Latency, Throughput: req.Throughput})...)
					}
				}
			}
		}
	}
}
