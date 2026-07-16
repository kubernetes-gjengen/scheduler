package common

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	k8 "k8s.io/api/core/v1"

	gograph "github.com/dominikbraun/graph"
)

var k8sSystemLabelPrefixes = []string{
	"kubernetes.io/",
	"beta.kubernetes.io/",
	"node.kubernetes.io/",
	"node-role.kubernetes.io/",
}

func IsSystemLabel(key string) bool {
	for _, prefix := range k8sSystemLabelPrefixes {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			return true
		}
	}
	return false
}

func PodToVertex(pod k8.Pod) *Node {
	networkComString := ""
	if len(pod.Spec.Containers[0].Env) > 0 {
		for _, env := range pod.Spec.Containers[0].Env {
			if env.Name == "NetworkComRequirements" {
				networkComString = env.Value
			}
		}
	}
	jsonNodeSelector, _ := json.Marshal(pod.Spec.NodeSelector)
	nodeSelector := string(jsonNodeSelector)
	// println("PodToVertex with CPU request", pod.Spec.Containers[0].Resources.Requests.Cpu().MilliValue(), "and memory request", pod.Spec.Containers[0].Resources.Requests.Memory().Value())
	return &Node{Name: pod.Name, Type: "pod", Properties: map[string]string{"nodeName": pod.Spec.NodeName, "networkComRequirements": networkComString, "status": string(pod.Status.Phase), "cpu": strconv.Itoa(int(pod.Spec.Containers[0].Resources.Requests.Cpu().MilliValue())), "memory": strconv.Itoa(int(pod.Spec.Containers[0].Resources.Requests.Memory().Value())), "nodeSelector": nodeSelector, "schedulerName": pod.Spec.SchedulerName}}
}

func NodeToVertex(node k8.Node, kind string) *Node {
	jsonLabels, _ := json.Marshal(node.Labels)
	labels := string(jsonLabels)
	return &Node{Name: node.Name, Type: kind, Properties: map[string]string{"cpu": strconv.Itoa(int(node.Status.Allocatable.Cpu().MilliValue())), "memory": strconv.Itoa(int(node.Status.Allocatable.Memory().Value())), "labels": labels}}
}

func VertexAttributes(kind string) []func(*gograph.VertexProperties) {
	switch kind {
	case "pod":
		return []func(*gograph.VertexProperties){
			gograph.VertexAttribute("shape", "ellipse"), gograph.VertexAttribute("colorscheme", "greens3"), gograph.VertexAttribute("style", "filled"), gograph.VertexAttribute("color", "2"), gograph.VertexAttribute("fillcolor", "1"),
		}
	case "pending_pod":
		return []func(*gograph.VertexProperties){
			gograph.VertexAttribute("shape", "ellipse"), gograph.VertexAttribute("colorscheme", "reds3"), gograph.VertexAttribute("style", "filled"), gograph.VertexAttribute("color", "2"), gograph.VertexAttribute("fillcolor", "1"),
		}
	case "node":
		return []func(*gograph.VertexProperties){
			gograph.VertexAttribute("shape", "rect"), gograph.VertexAttribute("colorscheme", "blues3"), gograph.VertexAttribute("style", "filled"), gograph.VertexAttribute("color", "2"), gograph.VertexAttribute("fillcolor", "1"),
		}
	case "offline_node":
		return []func(*gograph.VertexProperties){
			gograph.VertexAttribute("shape", "rect"), gograph.VertexAttribute("colorscheme", "reds3"), gograph.VertexAttribute("style", "filled"), gograph.VertexAttribute("color", "2"), gograph.VertexAttribute("fillcolor", "1"),
		}
	}
	return nil
}

// RemoveVertex removes the given vertex and all its relations
// Returns the given graph if the vertex doesnt exist
func RemoveVertex(graph gograph.Graph[string, *Node], vertex string) gograph.Graph[string, *Node] {
	_, err := graph.Vertex(vertex)
	// Check if the vertex exists. If not, return the input graph
	if err != nil {
		return graph
	}
	edges, _ := graph.Edges()
	for _, edge := range edges {
		if edge.Source == vertex || edge.Target == vertex {
			graph.RemoveEdge(edge.Source, edge.Target)
		}
	}
	graph.RemoveVertex(vertex)
	return graph
}

func EdgeAttributes(kind string, link Link) []func(*gograph.EdgeProperties) {
	switch kind {
	case "networkComRequirement":
		return []func(*gograph.EdgeProperties){
			gograph.EdgeAttribute("type", "networkComRequirement"), gograph.EdgeAttribute("label", strconv.Itoa(link.Latency)+"ms "+fmt.Sprintf("%.2f", link.Throughput)+"mbps"), gograph.EdgeAttribute("color", "green"), gograph.EdgeAttribute("latency", strconv.Itoa(link.Latency)), gograph.EdgeAttribute("throughput", fmt.Sprintf("%.2f", link.Throughput)),
		}
	case "connection":
		return []func(*gograph.EdgeProperties){
			gograph.EdgeAttribute("type", "connection"), gograph.EdgeAttribute("throughput", fmt.Sprintf("%.2f", link.Throughput)), gograph.EdgeAttribute("latency", strconv.Itoa(link.Latency)), gograph.EdgeAttribute("label", strconv.Itoa(link.Latency)+"ms "+fmt.Sprintf("%.2f", link.Throughput)+"mbps"),
		}
	case "offline_connection":
		return []func(*gograph.EdgeProperties){
			gograph.EdgeAttribute("type", "offline_connection"), gograph.EdgeAttribute("throughput", fmt.Sprintf("%.2f", link.Throughput)), gograph.EdgeAttribute("latency", strconv.Itoa(link.Latency)), gograph.EdgeAttribute("label", strconv.Itoa(link.Latency)+"ms "+fmt.Sprintf("%.2f", link.Throughput)+"mbps"), gograph.EdgeAttribute("style", "dotted"),
		}
	case "assign":
		return []func(*gograph.EdgeProperties){
			gograph.EdgeAttribute("type", "assign"),
		}
	}
	return nil
}

func NodesWithPrefix(graph gograph.Graph[string, *Node], prefix string) []string {
	nodes := []string{}
	vertices, _ := graph.AdjacencyMap()
	for vertex := range vertices {
		node, _ := graph.Vertex(vertex)
		if strings.HasPrefix(node.Name, prefix) {
			nodes = append(nodes, node.Name)
		}
	}
	return nodes
}

// AssignedNode Returns the NodeName a Pod is assigned to in the Graph. Returns "" if not assigned
func AssignedNode(graph gograph.Graph[string, *Node], podName string) string {
	edges, _ := graph.Edges()
	for _, edge := range edges {
		if edge.Source == podName && edge.Properties.Attributes["type"] == "assign" {
			return edge.Target
		}
	}
	return ""
}

// AssignedPods Returns a list of pods which are assigned to a given node
func AssignedPods(graph gograph.Graph[string, *Node], nodeName string) []string {
	assignedPods := []string{}
	edges, _ := graph.Edges()
	for _, edge := range edges {
		if edge.Target == nodeName && edge.Properties.Attributes["type"] == "assign" {
			assignedPods = append(assignedPods, edge.Source)
		}
	}
	return assignedPods
}

func IsNodeOnline(node *k8.Node) bool {
	for _, cond := range node.Status.Conditions {
		if cond.Type == k8.NodeReady {
			return cond.Status == k8.ConditionTrue
		}
	}
	return false
}
