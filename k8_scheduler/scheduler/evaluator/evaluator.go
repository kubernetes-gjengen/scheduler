package evaluator

import (
	"encoding/json"
	"log/slog"
	gomath "math"
	"strconv"
	"time"

	"k8_scheduler/common"
	"k8_scheduler/math"

	gograph "github.com/dominikbraun/graph"
)

var UnavailabilityMap = make(map[string][]time.Time)

// ScoreBreakdown is the per-penalty-term cost of a candidate placement.
// Kept split (rather than a single summed float) so callers can log which
// term actually moved instead of just the total.
type ScoreBreakdown struct {
	Resources   float64
	Unconnected float64
	Latency     float64
	Throughput  float64
	Labels      float64
	Stability   float64
	Spread      float64
	MovePod     float64
}

func (s ScoreBreakdown) Total() float64 {
	return s.Resources + s.Unconnected + s.Latency + s.Throughput + s.Labels + s.Stability + s.Spread + s.MovePod
}

func EvaluateStep(oldGraph gograph.Graph[string, *common.Node], newGraph gograph.Graph[string, *common.Node], debug bool) ScoreBreakdown {
	breakdown := Evaluate(newGraph, debug)
	move_pod_penalty := common.Cfg.Penalties.MovePod
	old_assignments := []gograph.Edge[string]{}
	new_assignments := []gograph.Edge[string]{}

	old_graph_edges, _ := oldGraph.Edges()
	for _, edge := range old_graph_edges {
		if edge.Properties.Attributes["type"] == "assign" {
			old_assignments = append(old_assignments, edge)
		}
	}

	new_graph_edges, _ := newGraph.Edges()
	for _, edge := range new_graph_edges {
		if edge.Properties.Attributes["type"] == "assign" {
			new_assignments = append(new_assignments, edge)
		}
	}
	for _, old_edge := range old_assignments {
		found := false
		for _, new_edge := range new_assignments {
			if old_edge.Source == new_edge.Source && old_edge.Target == new_edge.Target {
				found = true
			}
		}
		if !found {
			breakdown.MovePod += float64(move_pod_penalty)
		}
	}
	return breakdown
}

func Evaluate(graph gograph.Graph[string, *common.Node], debug bool) ScoreBreakdown {
	graph_copy := graph
	edges, _ := graph_copy.Edges()
	for _, edge := range edges {
		if edge.Properties.Attributes["type"] == "networkComRequirement" || edge.Properties.Attributes["type"] == "offline_connection" {
			graph_copy.RemoveEdge(edge.Source, edge.Target)
		}
	}
	if debug {
		println("-------New Evaluation-------")
	}

	unconnected, latency, throughput := network_penalty(graph_copy, debug)

	breakdown := ScoreBreakdown{
		Resources:   resources_penalty(graph_copy, debug),
		Unconnected: unconnected,
		Latency:     latency,
		Throughput:  throughput,
		Labels:      labels_penalty(graph_copy, debug),
		Stability:   node_stability_penalty(graph_copy, debug),
		Spread:      spread_penalty(graph_copy, debug),
	}

	if debug {
		println("Evaluation:", breakdown.Total())
	}

	return breakdown
}

func resources_penalty(graph gograph.Graph[string, *common.Node], debug bool) float64 {
	vertices, _ := graph.AdjacencyMap()
	val := 0.0

	edges, _ := graph.Edges()

	for vertex := range vertices {
		node, _ := graph.Vertex(vertex)
		if node.Type != "node" {
			continue
		}

		var cpuLoad int64
		var memLoadBytes int64

		for _, edge := range edges {
			if edge.Target == node.Name && edge.Properties.Attributes["type"] == "assign" {
				pod, _ := graph.Vertex(edge.Source)

				cpuReq, _ := strconv.ParseInt(pod.Properties["cpu"], 10, 64)
				memReq, _ := strconv.ParseInt(pod.Properties["memory"], 10, 64)

				cpuLoad += cpuReq
				memLoadBytes += memReq
			}
		}

		cpuLimit, _ := strconv.ParseInt(node.Properties["cpu"], 10, 64)
		memLimitMiB, _ := strconv.ParseInt(node.Properties["memory"], 10, 64)

		memLoadMiB := memLoadBytes / (1024 * 1024)

		if cpuLoad > cpuLimit && cpuLimit > 0 {
			val += (float64(cpuLoad-cpuLimit) / float64(cpuLimit)) * float64(common.Cfg.Penalties.CPU)
		}

		if memLoadMiB > memLimitMiB && memLimitMiB > 0 {
			val += (float64(memLoadMiB-memLimitMiB) / float64(memLimitMiB)) * float64(common.Cfg.Penalties.Memory)
		}

		if debug {
			println("node", node.Name,
				"CPU load:", cpuLoad, "CPU limit:", cpuLimit,
				"Mem load MiB:", memLoadMiB, "Mem limit MiB:", memLimitMiB)
		}
	}

	if debug {
		println("Resources penalty:", val)
	}

	return val
}

func network_penalty(graph gograph.Graph[string, *common.Node], debug bool) (unconnectedVal float64, latencyVal float64, throughputVal float64) {
	edges, _ := graph.Edges()
	for _, edge := range edges {
		if edge.Properties.Attributes["type"] == "connection" {
			json, _ := json.Marshal(math.LinearFunction{M: 0, A: 0, C: 0})
			edge.Properties.Attributes["wanted_connection"] = string(json)
		}
	}

	vertices, _ := graph.AdjacencyMap()
	for vertex := range vertices {
		pod, _ := graph.Vertex(vertex)
		if pod.Type == "pod" {
			networkComRequirementsString := pod.Properties["networkComRequirements"]
			networkComRequirements := []common.NetworkComRequirement{}
			err := json.Unmarshal([]byte(networkComRequirementsString), &networkComRequirements)
			if err != nil {
				if debug {
					println("Error unmarshalling networkComRequirements for pod", pod.Name, ":", err)
				}
				continue
			}
			// println("Pod", pod.Name, "has networkComRequirements:", networkComRequirements[0].Target, networkComRequirements[0].Throughput, networkComRequirements[0].Latency)

			for _, networkComRequirement := range networkComRequirements {
				destinations := common.NodesWithPrefix(graph, networkComRequirement.Target)
				if len(destinations) == 0 {
					if debug {
						println("No nodes with prefix", networkComRequirement.Target, "found")
					}
					continue
				}
				// TODO This only considers a single Target, extend to multiple ones
				shortestPath, err := gograph.ShortestPath(graph, common.AssignedNode(graph, pod.Name), common.AssignedNode(graph, destinations[0]))
				if err != nil {
					if debug {
						println("Error finding shortest path between pod", common.AssignedNode(graph, pod.Name), "and", common.AssignedNode(graph, destinations[0]), ":", err)
					}
					unconnectedVal += float64(common.Cfg.Penalties.UnconnectedPod)
					continue
				}
				if len(shortestPath) == 0 {
					if debug {
						println("No path found between pod", common.AssignedNode(graph, pod.Name), "and", common.AssignedNode(graph, destinations[0]))
					}
					unconnectedVal += float64(common.Cfg.Penalties.UnconnectedPod)
					continue
				}

				accumulated_latency := 0
				// Calculate the latency penalty
				for i := range shortestPath[0 : len(shortestPath)-1] {
					edge, err := graph.Edge(shortestPath[i], shortestPath[i+1])
					if err != nil {
						slog.Error("path edge error", "err", err)
						panic(err)
					}
					lat, _ := strconv.ParseInt(edge.Properties.Attributes["latency"], 10, 64)
					accumulated_latency += int(lat)
				}
				if debug {
					println("Accumulated Latency on ", shortestPath, accumulated_latency)
				}
				if accumulated_latency >= networkComRequirement.Latency {
					latency_penalty := float64(common.Cfg.Penalties.Latency * (accumulated_latency - networkComRequirement.Latency))
					if debug {
						println("Latency penalty: ", latency_penalty)
					}
					latencyVal += latency_penalty
				}

				// Calculate the throughput penalty
				lastAdditionalOutput := math.LinearFunction{M: float64(networkComRequirement.Throughput), A: 0, C: 0}

				for i := range shortestPath[0 : len(shortestPath)-1] {
					if debug {
						println("Current Link", shortestPath[i], shortestPath[i+1])
						println("LastAdditionalOutput: ", lastAdditionalOutput.String())
					}

					old_link_wanted_service := math.LinearFunction{}
					edge, _ := graph.Edge(shortestPath[i], shortestPath[i+1])
					_ = json.Unmarshal([]byte(edge.Properties.Attributes["wanted_connection"]), &old_link_wanted_service)

					new_link_wanted_service := math.Multiply(lastAdditionalOutput, old_link_wanted_service)
					edge.Properties.Attributes["wanted_connection"] = new_link_wanted_service.String()

					throughput, err := strconv.ParseFloat(edge.Properties.Attributes["throughput"], 64)
					if err != nil {
						if debug {
							println("Error parsing throughput:", err)
						}
						continue
					}

					if new_link_wanted_service.M > throughput {
						// scale down the carried output to the link capacity
						lastAdditionalOutput = math.Devide(
							math.LinearFunction{M: throughput, A: 0, C: 0},
							old_link_wanted_service,
						)

						throughputVal += float64(common.Cfg.Penalties.Throughput) *
							(new_link_wanted_service.M - throughput)
					}
					// else: capacity is sufficient → lastAdditionalOutput unchanged
				}
			}

		}
	}

	return unconnectedVal, latencyVal, throughputVal
}

func labels_penalty(graph gograph.Graph[string, *common.Node], debug bool) float64 {
	val := 0.0
	vertices, _ := graph.AdjacencyMap()
	for vertex := range vertices {
		node, _ := graph.Vertex(vertex)
		if node.Type == "node" {
			if node.Properties["labels"] != "" {
				// println("Node", node.Name, "has labels:", node.Properties["labels"])
				var nodeLabelsMap map[string]string
				err := json.Unmarshal([]byte(node.Properties["labels"]), &nodeLabelsMap)
				if err != nil {
					if debug {
						println("Error unmarshalling labels for node", node.Name, ":", err)
					}
					continue
				}
				edges, _ := graph.Edges()
				for _, edge := range edges {
					if edge.Target == node.Name && edge.Properties.Attributes["type"] == "assign" {
						pod, _ := graph.Vertex(edge.Source)
						node_selector := pod.Properties["nodeSelector"]
						if node_selector != "" {
							var nodeSelectorMap map[string]string

							err := json.Unmarshal([]byte(node_selector), &nodeSelectorMap)
							if err != nil {
								if debug {
									println("Error unmarshalling labels for pod", pod.Name, ":", err)
								}
								continue
							}

							for key, value := range nodeSelectorMap {
								if nodeLabelsMap[key] != value {
									if debug {
										println("Pod", pod.Name, "has label", key, "with value", value, "but node", node.Name, "has label", key, "with value", nodeLabelsMap[key])
									}
									val += float64(common.Cfg.Penalties.Label)
								}
							}

						}
					}
				}

			}
		}
	}
	return val
}

// Every node adds to the stability penality.
// It is calculated like this: val += node_crashes / floating_average_window
func node_stability_penalty(graph gograph.Graph[string, *common.Node], debug bool) float64 {
	val := 0.0
	vertices, _ := graph.AdjacencyMap()
	for vertex := range vertices {
		node, err := graph.Vertex(vertex)
		if err != nil {
			slog.Warn("node not in graph", "node", vertex)
			continue
		}
		if node.Type != "node" {
			continue
		}
		currentTime := time.Now()
		crashes := 0
		for _, unavailableMoment := range UnavailabilityMap[node.Name] {
			if unavailableMoment.After(currentTime.Add(-1 * time.Minute * time.Duration(common.Cfg.Stability.FloatingAverageWindow))) {
				crashes += 1
			}
		}
		if debug {
			slog.Debug("node stability", "node", node.Name, "crashes", crashes)
		}
		val += float64(crashes / common.Cfg.Stability.FloatingAverageWindow)
	}
	return val
}

func spread_penalty(graph gograph.Graph[string, *common.Node], debug bool) float64 {
	val := 0.0
	numPods := 0
	numNodes := 0
	vertices, _ := graph.AdjacencyMap()
	for vertex := range vertices {
		node, err := graph.Vertex(vertex)
		if err != nil {
			slog.Warn("node not in graph", "node", vertex)
			continue
		}
		if node.Type == "node" {
			numNodes++
		}
		if node.Type == "pod" {
			numPods++
		}
	}

	avgPodsPerNode := float64(numPods) / float64(numNodes)

	for vertex := range vertices {
		node, err := graph.Vertex(vertex)
		if err != nil {
			slog.Warn("node not in graph", "node", vertex)
			continue
		}
		if node.Type == "node" {
			assignedPods := common.AssignedPods(graph, node.Name)
			val += gomath.Abs(float64(len(assignedPods))-avgPodsPerNode) * float64(common.Cfg.Penalties.Spread)
		}
	}

	return val
}
