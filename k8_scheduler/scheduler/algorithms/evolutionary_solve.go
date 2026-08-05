package algorithms

import (
	"encoding/json"
	"k8_scheduler/common"
	networkgraph "k8_scheduler/networkGraph"
	"k8_scheduler/scheduler/evaluator"
	"log/slog"
	"math"
	"math/rand"
	"sort"

	gograph "github.com/dominikbraun/graph"
)


type Solution struct {
	graph gograph.Graph[string, *common.Node]
	value evaluator.ScoreBreakdown
}

// lastLoggedScore is the previous call's final score, used to decide
// whether a pending topology change (see networkgraph.PendingTopologyChange)
// is worth logging - only when it actually moved the score, not on every
// tick's RF jitter.
var lastLoggedScore = math.NaN()

// candidatesForPod returns the nodes matching pod's nodeSelector, and whether
// the pod has no nodeSelector at all (i.e. placement is unrestricted).
func candidatesForPod(nodes []*common.Node, pod *common.Node) ([]*common.Node, bool) {
	// A pod pinned to a specific node (e.g. by the DaemonSet controller's
	// per-pod node affinity) can only ever go on that one node.
	if required := pod.Properties["requiredNode"]; required != "" {
		for _, node := range nodes {
			if node.Name == required {
				return []*common.Node{node}, false
			}
		}
		return nil, false
	}

	nodeSelector := make(map[string]string)
	if pod.Properties["nodeSelector"] != "" {
		_ = json.Unmarshal([]byte(pod.Properties["nodeSelector"]), &nodeSelector)
	}

	var candidates []*common.Node
	for _, node := range nodes {
		if node.Type != "node" || node.Properties["labels"] == "" {
			continue
		}

		var nodeLabels map[string]string
		_ = json.Unmarshal([]byte(node.Properties["labels"]), &nodeLabels)

		match := true
		for k, v := range nodeSelector {
			if nodeLabels[k] != v {
				match = false
				break
			}
		}

		if match {
			candidates = append(candidates, node)
		}
	}

	return candidates, len(nodeSelector) == 0
}

func EvolutionarySolve(
	baseGraph gograph.Graph[string, *common.Node],
	pods []*common.Node,
	debug bool,
	visualize bool,
) gograph.Graph[string, *common.Node] {
	cfg := common.Cfg.Scheduler.Evolutionary
	generations := cfg.Generations
	childrenPerParent := cfg.ChildrenPerParent
	survivorsPerGen := cfg.SurvivorsPerGeneration

	common.LogSolveContext(pods)

	// ---------- Initial population (diverse) ----------

	survivors := make([]Solution, 0, survivorsPerGen)
	currentBest := Solution{nil, evaluator.ScoreBreakdown{Resources: math.MaxFloat64}}
	sort.SliceStable(pods, func(i, j int) bool {
		return pods[i].Properties["nodeSelector"] != ""
	})
	for i := 0; i < survivorsPerGen; i++ {

		g, _ := baseGraph.Clone()
		var nodes []*common.Node
		adjacencyMap, _ := g.AdjacencyMap()
		for vertexID := range adjacencyMap {
			vertex, _ := g.Vertex(vertexID)
			if vertex.Type == "node" {
				nodes = append(nodes, vertex)
			}
		}
		for _, pod := range pods {
			candidates, unrestricted := candidatesForPod(nodes, pod)

			// Assign pod to a random candidate node
			if len(candidates) > 0 {
				g = Random(g, pod, false, false, candidates...)
			} else if unrestricted {
				g = Random(g, pod, false, false) // no nodeSelector, no restriction
			} else {
				// nodeSelector is a hard filter: no node satisfies it, leave the pod unplaced
				g.AddVertex(pod, common.VertexAttributes("pending_pod")...)
			}
		}

		val := evaluator.EvaluateStep(baseGraph, g, false)
		sol := Solution{g, val}

		survivors = append(survivors, sol)

		if currentBest.graph == nil || val.Total() < currentBest.value.Total() {
			currentBest = sol
		}
	}

	if debug {
		println("Initial best:", currentBest.value.Total())
	}

	// ---------- Evolution loop ----------

	for gen := 0; gen < generations; gen++ {

		if debug {
			println("Generation:", gen)
		}

		if currentBest.value.Total() < 0.1 {
			println("Final best evaluation:", currentBest.value.Total())
			return currentBest.graph
		}

		children := make([]Solution, 0, survivorsPerGen*childrenPerParent)

		// ----- Produce children -----

		for _, parent := range survivors {
			for c := 0; c < childrenPerParent; c++ {

				child, _ := parent.graph.Clone()

				// Collect pods currently placed
				adjacency, _ := child.AdjacencyMap()

				var placedPods []*common.Node
				for v := range adjacency {
					node, _ := child.Vertex(v)
					if node.Type == "pod" && node.Properties["schedulerName"] == "custom-scheduler" {
						placedPods = append(placedPods, node)
					}
				}

				if len(placedPods) == 0 {
					continue
				}

				chosen := placedPods[rand.Intn(len(placedPods))]

				// Remove pod
				edges, _ := child.Edges()
				for _, e := range edges {
					if e.Source == chosen.Name || e.Target == chosen.Name {
						child.RemoveEdge(e.Source, e.Target)
					}
				}
				child.RemoveVertex(chosen.Name)

				// Reassign, respecting chosen's nodeSelector if it has one
				var nodes []*common.Node
				for v := range adjacency {
					vertex, _ := child.Vertex(v)
					if vertex != nil && vertex.Type == "node" {
						nodes = append(nodes, vertex)
					}
				}
				candidates, unrestricted := candidatesForPod(nodes, chosen)
				if len(candidates) > 0 {
					child = Random(child, chosen, false, false, candidates...)
				} else if unrestricted {
					child = Random(child, chosen, false, false)
				} else {
					child.AddVertex(chosen, common.VertexAttributes("pending_pod")...)
				}

				val := evaluator.EvaluateStep(baseGraph, child, false)

				if val.Total() < currentBest.value.Total() {
					currentBest = Solution{child, val}
				}

				if val.Total() < 0.1 {
					if debug {
						println("Perfect solution found")
					}
					return child
				}

				children = append(children, Solution{child, val})
			}
		}

		if len(children) == 0 {
			break
		}

		// ----- Select best children -----

		sort.Slice(children, func(i, j int) bool {
			return children[i].value.Total() < children[j].value.Total()
		})

		if len(children) < survivorsPerGen {
			survivors = children
		} else {
			survivors = children[:survivorsPerGen]
		}

		if debug {
			println("Best this gen:", survivors[0].value.Total())
		}
	}

	finalScore := currentBest.value.Total()
	if count, top, ok := networkgraph.PendingTopologyChange(); ok {
		if math.IsNaN(lastLoggedScore) || math.Abs(finalScore-lastLoggedScore) > 1e-9 {
			slog.Info("topology changed", "count", count, "top", top)
		}
	}
	lastLoggedScore = finalScore

	slog.Info("evolutionary solve complete",
		"score", finalScore,
		"score_resources", currentBest.value.Resources,
		"score_unconnected", currentBest.value.Unconnected,
		"score_latency", currentBest.value.Latency,
		"score_throughput", currentBest.value.Throughput,
		"score_labels", currentBest.value.Labels,
		"score_stability", currentBest.value.Stability,
		"score_spread", currentBest.value.Spread,
		"score_move_pod", currentBest.value.MovePod,
	)
	if currentBest.graph == nil {
		slog.Warn("no solution produced, returning base graph unchanged")
		return baseGraph
	}
	common.LogFinalAssignments(currentBest.graph)

	return currentBest.graph
}
