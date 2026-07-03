package algorithms

import (
	"encoding/json"
	"k8_scheduler/common"
	"k8_scheduler/scheduler/evaluator"
	"log/slog"
	"math"
	"math/rand"
	"sort"

	gograph "github.com/dominikbraun/graph"
)


type Solution struct {
	graph gograph.Graph[string, *common.Node]
	value float64
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
	currentBest := Solution{nil, math.MaxFloat64}
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
			nodeSelector := make(map[string]string)
			if pod.Properties["nodeSelector"] != "" {
				_ = json.Unmarshal([]byte(pod.Properties["nodeSelector"]), &nodeSelector)
			}

			// Find candidate nodes that match all labels
			var candidates []*common.Node

			for _, node := range nodes { // assuming nodes is a slice of *common.Node
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

			// Assign pod to a random candidate node (or pick first)
			if len(candidates) > 0 {
				g = Random(g, pod, false, false, candidates...) // modify Random to accept candidates
			} else {
				g = Random(g, pod, false, false) // fallback
			}
		}

		val := evaluator.EvaluateStep(baseGraph, g, false)
		sol := Solution{g, val}

		survivors = append(survivors, sol)

		if val < currentBest.value {
			currentBest = sol
		}
	}

	if debug {
		println("Initial best:", currentBest.value)
	}

	// ---------- Evolution loop ----------

	for gen := 0; gen < generations; gen++ {

		if debug {
			println("Generation:", gen)
		}

		if currentBest.value < 0.1 {
			println("Final best evaluation:", currentBest.value)
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
					if node.Type == "pod" {
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

				// Reassign randomly
				child = Random(child, chosen, false, false)

				val := evaluator.EvaluateStep(baseGraph, child, false)

				if val < currentBest.value {
					currentBest = Solution{child, val}
				}

				if val < 0.1 {
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
			return children[i].value < children[j].value
		})

		if len(children) < survivorsPerGen {
			survivors = children
		} else {
			survivors = children[:survivorsPerGen]
		}

		if debug {
			println("Best this gen:", survivors[0].value)
		}
	}

	slog.Info("evolutionary solve complete", "score", currentBest.value)
	common.LogFinalAssignments(currentBest.graph)

	return currentBest.graph
}
