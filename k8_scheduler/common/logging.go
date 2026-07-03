package common

import (
	"encoding/json"
	"log/slog"

	gograph "github.com/dominikbraun/graph"
)

func LogSolveContext(pods []*Node) {
	for _, pod := range pods {
		var selector map[string]string
		json.Unmarshal([]byte(pod.Properties["nodeSelector"]), &selector)
		args := []any{"pod", pod.Name}
		for k, v := range selector {
			args = append(args, k, v)
		}
		slog.Debug("pod selector", args...)
	}
}

func LogFinalAssignments(g gograph.Graph[string, *Node]) {
	edges, _ := g.Edges()
	for _, edge := range edges {
		if edge.Properties.Attributes["type"] != "assign" {
			continue
		}
		pod, _ := g.Vertex(edge.Source)
		node, _ := g.Vertex(edge.Target)

		labelMatch := true
		nodeSelector := pod.Properties["nodeSelector"]
		if nodeSelector != "" && nodeSelector != "{}" {
			var selectorMap, labelsMap map[string]string
			json.Unmarshal([]byte(nodeSelector), &selectorMap)
			json.Unmarshal([]byte(node.Properties["labels"]), &labelsMap)
			for k, v := range selectorMap {
				if labelsMap[k] != v {
					labelMatch = false
					break
				}
			}
		}
		slog.Debug("assignment", "pod", edge.Source, "node", edge.Target, "label_match", labelMatch)
	}
}
