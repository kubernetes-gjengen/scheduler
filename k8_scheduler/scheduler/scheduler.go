// Package scheduler bundles all function the scheduler provides
package scheduler

import (
	"k8_scheduler/common"
	"k8_scheduler/scheduler/algorithms"

	gograph "github.com/dominikbraun/graph"
	corev1 "k8s.io/api/core/v1"
)

/*
SchedulePods can schedule a set of Pods in a Graph. It will terminate early if no pods are provided. If you are looking to optimize a given graph, use the optimize Function instead.
*/
func SchedulePods(graph gograph.Graph[string, *common.Node], pods []corev1.Pod, debug bool, visualize bool) gograph.Graph[string, *common.Node] {
	graphCopy, _ := graph.Clone()
	if len(pods) == 0 {
		return graph
	}
	var vertices []*common.Node = []*common.Node{}
	for _, pod := range pods {
		vertices = append(vertices, common.PodToVertex(pod))
	}

	return algorithms.EvolutionarySolve(graphCopy, vertices, debug, visualize)
}

/*
This function is almost the same as the SchedulePods function. They could be merged, but are kept separate for better readability
*/
func Optimize(graph gograph.Graph[string, *common.Node], debug bool, visualize bool) gograph.Graph[string, *common.Node] {
	graphCopy, _ := graph.Clone()
	var vertices []*common.Node = []*common.Node{}
	return algorithms.EvolutionarySolve(graphCopy, vertices, debug, visualize)
}
