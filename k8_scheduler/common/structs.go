// Package common provides code which can be used in every module of the app
package common

import k8 "k8s.io/api/core/v1"

type Node struct {
	Name       string
	Type       string
	Properties map[string]string
}

// Wire format is network_prober.sh's send_json, e.g.
// {"from":"w1","to":"w2","latency":12,"throughput":0.7,"timestamp":1735000000} -
// field names don't match Source/Target, so without these tags encoding/json
// silently leaves them "" (Latency/Throughput/Timestamp still matched
// case-insensitively by luck).
type Link struct {
	Source     string  `json:"from"`
	Target     string  `json:"to"`
	Latency    int     `json:"latency"`
	Throughput float64 `json:"throughput"`
	Timestamp  int     `json:"timestamp"`
}

type NetworkComRequirement struct {
	Target     string
	Latency    int
	Throughput float64
}

type PendingAssignment struct {
	Timestamp  int
	Assignment Assignment
}
type Assignment struct {
	Source string
	Target string
}

type K8Knowledge struct {
	Pods  []k8.Pod
	Nodes []k8.Node
}
