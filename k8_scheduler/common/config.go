package common

import (
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Penalties struct {
		MovePod        int `yaml:"move_pod"`
		UnconnectedPod int `yaml:"unconnected_pod"`
		Label          int `yaml:"label"`
		Latency        int `yaml:"latency"`
		Throughput     int `yaml:"throughput"`
		Stability      int `yaml:"stability"`
		Spread         int `yaml:"spread"`
		CPU            int `yaml:"cpu"`
		Memory         int `yaml:"memory"`
	} `yaml:"penalties"`
	Stability struct {
		FloatingAverageWindow int `yaml:"floating_average_window"`
		LinkTimeout           int `yaml:"link_timeout"`
	} `yaml:"stability"`
	Scheduler struct {
		Evolutionary struct {
			Generations            int `yaml:"generations"`
			ChildrenPerParent      int `yaml:"children_per_parent"`
			SurvivorsPerGeneration int `yaml:"survivors_per_generation"`
		} `yaml:"evolutionary"`
	} `yaml:"scheduler"`
	MQTT struct {
		Host  string `yaml:"host"`
		Topic string `yaml:"topic"`
	}
}

var Cfg Config

func LoadConfig(filename string) {
	file, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("Error reading config file: %v", err)
	}

	err = yaml.Unmarshal(file, &Cfg)
	if err != nil {
		log.Fatalf("Error parsing config file: %v", err)
	}
}
