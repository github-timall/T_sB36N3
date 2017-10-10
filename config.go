package main

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"time"
)

type (
	Config struct {
		ServiceName			string 				`yaml:"service_name"`
		DsnEvent    		string 				`yaml:"dsn_event"`
		DsnVein     		string				`yaml:"dsn_vein"`
		HttpClientSettings 	HttpClientSettings	`yaml:"http_client_settings"`
		WorkerSettings		WorkerSettings		`yaml:"worker_settings"`
		Services 			[]Service 			`yaml:"services"`
	}

	Service struct {
		Name     	string 		`yaml:"name"`
		Url  		string 		`yaml:"url"`
		AccessToken string 		`yaml:"access_token"`
		Actions 	[]Action	`yaml:"actions"`
	}

	Action struct {
		Url  		string `yaml:"url"`
		EntityType 	string `yaml:"entity_type"`
		EventType 	string `yaml:"event_type"`
	}

	HttpClientSettings struct {
		Timeout time.Duration `yaml:"timeout"`
	}

	WorkerSettings struct {
		TransfersRun bool `yaml:"transfers_run"`
	}
)

func LoadConfig() (config *Config) {
	source, err := ioutil.ReadFile("config.yml")
	if err != nil {
		panic(err)
	}

	err = yaml.Unmarshal(source, &config)
	if err != nil {
		panic(err)
	}
	return
}
