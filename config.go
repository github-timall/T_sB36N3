package main

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type Db struct {
	Dsn		 	string `yaml:"dsn"`
	EventStore 	string `yaml:"event_store"`
}

type Service struct {
	Name     	string `yaml:"name"`
	Url  		string `yaml:"url"`
	AccessToken string `yaml:"access_token"`
}

type Config struct {
	Db       	Db
	Service		string `yaml:"service"`
	Services 	[]Service `yaml:"services"`
}

func LoadConfig() (config *Config, err error) {
	source, err := ioutil.ReadFile("config.yml")
	if err != nil {
		return
	}

	err = yaml.Unmarshal(source, &config)
	if err != nil {
		return
	}
	return
}
