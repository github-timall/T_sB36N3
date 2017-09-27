package main

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type Db struct {
	Dsn		 	string `yaml:"dsn"`
	Service		string `yaml:"service"`
	EventStore 	string `yaml:"event_store"`
}

type Service struct {
	Name     	string `yaml:"name"`
	Url  		string `yaml:"url"`
	AccessToken string `yaml:"access_token"`
}

type Settings struct {
	Db       Db
	Services []Service `yaml:"services"`
}

func LoadConfig() (settings *Settings, err error) {
	source, err := ioutil.ReadFile("config.yml")
	if err != nil {
		return
	}

	err = yaml.Unmarshal(source, &settings)
	if err != nil {
		return
	}
	return
}
