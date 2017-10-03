package main

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type Service struct {
	Name     	string `yaml:"name"`
	Url  		string `yaml:"url"`
	AccessToken string `yaml:"access_token"`
}

type Config struct {
	Service		string 		`yaml:"service"`
	DsnEvent    string 		`yaml:"dsn_event"`
	DsnVein     string		`yaml:"dsn_vein"`
	Services 	[]Service 	`yaml:"services"`
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
