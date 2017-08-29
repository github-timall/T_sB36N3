package config

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type Db struct {
	Dsn		 string `yaml:"dsn"`
	Table 	 string `yaml:"table"`
	LogTable string `yaml:"dus_log"`
}

type Service struct {
	Service     string `yaml:"service"`
	AccessToken string `yaml:"access_token"`
	UrlWebHook  string `yaml:"url_web_hook"`
}

type Settings struct {
	Db       Db
	Services []Service `yaml:"services"`
}

func Load() (settings *Settings, err error) {
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
