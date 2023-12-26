package main

import (
	polygonRest "github.com/polygon-io/client-go/rest"
	"github.com/sirupsen/logrus"
)

func LoggerInit() {
	//初始化logger
	logger = logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	logger.SetFormatter(&logrus.JSONFormatter{})

	restClient = polygonRest.NewWithClient(PolygonApiKey, &hc)
}
