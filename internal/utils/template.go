package utils

import (
	"fmt"
	"os"
	"text/template"
)

func CreateTemplate(templateName, text string) (*template.Template, error) {
	tmpl, err := template.New(templateName).Parse(text)
	if err != nil {
		return nil, fmt.Errorf("parse sample config template err: %+v", err)
	}
	return tmpl, nil
}

func RenderTemplate(tmpl *template.Template, maps map[string]string) (*os.File, error) {
	tmpf, err := os.CreateTemp("", "tmp")
	if err != nil {
		return nil, fmt.Errorf("create tmp config file err: %+v", err)
	}
	err = tmpl.Execute(tmpf, maps)
	if err != nil {
		return nil, fmt.Errorf("render sample config template err: %+v", err)
	}
	return tmpf, nil
}
