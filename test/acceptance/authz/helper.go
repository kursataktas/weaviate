//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"testing"
	"text/tabwriter"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/spec"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
	"golang.org/x/exp/rand"
)

type Endpoint struct {
	Path            string
	Method          string
	Summary         string
	RequestBodyData []byte
}

type collector struct {
	endpoints       []Endpoint
	methodEndpoints map[string][]Endpoint
}

func NewCollector() *collector {
	return &collector{
		methodEndpoints: make(map[string][]Endpoint),
		endpoints:       make([]Endpoint, 0),
	}
}

func (c *collector) CollectEndpoints() ([]Endpoint, error) {
	file, err := os.Open("../../../openapi-specs/schema.json")
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %v", err)
	}

	var specDoc spec.Swagger
	err = json.Unmarshal(data, &specDoc)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %v", err)
	}

	for path, pathItem := range specDoc.Paths.Paths {
		methods := map[string]*spec.Operation{
			"GET":     pathItem.Get,
			"POST":    pathItem.Post,
			"PUT":     pathItem.Put,
			"DELETE":  pathItem.Delete,
			"PATCH":   pathItem.Patch,
			"HEAD":    pathItem.Head,
			"OPTIONS": pathItem.Options,
		}

		for method, operation := range methods {
			if operation == nil {
				continue
			}

			var requestBodyData []byte
			for _, param := range operation.Parameters {
				if param.In == "body" && param.Schema != nil {
					requestBodyData, err = generateValidRequestBody(param.Schema, specDoc.Definitions)
					if err != nil {
						return nil, fmt.Errorf("failed to generate request body data: %v", err)
					}
				}
			}
			c.endpoints = append(c.endpoints, Endpoint{
				Path:            path,
				Method:          method,
				Summary:         operation.Summary,
				RequestBodyData: requestBodyData,
			})
		}
	}

	sort.Slice(c.endpoints, func(i, j int) bool {
		if c.endpoints[i].Path == c.endpoints[j].Path {
			return c.endpoints[i].Method < c.endpoints[j].Method
		}
		return c.endpoints[i].Path < c.endpoints[j].Path
	})
	return c.endpoints, nil
}

func (c *collector) PrettyPrint(endpoints ...map[string][]Endpoint) {
	if len(endpoints) == 0 {
		print(c.methodEndpoints)
		return
	}

	for _, endpointsMap := range endpoints {
		print(endpointsMap)
	}
}

func print(endpointsByMethod map[string][]Endpoint) {
	writer := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.Debug)
	for method, endpoints := range endpointsByMethod {
		fmt.Fprintf(writer, "\n%s Requests:\n", strings.ToUpper(method))
		fmt.Fprintln(writer, "Path\tMethod\tSummary")
		for _, endpoint := range endpoints {
			fmt.Fprintf(writer, "%s\t%s\t%s\n", endpoint.Path, endpoint.Method, endpoint.Summary)
		}
	}
	writer.Flush()
}

func (c *collector) AllEndpoints() []Endpoint {
	return c.endpoints
}

func (c *collector) GetEndpoints() []Endpoint {
	res := []Endpoint{}
	for _, endpoint := range c.endpoints {
		if endpoint.Method == "get" {
			res = append(res, endpoint)
		}
	}
	return res
}

func (c *collector) ReadEndpoints() map[string][]Endpoint {
	res := make(map[string][]Endpoint)
	for _, endpoint := range c.endpoints {
		if endpoint.Method == "get" || endpoint.Method == "head" {
			res[endpoint.Method] = append(res[endpoint.Method], endpoint)
		}
	}
	return res
}

func generateValidRequestBody(schema *spec.Schema, definitions map[string]spec.Schema) ([]byte, error) {
	body := make(map[string]interface{})

	// Generate data for each property in the object
	for propName, propSchema := range schema.Properties {
		body[propName] = generateValidData(&propSchema, definitions)
	}

	return json.Marshal(body)
}

func generateValidData(schema *spec.Schema, definitions map[string]spec.Schema) interface{} {
	// Resolve reference if necessary
	if schema.Ref.String() != "" {
		refSchema, err := resolveReference(schema.Ref.String(), definitions)
		if err != nil {
			log.Printf("Failed to resolve reference: %v", err)
			return nil
		}
		return generateValidData(refSchema, definitions)
	}

	switch schema.Type[0] {
	case "string":
		return "example"
	case "integer":
		return rand.Intn(100)
	case "boolean":
		return rand.Intn(2) == 0
	case "array":
		if schema.Items != nil {
			var array []interface{}
			for i := 0; i < 3; i++ {
				array = append(array, generateValidData(schema.Items.Schema, definitions))
			}
			return array
		}
	case "object":
		obj := make(map[string]interface{})
		for propName, propSchema := range schema.Properties {
			obj[propName] = generateValidData(&propSchema, definitions)
		}
		return obj
	}
	return nil
}

// resolveReference resolves a reference to a schema definition in the Swagger file
func resolveReference(ref string, definitions map[string]spec.Schema) (*spec.Schema, error) {
	ref = strings.TrimPrefix(ref, "#/definitions/")
	if schema, ok := definitions[ref]; ok {
		return &schema, nil
	}
	return nil, fmt.Errorf("reference %s not found", ref)
}

func deleteObjectClass(t *testing.T, class string, auth runtime.ClientAuthInfoWriter) {
	delParams := clschema.NewSchemaObjectsDeleteParams().WithClassName(class)
	delRes, err := helper.Client(t).Schema.SchemaObjectsDelete(delParams, auth)
	helper.AssertRequestOk(t, delRes, err, nil)
}

func createClass(t *testing.T, class *models.Class, auth runtime.ClientAuthInfoWriter) error {
	params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(class)
	_, err := helper.Client(t).Schema.SchemaObjectsCreate(params, auth)
	return err
}
