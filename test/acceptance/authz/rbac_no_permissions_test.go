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
	"bytes"
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

func TestCollectEndpoints(t *testing.T) {
	col := NewCollector()
	col.CollectEndpoints()
	col.PrettyPrint(col.ReadEndpoints())
}

func TestAuthzAllEndpointsNoPermissionDynamically(t *testing.T) {
	adminKey := "admin-Key"
	customKey := "custom-key"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.New().WithWeaviate().WithRBAC().
		WithRbacUser("admin-User", adminKey, "admin-User").
		WithRbacUser("custom-user", customKey, "custom").
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	clientAuth := helper.CreateAuth(adminKey)
	className := "ABC"
	helper.CreateClassWithAuthz(t, &models.Class{Class: className}, clientAuth)

	col := NewCollector()
	col.CollectEndpoints()
	endpoints := col.AllEndpoints()
	foundPaths := []string{}
	// TODO: verify
	expectedEndPoint := []string{
		// needs to handle with AuthZ missing
		"/",
		"/.well-known/live",
		"/.well-known/openid-configuration",
		"/.well-known/ready",
		"/authz/roles",
		"/authz/roles/add-permissions",
		"/authz/roles/remove-permissions",

		// verify
		"/batch/objects",
		"/batch/objects",
		"/batch/references",
		"/graphql/batch",
		"/meta",
		"/objects/{className}/{id}/references/{propertyName}",
		"/objects/{className}/{id}/references/{propertyName}",
		"/objects/{id}",
		"/objects/{id}/references/{propertyName}",
		"/objects/{id}/references/{propertyName}",
		"/schema/{className}/tenants",
		"/schema/{className}/tenants",
		"/schema/{className}/tenants",
	}
	for _, endpoint := range endpoints {
		url := fmt.Sprintf("http://%s/v1%s", compose.GetWeaviate().URI(), endpoint.Path)
		url = strings.ReplaceAll(url, "/objects/{className}/{id}", fmt.Sprintf("/objects/%s/%s", className, UUID1.String()))
		url = strings.ReplaceAll(url, "/objects/{id}", fmt.Sprintf("/objects/%s", UUID1.String()))
		url = strings.ReplaceAll(url, "{className}", className)
		url = strings.ReplaceAll(url, "{tenantName}", "Tenant1")
		url = strings.ReplaceAll(url, "{shardName}", "Shard1")
		url = strings.ReplaceAll(url, "{id}", "someId")
		url = strings.ReplaceAll(url, "{backend}", "filesystem")
		url = strings.ReplaceAll(url, "{propertyName}", "someProperty")

		t.Run(url+"("+strings.ToUpper(endpoint.Method)+")", func(t *testing.T) {
			require.NotContains(t, url, "{")
			require.NotContains(t, url, "}")

			var req *http.Request
			var err error

			endpoint.Method = strings.ToUpper(endpoint.Method)

			if endpoint.Method == "POST" || endpoint.Method == "PUT" || endpoint.Method == "PATCH" {
				t.Log(string(endpoint.RequestBodyData))
				req, err = http.NewRequest(endpoint.Method, url, bytes.NewBuffer(endpoint.RequestBodyData))
				require.Nil(t, err)
				req.Header.Set("Content-Type", "application/json")
			} else {
				req, err = http.NewRequest(endpoint.Method, url, nil)
				require.Nil(t, err)
			}

			req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", customKey))
			client := &http.Client{}
			resp, err := client.Do(req)
			require.Nil(t, err)
			defer resp.Body.Close()

			if http.StatusForbidden != resp.StatusCode {
				foundPaths = append(foundPaths, endpoint.Path)
			}

			if slices.Contains(expectedEndPoint, endpoint.Path) {
				return
			}
			require.Equal(t, http.StatusForbidden, resp.StatusCode)
		})
	}

	for _, path := range foundPaths {
		t.Log(path)
	}
}
