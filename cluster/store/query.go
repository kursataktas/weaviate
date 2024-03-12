package store

import (
	"encoding/json"
	"fmt"

	cmd "github.com/weaviate/weaviate/cluster/proto/cluster"
)

func (st *Store) Query(req *cmd.QueryRequest) (*cmd.QueryResponse, error) {
	st.log.Debug("server.query", "type", req.Type)

	var payload []byte
	var err error
	switch req.Type {
	case cmd.QueryRequest_TYPE_GET_READONLY_CLASS:
		payload, err = st.GetReadOnlyClass(req)
		if err != nil {
			return nil, fmt.Errorf("could not get read only class: %w", err)
		}
	default:
		// This could occur when a new command has been introduced in a later app version
		// At this point, we need to panic so that the app undergo an upgrade during restart
		const msg = "consider upgrading to newer version"
		st.log.Error("unknown command", "type", req.Type, "more", msg)
		panic(fmt.Sprintf("unknown command type=%d more=%s", req.Type, msg))
	}
	return &cmd.QueryResponse{Payload: payload}, nil
}

func (st *Store) GetReadOnlyClass(req *cmd.QueryRequest) ([]byte, error) {
	var payload []byte

	// Validate that the subcommand is the correct type
	subCommand := cmd.QueryReadOnlyClassRequest{}
	if err := json.Unmarshal(req.SubCommand, &subCommand); err != nil {
		return payload, fmt.Errorf("%w: %w", errBadRequest, err)
	}

	// Read the meta class to get both the class and sharding information
	metaClass, err := st.db.Schema.ReadMetaClass(subCommand.Class)
	if err != nil {
		return payload, nil
	}

	// Build the response, marshal and return
	response := cmd.QueryReadOnlyClassResponse{Class: &metaClass.Class, State: &metaClass.Sharding}
	payload, err = json.Marshal(response)
	if err != nil {
		return []byte{}, fmt.Errorf("could not marshal query response: %w", err)
	}
	return payload, nil
}
