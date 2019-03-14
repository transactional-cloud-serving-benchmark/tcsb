package simulation

import (
	"fmt"
	"io"
)

func GetScenarioByName(x string) (Scenario, error) {
	switch x {
	case "keyvalue":
		return &KeyValueScenario{}, nil
	default:
		return nil, fmt.Errorf("unknown scenario '%s'", x)
	}
}

// NOTE(rw): We don't need to worry about heap allocations here because it is
//           not on a fast path (this whole program execution happens offline).
type Scenario interface {
	SetDatabaseKindByName(string) error
	SetCommandModeByName(string) error
	SetEmitModeByName(string) error
	SetParamsFromString(string) error

	NewEmitter(io.Writer) Emitter
}

// Emitter is a function that repeatedly writes data for serialization.
//
// When the Emitter returns nil, call it again to serialize the next payload.
// When the Emitter returns io.EOF, the sequence is done.
// When the Emitter returns any other error, the function failed.
type Emitter func() error
