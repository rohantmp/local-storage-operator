// SPDX-License-Identifier: 0BSD

package errors

import "fmt"

// LsmError returned from JSON API
type LsmError struct {
	Code    int32  `json:"code"`
	Message string `json:"message"`
	Data    string `json:"data"`
}

func (e *LsmError) Error() string {
	if len(e.Data) > 0 {
		return fmt.Sprintf("code = %d, message = %s, data = %s", e.Code, e.Message, e.Data)
	}
	return fmt.Sprintf("code = %d, message = %s", e.Code, e.Message)
}

const (
	// LibBug ... Library bug
	LibBug int32 = 1

	// PluginBug ... Bug found in plugin
	PluginBug int32 = 2

	// JobStarted ... Job has been started
	JobStarted int32 = 7

	// TimeOut ... Plugin timeout
	TimeOut int32 = 11

	// DameonNotRunning ... lsmd does not appear to be running
	DameonNotRunning int32 = 12

	// PermissionDenied Insufficient permission
	PermissionDenied int32 = 13

	// InvalidArgument ... provided argument is incorrect
	InvalidArgument int32 = 101

	// NoSupport operation not supported
	NoSupport int32 = 153

	// NotFoundFs specfified file system not found
	NotFoundFs int32 = 201

	// PluginNotExist ... Plugin doesn't apprear to exist
	PluginNotExist int32 = 311

	//TransPortComunication ... Issue reading/writing to plugin
	TransPortComunication int32 = 400

	// TransPortInvalidArg parameter transported over IPC is invalid
	TransPortInvalidArg int32 = 402
)
