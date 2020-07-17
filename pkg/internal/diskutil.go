package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
)

const (
	// StateSuspended is a possible value of BlockDevice.State
	StateSuspended = "suspended"
	// DiskByIDDir is the path for symlinks to the device by id.
	DiskByIDDir = "/dev/disk/by-id/"
)

// IDPathNotFoundError indicates that a symlink to the device was not found in /dev/disk/by-id/
type IDPathNotFoundError struct {
	DeviceName string
}

func (e IDPathNotFoundError) Error() string {
	return fmt.Sprintf("IDPathNotFoundError: a symlink to  %q was not found in %q", e.DeviceName, DiskByIDDir)
}

// BlockDevice is the a block device as output by lsblk.
// All the fields are lsblk columns.
type BlockDevice struct {
	Name   string `json:"name"`
	Type   string `json:"type"`
	Model  string `json:"mode,omitempty"`
	Vendor string `json:"vendor,omitempty"`
	State  string `json:"state,omitempty"`
	FSType string `json:"fstype"`
	Size   string `json:"size"`
	// Children   []BlockDevice `json:"children,omitempty"`
	Rotational string `json:"rota"`
	ReadOnly   string `json:"ro,omitempty"`
	Removable  string `json:"rm,omitempty"`
	pathByID   string
}

// IDPathNotFoundError indicates that a symlink to the device was not found in /dev/disk/by-id/

// GetRotational as bool
func (b BlockDevice) GetRotational() (bool, error) {
	v, err := parseBitBool(b.Rotational)
	if err != nil {
		err = errors.Wrapf(err, "failed to parse rotational property %q as bool", b.Rotational)
	}
	return v, err
}

// GetReadOnly as bool
func (b BlockDevice) GetReadOnly() (bool, error) {
	v, err := parseBitBool(b.ReadOnly)
	if err != nil {
		err = errors.Wrapf(err, "failed to parse readOnly property %q as bool", b.ReadOnly)
	}
	return v, err
}

// GetRemovable as bool
func (b BlockDevice) GetRemovable() (bool, error) {
	v, err := parseBitBool(b.Removable)
	if err != nil {
		err = errors.Wrapf(err, "failed to parse removable property %q as bool", b.Removable)
	}
	return v, err
}

func parseBitBool(s string) (bool, error) {
	if s == "0" || s == "" {
		return false, nil
	} else if s == "1" {
		return true, nil
	}
	return false, fmt.Errorf("lsblk bool value not 0 or 1: %q", s)
}

// GetSize as int64
func (b BlockDevice) GetSize() (int64, error) {
	v, err := strconv.ParseInt(b.Size, 10, 64)
	if err != nil {
		err = errors.Wrapf(err, "failed to parse size property %q as int64", b.Size)
	}
	return v, err
}

// HasChildren check on BlockDevice
func (b BlockDevice) HasChildren() (bool, error) {
	sysDevDir := filepath.Join("/sys/block/", b.Name, "/*")
	paths, err := filepath.Glob(sysDevDir)
	if err != nil {
		return false, errors.Wrapf(err, "could not glob path: %q to verify partiions", sysDevDir)
	}
	for _, path := range paths {
		name := filepath.Base(path)
		if strings.HasPrefix(name, b.Name) {
			return true, nil
		}
	}
	return false, nil
}

// GetPathByID check on BlockDevice
func (b BlockDevice) GetPathByID() (string, error) {

	// return if previously populated value is valid
	if len(b.pathByID) > 0 && strings.HasPrefix(b.pathByID, DiskByIDDir) {
		evalsCorrectly, err := PathEvalsToDiskLabel(b.pathByID, b.Name)
		if err == nil && evalsCorrectly {
			return b.pathByID, nil
		}
	}
	b.pathByID = ""
	diskByIDDir := filepath.Join(DiskByIDDir, "/*")
	paths, err := filepath.Glob(diskByIDDir)
	if err != nil {
		return "", fmt.Errorf("could not list files in %q: %w", DiskByIDDir, err)
	}
	for _, path := range paths {
		isMatch, err := PathEvalsToDiskLabel(path, b.Name)
		if err != nil {
			return "", err
		}
		if isMatch {
			b.pathByID = path
			return path, nil
		}
	}
	// return path by label and error
	return fmt.Sprintf("/dev/%s", b.Name), IDPathNotFoundError{DeviceName: b.Name}
}

// PathEvalsToDiskLabel checks if the path is a symplink to a file devName
func PathEvalsToDiskLabel(path, devName string) (bool, error) {
	devPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("could not eval symLink %q:%w", devPath, err)
	}
	if filepath.Base(devPath) == devName {
		return true, nil
	}
	return false, nil
}

// ListBlockDevices using the lsblk command
func ListBlockDevices() ([]BlockDevice, []string, error) {
	var output bytes.Buffer
	var blockDevices []BlockDevice

	columns := "NAME,ROTA,TYPE,SIZE,MODEL,VENDOR,RO,RM,STATE,FSTYPE"
	args := []string{"--pairs", "-b", "-o", columns}
	cmd := exec.Command("lsblk", args...)
	cmd.Stdout = &output
	err := cmd.Run()
	if err != nil {
		return []BlockDevice{}, []string{}, err
	}

	badRows := make([]string, 0)
	// convert to json and then Marshal.
	outputMapList := make([]map[string]interface{}, 0)
	rowList := strings.Split(output.String(), "\n")
RowLoop:
	for _, row := range rowList {
		if len(strings.Trim(row, " ")) == 0 {
			break RowLoop
		}
		outputMap := make(map[string]interface{})
		// split by `" ` to avoid splitting on spaces in MODEL,VENDOR
		keyValues := strings.Split(row, `" `)
		for _, keyValue := range keyValues {
			keyValueList := strings.Split(keyValue, "=")
			if len(keyValueList) != 2 {
				continue
			}
			key := strings.ToLower(keyValueList[0])
			value := strings.Replace(keyValueList[1], `"`, "", -1)
			outputMap[key] = value
		}

		// only use device if name is populated, and non-empty
		v, found := outputMap["name"]
		if !found {
			badRows = append(badRows, row)
			break RowLoop
		}
		name := v.(string)
		if len(strings.Trim(name, " ")) == 0 {
			badRows = append(badRows, row)
			break RowLoop
		}
		outputMapList = append(outputMapList, outputMap)
	}

	if len(badRows) == len(rowList) {
		return []BlockDevice{}, badRows, fmt.Errorf("could not parse any of the lsblk rows")
	}

	jsonBytes, err := json.Marshal(outputMapList)
	if err != nil {
		return []BlockDevice{}, badRows, err
	}

	err = json.Unmarshal(jsonBytes, &blockDevices)
	if err != nil {
		return []BlockDevice{}, badRows, err
	}

	return blockDevices, badRows, nil
}

// PVCreationLock checks whether a PV can be created based on this device
// and Locks the device so that no PVs can be created on it while the lock is held.
// the PV lock will fail if:
// - another process holds an exclusive file lock on the device (using the syscall flock)
// - a symlink to this device exists in symlinkDirs
// returns:
// ExclusiveFileLock, must be unlocked
// bool determines if flock was placed on device.
// error
func PVCreationLock(device string, symlinkDirs ...string) (ExclusiveFileLock, bool, error) {
	lock := ExclusiveFileLock{Path: device}
	locked, err := lock.Lock()
	if err != nil || !locked {
		return lock, false, err
	}

	for _, dir := range symlinkDirs {
		links, err := GetMatchingSymlinksInDir(dir, device)
		// return false if symlinks exist
		if err != nil || len(links) > 0 {
			return lock, false, err
		}
	}
	return lock, true, nil
}

// GetMatchingSymlinksInDir returns all the files in dir that are the same file as path after evaluating symlinks
// it works using `find -L dir -samefile path`
func GetMatchingSymlinksInDir(dir, path string) ([]string, error) {
	cmd := exec.Command("find", "-L", dir, "-samefile", path)
	output, err := executeCmd(cmd)
	if err != nil {
		return []string{}, fmt.Errorf("failed to get symlinks in directory %q for device ID %q. %v", dir, path, err)
	}
	return strings.Split(strings.TrimSuffix(output, "\n"), "\n"), nil
}

func executeCmd(cmd *exec.Cmd) (string, error) {
	var out bytes.Buffer
	var err error
	cmd.Stdout = &out
	err = cmd.Run()
	if err != nil {
		return "", err
	}

	return out.String(), nil
}

type ExclusiveFileLock struct {
	Path   string
	locked bool
	fd     int
}

// Lock locks the file so other process cannot open the file
func (e *ExclusiveFileLock) Lock() (bool, error) {
	fd, errno := unix.Open(e.Path, unix.O_RDONLY|unix.O_EXCL, 0)
	e.fd = fd

	if errno == unix.EBUSY {
		// device is in use
		return false, nil
	} else if errno != nil {
		return false, errno
	}

	errno = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB)
	if errno == unix.EWOULDBLOCK {
		e.locked = false
	} else if errno == nil {
		e.locked = true
	} else if errno != nil {

	}
	return e.locked, nil
}

// Unlock releases the lock
func (e *ExclusiveFileLock) Unlock() error {
	if e.locked {
		return unix.Close(e.fd)
	}
	return nil

}
