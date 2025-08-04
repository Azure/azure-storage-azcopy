package e2etest

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type AzCopyEnvironmentManagerKey struct{}
type AzCopyEnvironmentKey struct{}
type AzCopyRunNumKey struct{}

func FetchAzCopyEnvironmentContext(cm ScenarioAsserter) *AzCopyEnvironmentContext {
	pCtx := cm.Context()
	out := pCtx.Value(AzCopyEnvironmentManagerKey{})

	if out == nil {
		envCtx := &AzCopyEnvironmentContext{
			ScenarioID:   uuid.New(),
			Environments: make([]*AzCopyEnvironment, 0),

			mu:          &sync.Mutex{},
			cleanupOnce: &sync.Once{},

			parent: pCtx,
		}

		cm.SetContext(envCtx)
		return envCtx
	}

	return out.(*AzCopyEnvironmentContext)
}

type LogUpload struct {
	EnvironmentID uint
	RunID         uint

	Stdout string
	Stderr string
}

type AzCopyEnvironmentContext struct {
	ScenarioID   uuid.UUID
	Environments []*AzCopyEnvironment
	LogUploads   []LogUpload

	// let's just code defensively and assume there will be a test case that will run azcopy instances in parallel
	mu          *sync.Mutex
	cleanupOnce *sync.Once

	parent context.Context
}

func (envCtx *AzCopyEnvironmentContext) Deadline() (deadline time.Time, ok bool) {
	return envCtx.parent.Deadline()
}

func (envCtx *AzCopyEnvironmentContext) Done() <-chan struct{} {
	return envCtx.parent.Done()
}

func (envCtx *AzCopyEnvironmentContext) Err() error {
	return envCtx.parent.Err()
}

func (envCtx *AzCopyEnvironmentContext) Value(key any) any {
	if key == (AzCopyEnvironmentManagerKey{}) {
		return envCtx
	}

	return envCtx.parent.Value(key)
}

func (envCtx *AzCopyEnvironmentContext) GetEnvTempPath(env *AzCopyEnvironment) string {
	return filepath.Join(os.TempDir(), envCtx.ScenarioID.String(), fmt.Sprintf("%03d", DerefOrZero(env.EnvironmentId)))
}

func (envCtx *AzCopyEnvironmentContext) GetEnvUploadPath(env *AzCopyEnvironment) string {
	return filepath.Join(GlobalConfig.AzCopyExecutableConfig.LogDropPath, envCtx.ScenarioID.String(), fmt.Sprintf("%03d", DerefOrZero(env.EnvironmentId)))
}

const (
	LogSubdir   = "log"
	PlanSubdir  = "plan"
	PprofSubdir = "pprof"
	PprofMemFmt = "%03d.memory.pprof"
	StdoutFmt   = "%03d.stdout.txt"
	StderrFmt   = "%03d.stderr.txt"
)

// CreateEnvironment should be called in the event an AzCopyEnvironment wasn't specified, and should be presumed to be Run 0.
func (envCtx *AzCopyEnvironmentContext) CreateEnvironment() *AzCopyEnvironment {
	envCtx.mu.Lock()
	defer envCtx.mu.Unlock()

	out := &AzCopyEnvironment{
		ParentContext: envCtx,
		EnvironmentId: pointerTo(uint(len(envCtx.Environments))),
		RunCount:      pointerTo[uint](1),
	}

	envCtx.Environments = append(envCtx.Environments, out)

	return out
}

// RegisterEnvironment should be called if an AzCopyEnvironment is specified, and will no-op if the env already was registered.
func (envCtx *AzCopyEnvironmentContext) RegisterEnvironment(env *AzCopyEnvironment) (runId uint) {
	envCtx.mu.Lock()
	defer envCtx.mu.Unlock()

	rc := DerefOrZero(env.RunCount)
	env.RunCount = pointerTo(rc + 1)

	if env.EnvironmentId != nil {
		return rc
	}

	env.EnvironmentId = pointerTo(uint(len(envCtx.Environments)))
	env.ParentContext = envCtx
	envCtx.Environments = append(envCtx.Environments, env)

	return rc
}

func (envCtx *AzCopyEnvironmentContext) RegisterLogUpload(upload LogUpload) {
	envCtx.mu.Lock()
	envCtx.mu.Unlock()

	envCtx.LogUploads = append(envCtx.LogUploads, upload)
}

func (envCtx *AzCopyEnvironmentContext) SetupCleanup(a ScenarioAsserter) {
	envCtx.cleanupOnce.Do(func() {
		a.Cleanup(func(a Asserter) {
			envCtx.DoCleanup(a)
		})
	})
}

func (envCtx *AzCopyEnvironmentContext) DoCleanup(a Asserter) {
	envCtx.mu.Lock()
	defer envCtx.mu.Unlock()

	// defer deletion of the temp dir
	defer func() {
		_ = os.RemoveAll(filepath.Join(os.TempDir(), envCtx.ScenarioID.String()))
	}()

	// Upload all of the files on the disk that are needed
	for _, v := range envCtx.Environments {
		v.DoCleanup(a)
	}

	if a.Failed() && GlobalConfig.AzCopyExecutableConfig.LogDropPath != "" {
		for _, logUpload := range envCtx.LogUploads {
			env := envCtx.Environments[logUpload.EnvironmentID]
			envUploadDir := envCtx.GetEnvUploadPath(env)

			if len(logUpload.Stdout) > 0 {
				logFile, err := os.Create(filepath.Join(envUploadDir, fmt.Sprintf(StdoutFmt, logUpload.RunID)))
				a.NoError(fmt.Sprintf("create stdout file "+StdoutFmt, logUpload.RunID), err)
				if err == nil {
					_, err = logFile.WriteString(logUpload.Stdout)
					a.NoError(fmt.Sprintf("write stdout file "+StdoutFmt, logUpload.RunID), err)
				}
			}

			if len(logUpload.Stderr) > 0 {
				logFile, err := os.Create(filepath.Join(envUploadDir, fmt.Sprintf(StderrFmt, logUpload.RunID)))
				a.NoError(fmt.Sprintf("create stderr file "+StderrFmt, logUpload.RunID), err)
				if err == nil {
					_, err = logFile.WriteString(logUpload.Stderr)
					a.NoError(fmt.Sprintf("write stderr file "+StderrFmt, logUpload.RunID), err)
				}
			}
		}
	}
}

func (env *AzCopyEnvironment) DoCleanup(a Asserter) {
	p := env.ParentContext
	envPath := p.GetEnvTempPath(env)

	// Upload the memory profiles even if we didn't fail
	for pprofRun := range *env.RunCount {
		memProfLoc := filepath.Join(
			envPath,
			PprofSubdir,
			fmt.Sprintf(PprofMemFmt, pprofRun))

		UploadMemoryProfile(a, memProfLoc, pprofRun)
	}

	// set up the log drop path
	logDropPath := GlobalConfig.AzCopyExecutableConfig.LogDropPath
	if !a.Failed() || logDropPath == "" {
		return
	}
	logDropPath = env.ParentContext.GetEnvUploadPath(env)

	// DRY
	CopyDir := func(source, dest string) error {
		err := os.MkdirAll(dest, os.ModePerm|os.ModeDir)
		if err != nil && !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("failed to create dest directory: %w", err)
		}

		var errList []error
		err = filepath.WalkDir(source, func(path string, d fs.DirEntry, err error) error {
			relPath := strings.TrimPrefix(path, source)
			if err != nil {
				errList = append(errList, fmt.Errorf("failed to read %s: %w", relPath, err))
				return nil
			}

			if d.IsDir() {
				err = os.MkdirAll(filepath.Join(dest, relPath), os.ModePerm|os.ModeDir)
				if err != nil {
					errList = append(errList, fmt.Errorf("failed to create dir %s: %w", relPath, err))
				}
				return nil
			}

			srcFile, err := os.Open(path)
			if err != nil {
				errList = append(errList, fmt.Errorf("failed to open file %s: %w", relPath, err))
				return nil
			}
			defer func() {
				_ = srcFile.Close()
			}()

			destFile, err := os.Create(filepath.Join(dest, relPath))
			if err != nil {
				errList = append(errList, fmt.Errorf("failed to create dest file %s: %w", relPath, err))
				return nil
			}
			defer func() {
				_ = destFile.Close()
			}()

			_, err = io.Copy(destFile, srcFile)
			if err != nil {
				errList = append(errList, fmt.Errorf(""))
			}

			return nil
		})

		if errCt := len(errList); errCt == 0 {
			return nil
		} else if errCt == 1 {
			return errList[0]
		} else {
			out := "Encountered multiple errors copying directory: "

			for _, v := range errList {
				out += "\n"

				out += v.Error()
			}

			return errors.New(out)
		}
	}

	// Upload logs
	err := CopyDir(*env.LogLocation, filepath.Join(logDropPath, LogSubdir))
	a.NoError("failed to copy logs", err)

	// Upload plans
	err = CopyDir(*env.JobPlanLocation, filepath.Join(logDropPath, PlanSubdir))
	a.NoError("failed to copy plans", err)

	uploadRelPath := strings.TrimPrefix(logDropPath, GlobalConfig.AzCopyExecutableConfig.LogDropPath)
	a.Log("Uploaded logs for session to %s", uploadRelPath)
}
