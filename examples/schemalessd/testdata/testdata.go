// Package testdata contains a suite of helper methods to assist with pkg/httpapi tests.
package testdata

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/rbastic/go-schemaless/examples/schemalessd/pkg/config"
	"github.com/rbastic/go-schemaless/examples/schemalessd/pkg/httpapi"
	"github.com/rbastic/go-schemaless/examples/schemalessd/pkg/log"

	git "gopkg.in/libgit2/git2go.v27"
)

// RepoName is our standard test repository display name (and relative on-disk path.)
const RepoName = "testrepo"

// TestFile is an uncommitted file that we write to the repository upon initialization.
const TestFile = "README"

// InitConfigRepos returns a slice of initialized config.Repo objects.
func InitConfigRepos(t *testing.T, repoName, repoPath string) []*config.Repo {
	var configRepos []*config.Repo
	configRepos = make([]*config.Repo, 1)
	configRepos[0] = &config.Repo{DisplayName: repoName, Path: repoPath}
	return configRepos
}

// CheckFatal is a small helper that checks for non-nil errors and calls t.Fatal(err) if so.
func CheckFatal(t *testing.T, err error) {
	if err != nil {
		panic(err)
	}
}

// CreateTestRepo is a mirror of CreateTestRepo from libgit2/git2go's git_test.go
func CreateTestRepo(t *testing.T) (*git.Repository, string, string) {
	// figure out where we can create the test repo
	repoPath, err := ioutil.TempDir("", RepoName)
	CheckFatal(t, err)
	repo, err := git.InitRepository(repoPath, false)
	CheckFatal(t, err)

	// test file preparation
	tmpFile := TestFile
	tmpFilePath := repoPath + "/" + tmpFile

	// write test file with "foo\n"
	err = ioutil.WriteFile(tmpFilePath, []byte("foo\n"), 0644)
	CheckFatal(t, err)

	return repo, repoPath, tmpFilePath
}

// CleanupTestRepo is a mirror of CleanupTestRepo from libgit2/git2go's git_test.go
func CleanupTestRepo(t *testing.T, repoPath string) {
	var err error
	r, err := git.OpenRepository(repoPath)
	CheckFatal(t, err)
	if r.IsBare() {
		err = os.RemoveAll(r.Path())
	} else {
		err = os.RemoveAll(r.Workdir())
	}
	CheckFatal(t, err)

	r.Free()
}

// NewHTTPAPI returns a fully configured http server with default logger and test repository configuration.
// After(t) needs to be called after the construction of an HTTPAPI.
func NewHTTPAPI(t *testing.T) (*httpapi.HTTPAPI, string) {
	repoPath := before(t)

	cfg := config.New()
	cfg.Address = ":4444"
	cfg.Repos = InitConfigRepos(t, RepoName, repoPath)

	l, err := log.New(false)
	CheckFatal(t, err)

	hs, err := httpapi.New(cfg, l)
	CheckFatal(t, err)

	return hs, repoPath
}

func before(t *testing.T) string {
	_, repoPath, _ := CreateTestRepo(t)
	return repoPath
}

// After is meant to be called using a defer() after NewHTTPAPI().
// It cleans up our test repository.
func After(t *testing.T, repoPath string) {
	CleanupTestRepo(t, repoPath)
}
