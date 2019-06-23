package simplegcs

import (
	"os"
	"path"
	"testing"
	"time"

	"github.com/mholt/certmagic"
	"github.com/stretchr/testify/assert"
)

const testbucket = "simple_gcs_test_bucket"

// THIS MUST BET SET TO A VALID PROJECT ID
const testProjectID = ""

func setup() *Storage {
	if testProjectID == "" {
		println("YOU MUST SET A VALID TESTPROJECTID")
		os.Exit(-1)
	}

	s, err := New(testbucket)
	if err != nil {
		panic(err)
	}

	return s
}

func cleanup(s *Storage) {
	if err := s.BucketDelete(); err != nil {
		panic(err)
	}
}

func TestNew(t *testing.T) {
	setup()
}

func TestStorage_Store(t *testing.T) {
	s := setup()
	err := s.Store(path.Join("acme", "example.com", "sites", "example.com", "example.com.crt"), []byte("crt data"))
	assert.NoError(t, err)
	cleanup(s)
}

func TestStorage_Exists(t *testing.T) {
	s := setup()

	key := path.Join("acme", "example.com", "sites", "example.com", "example.com.crt")

	err := s.Store(key, []byte("crt data"))
	assert.NoError(t, err)

	exists := s.Exists(key)
	assert.True(t, exists)
	cleanup(s)
}

func TestStorage_Load(t *testing.T) {
	s := setup()

	key := path.Join("acme", "example.com", "sites", "example.com", "example.com.crt")
	content := []byte("crt data")

	err := s.Store(key, content)
	assert.NoError(t, err)

	contentLoded, err := s.Load(key)
	assert.NoError(t, err)

	assert.Equal(t, content, contentLoded)
	cleanup(s)
}

func TestStorage_Delete(t *testing.T) {
	s := setup()

	key := path.Join("acme", "example.com", "sites", "example.com", "example.com.crt")
	content := []byte("crt data")

	err := s.Store(key, content)
	assert.NoError(t, err)

	err = s.Delete(key)
	assert.NoError(t, err)

	exists := s.Exists(key)
	assert.False(t, exists)

	contentLoaded, err := s.Load(key)
	assert.Nil(t, contentLoaded)

	_, ok := err.(certmagic.ErrNotExist)
	assert.True(t, ok)
	cleanup(s)
}

func TestStorage_Stat(t *testing.T) {
	s := setup()

	key := path.Join("acme", "example.com", "sites", "example.com", "example.com.crt")
	content := []byte("crt data")

	err := s.Store(key, content)
	assert.NoError(t, err)

	info, err := s.Stat(key)
	assert.NoError(t, err)

	assert.Equal(t, key, info.Key)
	cleanup(s)
}

func TestStorage_List(t *testing.T) {
	s := setup()

	err := s.Store(path.Join("acme", "example.com", "sites", "example.com", "example.com.crt"), []byte("crt"))
	assert.NoError(t, err)
	err = s.Store(path.Join("acme", "example.com", "sites", "example.com", "example.com.key"), []byte("key"))
	assert.NoError(t, err)
	err = s.Store(path.Join("acme", "example.com", "sites", "example.com", "example.com.json"), []byte("meta"))
	assert.NoError(t, err)

	keys, err := s.List(path.Join("acme", "example.com", "sites", "example.com"), true)
	assert.NoError(t, err)
	assert.Len(t, keys, 3)
	assert.Contains(t, keys, path.Join("acme", "example.com", "sites", "example.com", "example.com.crt"))

	cleanup(s)
}

func TestStorage_LockUnlock(t *testing.T) {
	s := setup()
	lockKey := path.Join("acme", "example.com", "sites", "example.com", "lock")

	err := s.Lock(lockKey)
	assert.NoError(t, err)

	err = s.Unlock(lockKey)
	assert.NoError(t, err)
	cleanup(s)
}

func TestStorage_TwoLocks(t *testing.T) {
	s := setup()
	s2 := setup()
	lockKey := path.Join("acme", "example.com", "sites", "example.com", "lock")

	err := s.Lock(lockKey)
	assert.NoError(t, err)

	go time.AfterFunc(5*time.Second, func() {
		err = s.Unlock(lockKey)
		assert.NoError(t, err)
	})

	err = s2.Lock(lockKey)
	assert.NoError(t, err)

	err = s2.Unlock(lockKey)
	assert.NoError(t, err)
	cleanup(s)
	cleanup(s2)
}
