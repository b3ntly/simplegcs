package simplegcs

import (
	"errors"
	"io/ioutil"

	"github.com/marcacohen/gcslock"
	"github.com/mholt/certmagic"
	gstorage "cloud.google.com/go/storage"
	gcontext "golang.org/x/net/context"
	giterator "google.golang.org/api/iterator"
)

// Storage ...
type Storage struct {
	bucketName string
	client     *gstorage.Client
}

// New ...
func (s *Storage) New(bucketName string) (*Storage, error) {
	client, err := gstorage.NewClient(gcontext.Background())
	if err != nil {
		return nil, err
	}

	return &Storage{bucketName: bucketName, client: client}, nil
}

// Lock ...
func (s *Storage) Lock(key string) error {
	locker, err := gcslock.New(nil, s.bucketName, key)
	if err != nil {
		return err
	}

	// BLOCKING CALL
	locker.Lock()
	return err
}

// Unlock ...
func (s *Storage) Unlock(key string) (err error) {
	defer func() {
		if err2, ok := recover().(error); ok {
			err = err2
			return
		}

		err = errors.New("paniced while unlocking")
	}()

	locker, err := gcslock.New(nil, s.bucketName, key)
	if err != nil {
		return err
	}

	// RUNTIME ERROR IF LOCKER IS NOT ALREADY LOCKED
	locker.Unlock()
	return err
}

// Store ...
func (s *Storage) Store(key string, value []byte) error {
	w := s.client.Bucket(s.bucketName).Object(key).NewWriter(gcontext.Background())
	if _, err := w.Write(value); err != nil {
		return err
	}

	return w.Close()
}

// Load ...
func (s *Storage) Load(key string) ([]byte, error) {
	reader, err := s.client.Bucket(s.bucketName).Object(key).NewReader(gcontext.Background())
	if err != nil {
		return nil, err
	}

	return ioutil.ReadAll(reader)
}

// Delete ...
func (s *Storage) Delete(key string) error { 
	return s.client.Bucket(s.bucketName).Object(key).Delete(gcontext.Background())
}

// Exists ...
func (s *Storage) Exists(key string) bool {
	_, err := s.client.Bucket(s.bucketName).Object(key).NewReader(gcontext.Background())
	if err != nil {
		return false
	}

	return true 
}

// List ...
func (s *Storage) List(prefix string, recursive bool) (keys []string, err error) { 
	query := &gstorage.Query{Prefix:prefix}
	iterator := s.client.Bucket(s.bucketName).Objects(gcontext.Background(), query)

	for {
		attrs, err := iterator.Next()
		if err == giterator.Done {
			break
		}

		if err != nil {
			return nil, err
		}

		keys = append(keys, attrs.Name)
	}

	return keys, err 
}

// Stat ...
func (s *Storage) Stat(key string) (certmagic.KeyInfo, error) {
	attrs, err := s.client.Bucket(s.bucketName).Object(key).Attrs(gcontext.Background())
	if err != nil {
		return certmagic.KeyInfo{}, err
	}

	return certmagic.KeyInfo{
		Key:        key,
		Size:       attrs.Size,
		Modified:   attrs.Updated,
		IsTerminal: true,
	}, nil 
}
