// Copyright 2018 Google Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package datastore_keys_exist

import (
	"errors"
	"google.golang.org/appengine/datastore"
	"golang.org/x/net/context"
)

type workerResult struct {
	Exists bool
	Error error
	Job workerJob
}

type workerJob struct {
	Key *datastore.Key
	Index int
}

// Pulls tasks from incoming jobs channel & looks up if key exists,
// sends true (key exists) or false (key doesn't exist) to results channel.
func worker(client DatastoreInterface, ctx context.Context, jobs <-chan workerJob, results chan<- workerResult) {
	var exists bool
	var err error
	for j := range jobs {
		exists, err = client.KeyExists(ctx, j.Key)
		results <- workerResult{
			Exists: exists,
			Error: err,
			Job: j,
		}
	}
}

// Determines if a datastore key exists.
func KeysExist(client DatastoreInterface, ctx context.Context, keys []*datastore.Key, workers int) ([]bool, error) {
	if workers <= 0 {
		return nil, errors.New("Worker must be >= 1")
	}

	jobs := make(chan workerJob, len(keys))
	results := make(chan workerResult, len(keys))

	// Create worker pool
	for w := 0; w < workers; w++ {
		go worker(client, ctx, jobs, results)
	}

	// Schedule jobs
	for i, k := range keys {
		jobs <- workerJob{
			Key: k,
			Index: i,
		}
	}
	close(jobs)

	// Block & Get results
	out := make([]bool, len(keys))
	for range keys {
		wr := <-results

		// TODO: You might want to have more graceful
		// error handling and not disregard current results
		if wr.Error != nil {
			return nil, wr.Error
		}

		out[wr.Job.Index] = wr.Exists
	}
	return out, nil
}

type DatastoreInterface interface {
	// Checks if a given Key exists in Datastore
	KeyExists(c context.Context, key *datastore.Key) (bool, error)
}

type DatastoreProd struct {}
func (d DatastoreProd) KeyExists(ctx context.Context, key *datastore.Key) (bool, error) {
	q := datastore.NewQuery(key.Kind()).Filter("__key__ =", key).KeysOnly()

	// Only get first result, as length of results in [0, 1]
	it := q.Run(ctx);

	// We don't care about actual data returned
	var i interface{}
	_, err := it.Next(&i)

	if err == datastore.Done {
		return false, nil
	} else if err != nil {
		return false, err
	} else {
		return true, nil
	}

}
