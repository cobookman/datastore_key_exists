// Copyright 2018 Google Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package datastore_keys_exist

import (
	"errors"
	"testing"
	"google.golang.org/appengine/datastore"
        "golang.org/x/net/context"
	"reflect"
	"unsafe"
)


func TestKeyExists(t *testing.T) {
	d := DatastoreStub{}
	ctx := context.Background()

	keys := []*datastore.Key{
		d.NewKey("MyKind", "keyA", 0, nil, "myapp", ""),
		d.NewKey("MyKind", "keyB", 0, nil, "myapp", ""),
		d.NewKey("MyKind", "keyC", 0, nil, "myapp", ""),
                d.NewKey("MyKind", "keyD", 0, nil, "myapp", ""),
                d.NewKey("MyKind", "keyE", 0, nil, "myapp", ""),
		d.NewKey("MyKind", "keyF", 0, nil, "myapp", ""),
		d.NewKey("MyKind", "keyNoExists", 0, nil, "myapp", ""),
	}
	res, err := KeysExist(d, ctx, keys, 3)
	if err != nil {
		t.Fatal("returned err", err)
	}

	if len(res) != len(keys) {
		t.Fatal("All keys should have been found")
	}

	for i := 0; i < 6; i++ {
		if res[i] != true {
			t.Fatal(keys[i].StringID(), "Should have existed")
		}
	}

	if res[6] != false {
		t.Fatal(keys[6].StringID(), "Should not of existed")
	}

	// Test that errors are properly passed along. E.g: in DatastoreStub
	// error is thrown if kind is not `MyKind`
	keys = []*datastore.Key{
		d.NewKey("NoKindExists", "keyA", 0, nil, "myapp", ""),
                d.NewKey("MyKind", "keyB", 0, nil, "myapp", ""),
                d.NewKey("MyKind", "keyC", 0, nil, "myapp", ""),
                d.NewKey("MyKind", "keyD", 0, nil, "myapp", ""),
                d.NewKey("MyKind", "keyE", 0, nil, "myapp", ""),
                d.NewKey("MyKind", "keyF", 0, nil, "myapp", ""),
	}
	res, err = KeysExist(d, ctx, keys, 3)
	if err == nil {
		t.Fatal("Key with Kind 'NoKindExists' should have thrown an error")
	}
	if res != nil {
		t.Fatal("When error thrown, res should be nil")
	}

	// Tests that if worker pool is 0 error thrown
	keys = []*datastore.Key{
		d.NewKey("MyKind", "keyB", 0, nil, "myapp", ""),
	}
	res, err = KeysExist(d, ctx, keys, -1)
	if err == nil {
		t.Fatal("Should throw error if worker pool size is not >= 1")
	}
	if res != nil {
		t.Fatal("When error thrown, res should be nil")
	}
}


type DatastoreStub struct {}
func (d DatastoreStub) KeyExists(c context.Context, key *datastore.Key) (bool, error) {
	if (key.Kind() != "MyKind") {
		return false, errors.New("Kind Doesn't exist")
	}

	if (key.StringID() == "keyA" ||
	    key.StringID() == "keyB" ||
            key.StringID() == "keyC" ||
            key.StringID() == "keyD" ||
            key.StringID() == "keyE" ||
            key.StringID() == "keyF") {
		return true, nil
	} else {
		return false, nil
	}
}

func (d DatastoreStub) NewKey(kind string, stringID string, intID int64, parent *datastore.Key, appID string, namespace string) *datastore.Key {
	k := new(datastore.Key)
	ks := reflect.ValueOf(k).Elem()
	_kind := ks.Field(0)
	_stringID := ks.Field(1)
	_intID := ks.Field(2)
	_parent := ks.Field(3)
	_appID := ks.Field(4)
	_namespace := ks.Field(5)

	_kind = reflect.NewAt(_kind.Type(), unsafe.Pointer(_kind.UnsafeAddr())).Elem()
	_stringID = reflect.NewAt(_stringID.Type(), unsafe.Pointer(_stringID.UnsafeAddr())).Elem()
        _intID = reflect.NewAt(_intID.Type(), unsafe.Pointer(_intID.UnsafeAddr())).Elem()
        _parent = reflect.NewAt(_parent.Type(), unsafe.Pointer(_parent.UnsafeAddr())).Elem()
        _appID = reflect.NewAt(_appID.Type(), unsafe.Pointer(_appID.UnsafeAddr())).Elem()
        _namespace = reflect.NewAt(_namespace.Type(), unsafe.Pointer(_namespace.UnsafeAddr())).Elem()

	_kind.Set(reflect.ValueOf(&kind).Elem())
	_stringID.Set(reflect.ValueOf(&stringID).Elem())
	_intID.Set(reflect.ValueOf(&intID).Elem())
	_parent.Set(reflect.ValueOf(&parent).Elem())
	_appID.Set(reflect.ValueOf(&appID).Elem())
	_namespace.Set(reflect.ValueOf(&namespace).Elem())

	return k
}
