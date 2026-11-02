// Copyright 2023-2025 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestPlaceHolderIndex(t *testing.T) {
	testString := "$1"
	transformType, indexes, nbPartitions, _, err := indexPlaceHolders(testString)
	var position int32

	if err != nil || transformType != Wildcard || len(indexes) != 1 || indexes[0] != 1 || nbPartitions != -1 {
		t.Fatalf("Error parsing %s", testString)
	}

	testString = "{{partition(10,1,2,3)}}"

	transformType, indexes, nbPartitions, _, err = indexPlaceHolders(testString)

	if err != nil || transformType != Partition || !reflect.DeepEqual(indexes, []int{1, 2, 3}) || nbPartitions != 10 {
		t.Fatalf("Error parsing %s", testString)
	}

	testString = "{{ Partition (10,1,2,3) }}"

	transformType, indexes, nbPartitions, _, err = indexPlaceHolders(testString)

	if err != nil || transformType != Partition || !reflect.DeepEqual(indexes, []int{1, 2, 3}) || nbPartitions != 10 {
		t.Fatalf("Error parsing %s", testString)
	}

	testString = "{{wildcard(2)}}"
	transformType, indexes, nbPartitions, _, err = indexPlaceHolders(testString)

	if err != nil || transformType != Wildcard || len(indexes) != 1 || indexes[0] != 2 || nbPartitions != -1 {
		t.Fatalf("Error parsing %s", testString)
	}

	testString = "{{SplitFromLeft(2,1)}}"
	transformType, indexes, position, _, err = indexPlaceHolders(testString)

	if err != nil || transformType != SplitFromLeft || len(indexes) != 1 || indexes[0] != 2 || position != 1 {
		t.Fatalf("Error parsing %s", testString)
	}

	testString = "{{SplitFromRight(3,2)}}"
	transformType, indexes, position, _, err = indexPlaceHolders(testString)

	if err != nil || transformType != SplitFromRight || len(indexes) != 1 || indexes[0] != 3 || position != 2 {
		t.Fatalf("Error parsing %s", testString)
	}

	testString = "{{SliceFromLeft(2,2)}}"
	transformType, indexes, sliceSize, _, err := indexPlaceHolders(testString)

	if err != nil || transformType != SliceFromLeft || len(indexes) != 1 || indexes[0] != 2 || sliceSize != 2 {
		t.Fatalf("Error parsing %s", testString)
	}

	testString = "{{Left(3,2)}}"
	transformType, indexes, position, _, err = indexPlaceHolders(testString)

	if err != nil || transformType != Left || len(indexes) != 1 || indexes[0] != 3 || position != 2 {
		t.Fatalf("Error parsing %s", testString)
	}

	testString = "{{Right(3,2)}}"
	transformType, indexes, position, _, err = indexPlaceHolders(testString)

	if err != nil || transformType != Right || len(indexes) != 1 || indexes[0] != 3 || position != 2 {
		t.Fatalf("Error parsing %s", testString)
	}
}

func TestSubjectTransformHelpers(t *testing.T) {
	equals := func(a, b []string) bool {
		if len(a) != len(b) {
			return false
		}
		for i, v := range a {
			if v != b[i] {
				return false
			}
		}
		return true
	}

	filter, placeHolders := transformUntokenize("bar")
	if filter != "bar" || len(placeHolders) != 0 {
		t.Fatalf("transformUntokenize for not returning expected result")
	}

	filter, placeHolders = transformUntokenize("foo.$2.$1")
	if filter != "foo.*.*" || !equals(placeHolders, []string{"$2", "$1"}) {
		t.Fatalf("transformUntokenize for not returning expected result")
	}

	filter, placeHolders = transformUntokenize("foo.{{wildcard(2)}}.{{wildcard(1)}}")
	if filter != "foo.*.*" || !equals(placeHolders, []string{"{{wildcard(2)}}", "{{wildcard(1)}}"}) {
		t.Fatalf("transformUntokenize for not returning expected result")
	}

	newReversibleTransform := func(src, dest string) *subjectTransform {
		tr, err := NewSubjectTransformStrict(src, dest)
		if err != nil {
			t.Fatalf("Error getting reversible transform: %s to %s", src, dest)
		}
		return tr
	}

	tr := newReversibleTransform("foo.*.*", "bar.$2.{{Wildcard(1)}}")
	subject := "foo.b.a"
	transformed := tr.TransformSubject(subject)
	reverse := tr.reverse()
	if reverse.TransformSubject(transformed) != subject {
		t.Fatal("Reversed transform subject not matching")
	}
}

func TestSubjectTransforms(t *testing.T) {
	shouldErr := func(src, dest string, strict bool) {
		t.Helper()
		if _, err := NewSubjectTransformWithStrict(src, dest, strict); err != ErrBadSubject && !errors.Is(err, ErrInvalidMappingDestination) {
			t.Fatalf("Did not get an error for src=%q and dest=%q", src, dest)
		}
	}

	// Must be valid subjects.
	shouldErr("foo..", "bar", false)

	// Wildcards are allowed in src, but must be matched by token placements on the other side.
	// e.g. foo.* -> bar.$1.
	// Need to have as many pwcs as placements on other side

	shouldErr("foo.*", "bar.*", false)
	shouldErr("foo.*", "bar.$2", false)                   // Bad pwc token identifier
	shouldErr("foo.*", "bar.$1.>", false)                 // fwcs have to match.
	shouldErr("foo.>", "bar.baz", false)                  // fwcs have to match.
	shouldErr("foo.*.*", "bar.$2", true)                  // Must place all pwcs.
	shouldErr("foo.*", "foo.$foo", true)                  // invalid $ value
	shouldErr("foo.*", "bar.{{Partition(2,1)}}", true)    // can only use Wildcard function (and old-style $x) in import transform
	shouldErr("foo.*", "foo.{{wildcard(2)}}", false)      // Mapping function being passed an out of range wildcard index
	shouldErr("foo.*", "foo.{{unimplemented(1)}}", false) // Mapping trying to use an unknown mapping function
	shouldErr("foo.*", "foo.{{partition()}}", false)      // Not enough arguments passed to the mapping function
	shouldErr("foo.*", "foo.{{random()}}", false)         // Not enough arguments passed to the random function
	shouldErr("foo.*", "foo.{{wildcard(foo)}}", false)    // Invalid argument passed to the mapping function
	shouldErr("foo.*", "foo.{{wildcard()}}", false)       // Not enough arguments passed to the mapping function
	shouldErr("foo.*", "foo.{{wildcard(1,2)}}", false)    // Too many arguments passed to the mapping function
	shouldErr("foo.*", "foo.{{ wildcard5) }}", false)     // Bad mapping function
	shouldErr("foo.*", "foo.{{splitLeft(2,2}}", false)    // arg out of range
	shouldErr("foo", "bla.{{wildcard(1)}}", false)        // arg out of range with no wildcard in the source

	shouldErr("foo.*", fmt.Sprintf("foo.{{partition(%d)}}", math.MaxInt32+1), false) // Larger than int32
	shouldErr("foo.*", fmt.Sprintf("foo.{{random(%d)}}", math.MaxInt32+1), false)    // Larger than int32

	shouldBeOK := func(src, dest string, strict bool) *subjectTransform {
		t.Helper()
		tr, err := NewSubjectTransformWithStrict(src, dest, strict)
		if err != nil {
			t.Fatalf("Got an error %v for src=%q and dest=%q", err, src, dest)
		}
		return tr
	}

	shouldBeOK("foo.*", "bar.{{Wildcard(1)}}", true)

	shouldBeOK("foo.*.*", "bar.$2", false)              // don't have to use all pwcs.
	shouldBeOK("foo.*.*", "bar.{{wildcard(1)}}", false) // don't have to use all pwcs.
	shouldBeOK("foo.*.*", "bar.{{partition(1)}}", false)
	shouldBeOK("foo.*.*", "bar.{{random(5)}}", false)
	shouldBeOK("foo", "bar", false)
	shouldBeOK("foo.*.bar.*.baz", "req.$2.$1", false)
	shouldBeOK("baz.>", "mybaz.>", false)
	shouldBeOK("*", "{{splitfromleft(1,1)}}", false)
	shouldBeOK("", "prefix.>", false)
	shouldBeOK("*.*", "{{partition(10,1,2)}}", false)
	shouldBeOK("foo.*.*", "foo.{{wildcard(1)}}.{{wildcard(2)}}.{{partition(5,1,2)}}", false)

	shouldBeOK("foo.*", fmt.Sprintf("foo.{{partition(%d)}}", math.MaxInt32), false) // Exactly int32
	shouldBeOK("foo.*", fmt.Sprintf("foo.{{random(%d)}}", math.MaxInt32), false)    // Exactly int32
	shouldBeOK("foo.bar", fmt.Sprintf("foo.{{random(%d)}}", math.MaxInt32), false)  // Exactly int32

	shouldMatch := func(src, dest, sample string, expected ...string) {
		t.Helper()
		tr := shouldBeOK(src, dest, false)
		if tr != nil {
			s, err := tr.Match(sample)
			if err != nil {
				t.Fatalf("Got an error %v when expecting a match for %q to %q", err, sample, expected)
			}
			if !slices.Contains(expected, s) {
				t.Fatalf("Dest does not match what was expected. Got %q, expected %q", s, expected)
			}
		}
	}

	shouldMatch("", "prefix.>", "foo", "prefix.foo")
	shouldMatch("foo", "", "foo", "foo")
	shouldMatch("foo", "bar", "foo", "bar")
	shouldMatch("foo.*.bar.*.baz", "req.$2.$1", "foo.A.bar.B.baz", "req.B.A")
	shouldMatch("foo.*.bar.*.baz", "req.{{wildcard(2)}}.{{wildcard(1)}}", "foo.A.bar.B.baz", "req.B.A")
	shouldMatch("baz.>", "my.pre.>", "baz.1.2.3", "my.pre.1.2.3")
	shouldMatch("baz.>", "foo.bar.>", "baz.1.2.3", "foo.bar.1.2.3")
	shouldMatch("*", "foo.bar.$1", "foo", "foo.bar.foo")
	shouldMatch("*", "{{splitfromleft(1,3)}}", "12345", "123.45")
	shouldMatch("*", "{{SplitFromRight(1,3)}}", "12345", "12.345")
	shouldMatch("*", "{{SliceFromLeft(1,3)}}", "1234567890", "123.456.789.0")
	shouldMatch("*", "{{SliceFromRight(1,3)}}", "1234567890", "1.234.567.890")
	shouldMatch("*", "{{split(1,-)}}", "-abc-def--ghi-", "abc.def.ghi")
	shouldMatch("*", "{{split(1,-)}}", "abc-def--ghi-", "abc.def.ghi")
	shouldMatch("*.*", "{{split(2,-)}}.{{splitfromleft(1,2)}}", "foo.-abc-def--ghij-", "abc.def.ghij.fo.o") // combo + checks split for multiple instance of deliminator and deliminator being at the start or end
	shouldMatch("*", "{{right(1,1)}}", "1234", "4")
	shouldMatch("*", "{{right(1,3)}}", "1234", "234")
	shouldMatch("*", "{{right(1,6)}}", "1234", "1234")
	shouldMatch("*", "{{left(1,1)}}", "1234", "1")
	shouldMatch("*", "{{left(1,3)}}", "1234", "123")
	shouldMatch("*", "{{left(1,6)}}", "1234", "1234")
	shouldMatch("*", "bar.{{partition(0)}}", "baz", "bar.0")
	shouldMatch("*", "bar.{{partition(10, 0)}}", "foo", "bar.3")
	shouldMatch("*.*", "bar.{{partition(10)}}", "foo.bar", "bar.6")
	shouldMatch("*", "bar.{{partition(10)}}", "foo", "bar.3")
	shouldMatch("*", "bar.{{partition(10)}}", "baz", "bar.0")
	shouldMatch("*", "bar.{{partition(10)}}", "qux", "bar.9")
	shouldMatch("*", "bar.{{random(0)}}", "qux", "bar.0")
	for range 100 {
		shouldMatch("*", "bar.{{random(6)}}", "qux", "bar.0", "bar.1", "bar.2", "bar.3", "bar.4", "bar.5")
	}
	shouldBeOK("foo.bar", "baz.{{partition(10)}}", false)
	shouldMatch("foo.bar", "baz.{{partition(10)}}", "foo.bar", "baz.6")
	shouldMatch("foo.baz", "qux.{{partition(10)}}", "foo.baz", "qux.4")
	shouldMatch("test.subject", "result.{{partition(5)}}", "test.subject", "result.0")
}

func TestSubjectTransformDoesntPanicTransformingMissingToken(t *testing.T) {
	defer func() {
		p := recover()
		require_True(t, p == nil)
	}()

	tr, err := NewSubjectTransform("foo.*", "one.two.{{wildcard(1)}}")
	require_NoError(t, err)
	require_Equal(t, tr.TransformTokenizedSubject([]string{"foo"}), "one.two.")
}

func TestSubjectPlaceholderExpansion(t *testing.T) {
	ctx := subjectPlaceholderContext{account: "ACC", importAccount: "EXP", leafRemote: "REM"}
	accHash, expHash, remHash := getHash("ACC"), getHash("EXP"), getHash("REM")

	for _, test := range []struct {
		name     string
		subject  string
		ctx      subjectPlaceholderContext
		expected string
		err      error
	}{
		{"no placeholder", "foo.bar.>", ctx, "foo.bar.>", nil},
		{"account hash", "foo.{{account-hash()}}.>", ctx, "foo." + accHash + ".>", nil},
		{"account hash capitalized", "foo.{{Account-Hash()}}", ctx, "foo." + accHash, nil},
		{"account hash spaces", "foo.{{ account-hash( ) }}", ctx, "foo." + accHash, nil},
		{"account hash import", "$JS.FC._.{{account-hash-import()}}.>", ctx, "$JS.FC._." + expHash + ".>", nil},
		{"account hash import lower case", "{{account-hash-import()}}", ctx, expHash, nil},
		{"account hash leaf remote", "{{account-hash-leaf-remote()}}", ctx, remHash, nil},
		{"account hash leaf remote capitalized", "{{Account-Hash-Leaf-Remote()}}", ctx, remHash, nil},
		{"mixed with other function", "{{account-hash()}}.{{wildcard(1)}}.{{account-hash-import()}}", ctx, accHash + ".{{wildcard(1)}}." + expHash, nil},
		{"other function untouched", "foo.{{wildcard(1)}}", ctx, "foo.{{wildcard(1)}}", nil},
		{"unknown function untouched", "foo.{{unknown()}}", ctx, "foo.{{unknown()}}", nil},
		{"camel case untouched", "foo.{{accountHash()}}", ctx, "foo.{{accountHash()}}", nil},
		{"camel case capitalized untouched", "foo.{{AccountHash()}}", ctx, "foo.{{AccountHash()}}", nil},
		{"lower case untouched", "foo.{{accounthash()}}", ctx, "foo.{{accounthash()}}", nil},
		{"camel case import untouched", "foo.{{accountHashImport()}}", ctx, "foo.{{accountHashImport()}}", nil},
		{"camel case leaf remote untouched", "foo.{{accountHashLeafRemote()}}", ctx, "foo.{{accountHashLeafRemote()}}", nil},
		{"too many args", "{{account-hash(a,b)}}", ctx, _EMPTY_, ErrMappingDestinationTooManyArgs},
		{"no arg allowed", "{{account-hash(OTHER)}}", ctx, _EMPTY_, ErrMappingDestinationTooManyArgs},
		{"no arg allowed for import", "{{account-hash-import(a)}}", ctx, _EMPTY_, ErrMappingDestinationTooManyArgs},
		{"no arg allowed for leaf remote", "{{account-hash-leaf-remote(a)}}", ctx, _EMPTY_, ErrMappingDestinationTooManyArgs},
		{"import not available", "{{account-hash-import()}}", subjectPlaceholderContext{account: "ACC"}, _EMPTY_, ErrMappingFunctionNotAvailable},
		{"leaf remote not available", "{{account-hash-leaf-remote()}}", subjectPlaceholderContext{account: "ACC"}, _EMPTY_, ErrMappingFunctionNotAvailable},
		{"account not available", "{{account-hash()}}", subjectPlaceholderContext{}, _EMPTY_, ErrMappingFunctionNotAvailable},
		{"leaf remote pending", "a.{{account-hash-leaf-remote()}}.{{account-hash()}}", subjectPlaceholderContext{account: "ACC", leafRemotePending: true}, "a.{{account-hash-leaf-remote()}}." + accHash, nil},
		{"leaf remote pending still checks args", "{{account-hash-leaf-remote(x)}}", subjectPlaceholderContext{leafRemotePending: true}, _EMPTY_, ErrMappingDestinationTooManyArgs},
		{"leaf remote pending does not cover import", "{{account-hash-import()}}", subjectPlaceholderContext{account: "ACC", leafRemotePending: true}, _EMPTY_, ErrMappingFunctionNotAvailable},
		{"leaf remote pending does not cover account", "{{account-hash()}}", subjectPlaceholderContext{leafRemotePending: true}, _EMPTY_, ErrMappingFunctionNotAvailable},
		{"name with dot is hashed", "{{account-hash()}}", subjectPlaceholderContext{account: "my.acc"}, getHash("my.acc"), nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := expandSubjectPlaceholders(test.subject, test.ctx)
			if test.err != nil {
				require_Error(t, err, ErrInvalidMappingDestination)
				var mde *mappingDestinationErr
				if !errors.As(err, &mde) || mde.err != test.err {
					t.Fatalf("Expected error %v, got %v", test.err, err)
				}
				return
			}
			require_NoError(t, err)
			require_Equal(t, got, test.expected)
		})
	}
}

func TestSubjectPlaceholderNotEvaluatedByTransforms(t *testing.T) {
	// The account scoped functions are resolved when a subject is installed, the transform
	// engine itself rejects them with a specific error.
	for _, token := range []string{"{{account-hash()}}", "{{account-hash-import()}}", "{{account-hash-leaf-remote()}}"} {
		require_True(t, isAccountMappingFunction(token))
		tt, _, _, _, err := indexPlaceHolders(token)
		require_Equal(t, tt, BadTransform)
		require_Error(t, err, ErrMappingFunctionNotAvailable)
		_, err = NewSubjectTransform("foo.*", "bar."+token+".{{wildcard(1)}}")
		require_Error(t, err, ErrMappingFunctionNotAvailable)
		require_Error(t, ValidateMapping("foo", token), ErrMappingFunctionNotAvailable)
	}
	require_False(t, isAccountMappingFunction("{{wildcard(1)}}"))
	require_False(t, isAccountMappingFunction("account-hash()"))
	require_False(t, isAccountMappingFunction("{{accountHash()}}"))
	require_False(t, isAccountMappingFunction("{{account()}}"))
}

func TestPermissionsPlaceholderExpansion(t *testing.T) {
	ctx := subjectPlaceholderContext{account: "ACC", leafRemote: "REM"}
	accHash, remHash := getHash("ACC"), getHash("REM")

	// Nothing to expand: same pointer back.
	perms := &Permissions{Publish: &SubjectPermission{Allow: []string{"foo"}}}
	require_False(t, permissionsHavePlaceholders(perms))
	got, err := expandPermissionsPlaceholders(perms, ctx)
	require_NoError(t, err)
	require_True(t, got == perms)
	got, err = expandPermissionsPlaceholders(nil, ctx)
	require_NoError(t, err)
	require_True(t, got == nil)

	perms = &Permissions{
		Publish:   &SubjectPermission{Allow: []string{"foo.{{account-hash()}}"}, Deny: []string{"bar.{{account-hash-leaf-remote()}}.>"}},
		Subscribe: &SubjectPermission{Allow: []string{"baz.{{account-hash()}} myqueue"}, Deny: []string{"{{account-hash-import()}}"}},
		Response:  &ResponsePermission{MaxMsgs: 1},
	}
	require_True(t, permissionsHavePlaceholders(perms))
	got, err = expandPermissionsPlaceholders(perms, ctx)
	// The import function is not available here: that entry is kept as is and reported.
	require_Error(t, err, ErrMappingFunctionNotAvailable)
	require_True(t, got != perms)
	require_Equal(t, got.Publish.Allow[0], "foo."+accHash)
	require_Equal(t, got.Publish.Deny[0], "bar."+remHash+".>")
	require_Equal(t, got.Subscribe.Allow[0], "baz."+accHash+" myqueue")
	require_Equal(t, got.Subscribe.Deny[0], "{{account-hash-import()}}")
	require_Equal(t, got.Response.MaxMsgs, 1)
	// The original is untouched.
	require_Equal(t, perms.Publish.Allow[0], "foo.{{account-hash()}}")
	require_Equal(t, perms.Publish.Deny[0], "bar.{{account-hash-leaf-remote()}}.>")
	require_Equal(t, perms.Subscribe.Allow[0], "baz.{{account-hash()}} myqueue")

	// With the leaf remote account pending, only that function is left alone without error;
	// the import function is still reported.
	got, err = expandPermissionsPlaceholders(perms, subjectPlaceholderContext{account: "ACC", leafRemotePending: true})
	require_Error(t, err, ErrMappingFunctionNotAvailable)
	require_Contains(t, err.Error(), "{{account-hash-import()}}")
	require_False(t, strings.Contains(err.Error(), "{{account-hash-leaf-remote()}}"))
	require_Equal(t, got.Publish.Allow[0], "foo."+accHash)
	require_Equal(t, got.Publish.Deny[0], "bar.{{account-hash-leaf-remote()}}.>")
	require_Equal(t, got.Subscribe.Deny[0], "{{account-hash-import()}}")
}
