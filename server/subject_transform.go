// Copyright 2023-2026 The NATS Authors
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
	"hash/fnv"
	"math"
	"math/rand/v2"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// Subject mapping and transform setups.
var (
	commaSeparatorRegEx                = regexp.MustCompile(`,\s*`)
	partitionMappingFunctionRegEx      = regexp.MustCompile(`{{\s*[pP]artition\s*\((.*)\)\s*}}`)
	wildcardMappingFunctionRegEx       = regexp.MustCompile(`{{\s*[wW]ildcard\s*\((.*)\)\s*}}`)
	splitFromLeftMappingFunctionRegEx  = regexp.MustCompile(`{{\s*[sS]plit[fF]rom[lL]eft\s*\((.*)\)\s*}}`)
	splitFromRightMappingFunctionRegEx = regexp.MustCompile(`{{\s*[sS]plit[fF]rom[rR]ight\s*\((.*)\)\s*}}`)
	sliceFromLeftMappingFunctionRegEx  = regexp.MustCompile(`{{\s*[sS]lice[fF]rom[lL]eft\s*\((.*)\)\s*}}`)
	sliceFromRightMappingFunctionRegEx = regexp.MustCompile(`{{\s*[sS]lice[fF]rom[rR]ight\s*\((.*)\)\s*}}`)
	splitMappingFunctionRegEx          = regexp.MustCompile(`{{\s*[sS]plit\s*\((.*)\)\s*}}`)
	leftMappingFunctionRegEx           = regexp.MustCompile(`{{\s*[lL]eft\s*\((.*)\)\s*}}`)
	rightMappingFunctionRegEx          = regexp.MustCompile(`{{\s*[rR]ight\s*\((.*)\)\s*}}`)
	randomMappingFunctionRegEx         = regexp.MustCompile(`{{\s*[rR]andom\s*\((.*)\)\s*}}`)
)

// Enum for the subject mapping subjectTransform function types
const (
	NoTransform int16 = iota
	BadTransform
	Partition
	Wildcard
	SplitFromLeft
	SplitFromRight
	SliceFromLeft
	SliceFromRight
	Split
	Left
	Right
	Random
)

// Transforms for arbitrarily mapping subjects from one to another for maps, tees and filters.
// These can also be used for proper mapping on wildcard exports/imports.
// These will be grouped and caching and locking are assumed to be in the upper layers.
type subjectTransform struct {
	src, dest            string
	dtoks                []string // destination tokens
	stoks                []string // source tokens
	dtokmftypes          []int16  // destination token mapping function types
	dtokmftokindexesargs [][]int  // destination token mapping function array of source token index arguments
	dtokmfintargs        []int32  // destination token mapping function int32 arguments
	dtokmfstringargs     []string // destination token mapping function string arguments
}

// SubjectTransformer transforms subjects using mappings
//
// This API is not part of the public API and not subject to SemVer protections
type SubjectTransformer interface {
	// TODO(dlc) - We could add in client here to allow for things like foo -> foo.$ACCOUNT
	Match(string) (string, error)
	TransformSubject(subject string) string
	TransformTokenizedSubject(tokens []string) string
}

func NewSubjectTransformWithStrict(src, dest string, strict bool) (*subjectTransform, error) {
	// strict = true for import subject mappings that need to be reversible
	// (meaning can only use the Wildcard function and must use all the pwcs that are present in the source)
	// No source given is equivalent to the source being ">"

	if dest == _EMPTY_ {
		return nil, nil
	}

	if src == _EMPTY_ {
		src = fwcs
	}

	// Both entries need to be valid subjects.
	sv, stokens, npwcs, hasFwc := subjectInfo(src)
	dv, dtokens, dnpwcs, dHasFwc := subjectInfo(dest)

	// Make sure both are valid, match fwc if present and there are no pwcs in the dest subject.
	if !sv || !dv || dnpwcs > 0 || hasFwc != dHasFwc {
		return nil, ErrBadSubject
	}

	var dtokMappingFunctionTypes []int16
	var dtokMappingFunctionTokenIndexes [][]int
	var dtokMappingFunctionIntArgs []int32
	var dtokMappingFunctionStringArgs []string

	// If the src has partial wildcards then the dest needs to have the token place markers.
	if npwcs > 0 || hasFwc {
		// We need to count to make sure that the dest has token holders for the pwcs.
		sti := make(map[int]int)
		for i, token := range stokens {
			if len(token) == 1 && token[0] == pwc {
				sti[len(sti)+1] = i
			}
		}

		nphs := 0
		for _, token := range dtokens {
			tranformType, transformArgWildcardIndexes, transfomArgInt, transformArgString, err := indexPlaceHolders(token)
			if err != nil {
				return nil, err
			}

			if strict {
				if tranformType != NoTransform && tranformType != Wildcard {
					return nil, &mappingDestinationErr{token, ErrMappingDestinationNotSupportedForImport}
				}
			}

			if tranformType == NoTransform {
				dtokMappingFunctionTypes = append(dtokMappingFunctionTypes, NoTransform)
				dtokMappingFunctionTokenIndexes = append(dtokMappingFunctionTokenIndexes, []int{-1})
				dtokMappingFunctionIntArgs = append(dtokMappingFunctionIntArgs, -1)
				dtokMappingFunctionStringArgs = append(dtokMappingFunctionStringArgs, _EMPTY_)
			} else if tranformType == Random {
				dtokMappingFunctionTypes = append(dtokMappingFunctionTypes, Random)
				dtokMappingFunctionTokenIndexes = append(dtokMappingFunctionTokenIndexes, []int{})
				dtokMappingFunctionIntArgs = append(dtokMappingFunctionIntArgs, transfomArgInt)
				dtokMappingFunctionStringArgs = append(dtokMappingFunctionStringArgs, _EMPTY_)
			} else {
				nphs += len(transformArgWildcardIndexes)
				// Now build up our runtime mapping from dest to source tokens.
				var stis []int
				for _, wildcardIndex := range transformArgWildcardIndexes {
					if wildcardIndex > npwcs {
						return nil, &mappingDestinationErr{fmt.Sprintf("%s: [%d]", token, wildcardIndex), ErrMappingDestinationIndexOutOfRange}
					}
					stis = append(stis, sti[wildcardIndex])
				}
				dtokMappingFunctionTypes = append(dtokMappingFunctionTypes, tranformType)
				dtokMappingFunctionTokenIndexes = append(dtokMappingFunctionTokenIndexes, stis)
				dtokMappingFunctionIntArgs = append(dtokMappingFunctionIntArgs, transfomArgInt)
				dtokMappingFunctionStringArgs = append(dtokMappingFunctionStringArgs, transformArgString)

			}
		}
		if strict && nphs < npwcs {
			// not all wildcards are being used in the destination
			return nil, &mappingDestinationErr{dest, ErrMappingDestinationNotUsingAllWildcards}
		}
	} else {
		// no wildcards used in the source: check that no transform functions are used in the destination
		for _, token := range dtokens {
			tranformType, _, transfomArgInt, _, err := indexPlaceHolders(token)
			if err != nil {
				return nil, err
			}

			if tranformType == NoTransform {
				dtokMappingFunctionTypes = append(dtokMappingFunctionTypes, NoTransform)
				dtokMappingFunctionTokenIndexes = append(dtokMappingFunctionTokenIndexes, []int{-1})
				dtokMappingFunctionIntArgs = append(dtokMappingFunctionIntArgs, -1)
				dtokMappingFunctionStringArgs = append(dtokMappingFunctionStringArgs, _EMPTY_)
			} else if tranformType == Random || tranformType == Partition {
				dtokMappingFunctionTypes = append(dtokMappingFunctionTypes, tranformType)
				dtokMappingFunctionTokenIndexes = append(dtokMappingFunctionTokenIndexes, []int{})
				dtokMappingFunctionIntArgs = append(dtokMappingFunctionIntArgs, transfomArgInt)
				dtokMappingFunctionStringArgs = append(dtokMappingFunctionStringArgs, _EMPTY_)
			} else {
				return nil, &mappingDestinationErr{token, ErrMappingDestinationIndexOutOfRange}
			}
		}
	}

	return &subjectTransform{
		src:                  src,
		dest:                 dest,
		dtoks:                dtokens,
		stoks:                stokens,
		dtokmftypes:          dtokMappingFunctionTypes,
		dtokmftokindexesargs: dtokMappingFunctionTokenIndexes,
		dtokmfintargs:        dtokMappingFunctionIntArgs,
		dtokmfstringargs:     dtokMappingFunctionStringArgs,
	}, nil
}

func NewSubjectTransform(src, dest string) (*subjectTransform, error) {
	return NewSubjectTransformWithStrict(src, dest, false)
}

func NewSubjectTransformStrict(src, dest string) (*subjectTransform, error) {
	return NewSubjectTransformWithStrict(src, dest, true)
}

func getMappingFunctionArgs(functionRegEx *regexp.Regexp, token string) []string {
	commandStrings := functionRegEx.FindStringSubmatch(token)
	if len(commandStrings) > 1 {
		return commaSeparatorRegEx.Split(commandStrings[1], -1)
	}
	return nil
}

// Helper for mapping functions that take a wildcard index and an integer as arguments
func transformIndexIntArgsHelper(token string, args []string, transformType int16) (int16, []int, int32, string, error) {
	if len(args) < 2 {
		return BadTransform, []int{}, -1, _EMPTY_, &mappingDestinationErr{token, ErrMappingDestinationNotEnoughArgs}
	}
	if len(args) > 2 {
		return BadTransform, []int{}, -1, _EMPTY_, &mappingDestinationErr{token, ErrMappingDestinationTooManyArgs}
	}
	i, err := strconv.Atoi(strings.Trim(args[0], " "))
	if err != nil {
		return BadTransform, []int{}, -1, _EMPTY_, &mappingDestinationErr{token, ErrMappingDestinationInvalidArg}
	}
	mappingFunctionIntArg, err := strconv.ParseInt(strings.Trim(args[1], " "), 10, 32)
	if err != nil {
		return BadTransform, []int{}, -1, _EMPTY_, &mappingDestinationErr{token, ErrMappingDestinationInvalidArg}
	}

	return transformType, []int{i}, int32(mappingFunctionIntArg), _EMPTY_, nil
}

// Helper to ingest and index the subjectTransform destination token (e.g. $x or {{}}) in the token
// returns a transformation type, and three function arguments: an array of source subject token indexes,
// and a single number (e.g. number of partitions, or a slice size), and a string (e.g.a split delimiter)
func indexPlaceHolders(token string) (int16, []int, int32, string, error) {
	length := len(token)
	if length > 1 {
		// old $1, $2, etc... mapping format still supported to maintain backwards compatibility
		if token[0] == '$' { // simple non-partition mapping
			tp, err := strconv.Atoi(token[1:])
			if err != nil {
				// other things rely on tokens starting with $ so not an error just leave it as is
				return NoTransform, []int{-1}, -1, _EMPTY_, nil
			}
			return Wildcard, []int{tp}, -1, _EMPTY_, nil
		}

		// New 'mustache' style mapping
		if isMappingFunctionToken(token) {
			// Account scoped functions are resolved when the subject is installed, never at transform time.
			// Better error returned here in case some future code path builds a transform without expanding it first
			if isAccountMappingFunction(token) {
				return BadTransform, []int{}, -1, _EMPTY_, &mappingDestinationErr{token, ErrMappingFunctionNotAvailable}
			}

			// wildcard(wildcard token index) (equivalent to $)
			args := getMappingFunctionArgs(wildcardMappingFunctionRegEx, token)
			if args != nil {
				if len(args) == 1 && args[0] == _EMPTY_ {
					return BadTransform, []int{}, -1, _EMPTY_, &mappingDestinationErr{token, ErrMappingDestinationNotEnoughArgs}
				}
				if len(args) == 1 {
					tokenIndex, err := strconv.Atoi(strings.Trim(args[0], " "))
					if err != nil {
						return BadTransform, []int{}, -1, _EMPTY_, &mappingDestinationErr{token, ErrMappingDestinationInvalidArg}
					}
					return Wildcard, []int{tokenIndex}, -1, _EMPTY_, nil
				} else {
					return BadTransform, []int{}, -1, _EMPTY_, &mappingDestinationErr{token, ErrMappingDestinationTooManyArgs}
				}
			}

			// partition(number of partitions, token1, token2, ...)
			args = getMappingFunctionArgs(partitionMappingFunctionRegEx, token)
			if args != nil {
				if len(args) < 1 {
					return BadTransform, []int{}, -1, _EMPTY_, &mappingDestinationErr{token, ErrMappingDestinationNotEnoughArgs}
				}
				if len(args) == 1 {
					mappingFunctionIntArg, err := strconv.Atoi(strings.Trim(args[0], " "))
					if err != nil || mappingFunctionIntArg > math.MaxInt32 {
						return BadTransform, []int{}, -1, _EMPTY_, &mappingDestinationErr{token, ErrMappingDestinationInvalidArg}
					}
					return Partition, []int{}, int32(mappingFunctionIntArg), _EMPTY_, nil
				}
				if len(args) >= 2 {
					mappingFunctionIntArg, err := strconv.Atoi(strings.Trim(args[0], " "))
					if err != nil || mappingFunctionIntArg > math.MaxInt32 {
						return BadTransform, []int{}, -1, _EMPTY_, &mappingDestinationErr{token, ErrMappingDestinationInvalidArg}
					}
					var numPositions = len(args[1:])
					tokenIndexes := make([]int, numPositions)
					for ti, t := range args[1:] {
						i, err := strconv.Atoi(strings.Trim(t, " "))
						if err != nil {
							return BadTransform, []int{}, -1, _EMPTY_, &mappingDestinationErr{token, ErrMappingDestinationInvalidArg}
						}
						tokenIndexes[ti] = i
					}

					return Partition, tokenIndexes, int32(mappingFunctionIntArg), _EMPTY_, nil
				}
			}

			// SplitFromLeft(token, position)
			args = getMappingFunctionArgs(splitFromLeftMappingFunctionRegEx, token)
			if args != nil {
				return transformIndexIntArgsHelper(token, args, SplitFromLeft)
			}

			// SplitFromRight(token, position)
			args = getMappingFunctionArgs(splitFromRightMappingFunctionRegEx, token)
			if args != nil {
				return transformIndexIntArgsHelper(token, args, SplitFromRight)
			}

			// SliceFromLeft(token, position)
			args = getMappingFunctionArgs(sliceFromLeftMappingFunctionRegEx, token)
			if args != nil {
				return transformIndexIntArgsHelper(token, args, SliceFromLeft)
			}

			// SliceFromRight(token, position)
			args = getMappingFunctionArgs(sliceFromRightMappingFunctionRegEx, token)
			if args != nil {
				return transformIndexIntArgsHelper(token, args, SliceFromRight)
			}

			// Right(token, length)
			args = getMappingFunctionArgs(rightMappingFunctionRegEx, token)
			if args != nil {
				return transformIndexIntArgsHelper(token, args, Right)
			}

			// Left(token, length)
			args = getMappingFunctionArgs(leftMappingFunctionRegEx, token)
			if args != nil {
				return transformIndexIntArgsHelper(token, args, Left)
			}

			// split(token, deliminator)
			args = getMappingFunctionArgs(splitMappingFunctionRegEx, token)
			if args != nil {
				if len(args) < 2 {
					return BadTransform, []int{}, -1, _EMPTY_, &mappingDestinationErr{token, ErrMappingDestinationNotEnoughArgs}
				}
				if len(args) > 2 {
					return BadTransform, []int{}, -1, _EMPTY_, &mappingDestinationErr{token, ErrMappingDestinationTooManyArgs}
				}
				i, err := strconv.Atoi(strings.Trim(args[0], " "))
				if err != nil {
					return BadTransform, []int{}, -1, _EMPTY_, &mappingDestinationErr{token, ErrMappingDestinationInvalidArg}
				}
				if strings.Contains(args[1], " ") || strings.Contains(args[1], tsep) {
					return BadTransform, []int{}, -1, _EMPTY_, &mappingDestinationErr{token: token, err: ErrMappingDestinationInvalidArg}
				}

				return Split, []int{i}, -1, args[1], nil
			}

			// Random(max)
			args = getMappingFunctionArgs(randomMappingFunctionRegEx, token)
			if args != nil {
				if len(args) != 1 {
					return BadTransform, []int{}, -1, _EMPTY_, &mappingDestinationErr{token, ErrMappingDestinationNotEnoughArgs}
				}
				mappingFunctionIntArg, err := strconv.Atoi(strings.Trim(args[0], " "))
				if err != nil || mappingFunctionIntArg > math.MaxInt32 {
					return BadTransform, []int{}, -1, _EMPTY_, &mappingDestinationErr{token, ErrMappingDestinationInvalidArg}
				}
				return Random, []int{}, int32(mappingFunctionIntArg), _EMPTY_, nil
			}

			return BadTransform, []int{}, -1, _EMPTY_, &mappingDestinationErr{token, ErrUnknownMappingDestinationFunction}
		}
	}
	return NoTransform, []int{-1}, -1, _EMPTY_, nil
}

// Helper function to tokenize subjects with partial wildcards into formal transform destinations.
// e.g. "foo.*.*" -> "foo.$1.$2"
func transformTokenize(subject string) string {
	// We need to make the appropriate markers for the wildcards etc.
	i := 1
	var nda []string
	for token := range strings.SplitSeq(subject, tsep) {
		if token == pwcs {
			nda = append(nda, fmt.Sprintf("$%d", i))
			i++
		} else {
			nda = append(nda, token)
		}
	}
	return strings.Join(nda, tsep)
}

// Helper function to go from transform destination to a subject with partial wildcards and ordered list of placeholders
// E.g.:
//
//		"bar" -> "bar", []
//		"foo.$2.$1" -> "foo.*.*", ["$2","$1"]
//	    "foo.{{wildcard(2)}}.{{wildcard(1)}}" -> "foo.*.*", ["{{wildcard(2)}}","{{wildcard(1)}}"]
func transformUntokenize(subject string) (string, []string) {
	var phs []string
	var nda []string

	for token := range strings.SplitSeq(subject, tsep) {
		if args := getMappingFunctionArgs(wildcardMappingFunctionRegEx, token); (len(token) > 1 && token[0] == '$' && token[1] >= '1' && token[1] <= '9') || (len(args) == 1 && args[0] != _EMPTY_) {
			phs = append(phs, token)
			nda = append(nda, pwcs)
		} else {
			nda = append(nda, token)
		}
	}
	return strings.Join(nda, tsep), phs
}

func tokenizeSubject(subject string) []string {
	// Tokenize the subject.
	tsa := [32]string{}
	tts := tsa[:0]
	start := 0
	for i := 0; i < len(subject); i++ {
		if subject[i] == btsep {
			tts = append(tts, subject[start:i])
			start = i + 1
		}
	}
	tts = append(tts, subject[start:])
	return tts
}

// Match will take a literal published subject that is associated with a client and will match and subjectTransform
// the subject if possible.
//
// This API is not part of the public API and not subject to SemVer protections
func (tr *subjectTransform) Match(subject string) (string, error) {
	// Special case: matches any and no no-op subjectTransform. May not be legal config for some features
	// but specific validations made at subjectTransform create time
	if (tr.src == fwcs || tr.src == _EMPTY_) && (tr.dest == fwcs || tr.dest == _EMPTY_) {
		return subject, nil
	}

	tts := tokenizeSubject(subject)

	// TODO(jnm): optimization -> not sure this is actually needed but was there in initial code
	if !isValidLiteralSubject(slices.Values(tts)) {
		return _EMPTY_, ErrBadSubject
	}

	if (tr.src == _EMPTY_ || tr.src == fwcs) || isSubsetMatch(tts, tr.src) {
		return tr.TransformTokenizedSubject(tts), nil
	}
	return _EMPTY_, ErrNoTransforms
}

// TransformSubject transforms a subject
//
// This API is not part of the public API and not subject to SemVer protection
func (tr *subjectTransform) TransformSubject(subject string) string {
	return tr.TransformTokenizedSubject(tokenizeSubject(subject))
}

func (tr *subjectTransform) getRandomPartition(ceiling int) string {
	// Avoid an integer divide by zero panic below.
	if ceiling == 0 {
		return "0"
	}

	return strconv.Itoa(int(rand.Int32()) % ceiling)
}

func (tr *subjectTransform) getHashPartition(key []byte, numBuckets int) string {
	// Avoid an integer divide by zero panic below.
	if numBuckets == 0 {
		return "0"
	}

	h := fnv.New32a()
	_, _ = h.Write(key)

	return strconv.Itoa(int(h.Sum32() % uint32(numBuckets)))
}

// Do a subjectTransform on the subject to the dest subject.
func (tr *subjectTransform) TransformTokenizedSubject(tokens []string) string {
	if len(tr.dtokmftypes) == 0 {
		return tr.dest
	}

	var b strings.Builder

	// We need to walk destination tokens and create the mapped subject pulling tokens or mapping functions
	li := len(tr.dtokmftypes) - 1
	for i, mfType := range tr.dtokmftypes {
		if mfType == NoTransform {
			// Break if fwc
			if len(tr.dtoks[i]) == 1 && tr.dtoks[i][0] == fwc {
				break
			}
			b.WriteString(tr.dtoks[i])
		} else {
			switch mfType {
			case Partition:
				var (
					_buffer       [64]byte
					keyForHashing = _buffer[:0]
				)
				if len(tr.dtokmftokindexesargs[i]) > 0 {
					// When token positions are specified.
					for _, sourceToken := range tr.dtokmftokindexesargs[i] {
						keyForHashing = append(keyForHashing, []byte(tokens[sourceToken])...)
					}
				} else {
					// When using the shorthand partition(n).
					keyForHashing = append(keyForHashing, strings.Join(tokens, ".")...)
				}
				b.WriteString(tr.getHashPartition(keyForHashing, int(tr.dtokmfintargs[i])))
			case Wildcard: // simple substitution
				switch {
				case len(tr.dtokmftokindexesargs) < i:
					break
				case len(tr.dtokmftokindexesargs[i]) < 1:
					break
				case len(tokens) <= tr.dtokmftokindexesargs[i][0]:
					break
				default:
					b.WriteString(tokens[tr.dtokmftokindexesargs[i][0]])
				}
			case SplitFromLeft:
				sourceToken := tokens[tr.dtokmftokindexesargs[i][0]]
				sourceTokenLen := len(sourceToken)
				position := int(tr.dtokmfintargs[i])
				if position > 0 && position < sourceTokenLen {
					b.WriteString(sourceToken[:position])
					b.WriteString(tsep)
					b.WriteString(sourceToken[position:])
				} else { // too small to split at the requested position: don't split
					b.WriteString(sourceToken)
				}
			case SplitFromRight:
				sourceToken := tokens[tr.dtokmftokindexesargs[i][0]]
				sourceTokenLen := len(sourceToken)
				position := int(tr.dtokmfintargs[i])
				if position > 0 && position < sourceTokenLen {
					b.WriteString(sourceToken[:sourceTokenLen-position])
					b.WriteString(tsep)
					b.WriteString(sourceToken[sourceTokenLen-position:])
				} else { // too small to split at the requested position: don't split
					b.WriteString(sourceToken)
				}
			case SliceFromLeft:
				sourceToken := tokens[tr.dtokmftokindexesargs[i][0]]
				sourceTokenLen := len(sourceToken)
				sliceSize := int(tr.dtokmfintargs[i])
				if sliceSize > 0 && sliceSize < sourceTokenLen {
					for i := 0; i+sliceSize <= sourceTokenLen; i += sliceSize {
						if i != 0 {
							b.WriteString(tsep)
						}
						b.WriteString(sourceToken[i : i+sliceSize])
						if i+sliceSize != sourceTokenLen && i+sliceSize+sliceSize > sourceTokenLen {
							b.WriteString(tsep)
							b.WriteString(sourceToken[i+sliceSize:])
							break
						}
					}
				} else { // too small to slice at the requested size: don't slice
					b.WriteString(sourceToken)
				}
			case SliceFromRight:
				sourceToken := tokens[tr.dtokmftokindexesargs[i][0]]
				sourceTokenLen := len(sourceToken)
				sliceSize := int(tr.dtokmfintargs[i])
				if sliceSize > 0 && sliceSize < sourceTokenLen {
					remainder := sourceTokenLen % sliceSize
					if remainder > 0 {
						b.WriteString(sourceToken[:remainder])
						b.WriteString(tsep)
					}
					for i := remainder; i+sliceSize <= sourceTokenLen; i += sliceSize {
						b.WriteString(sourceToken[i : i+sliceSize])
						if i+sliceSize < sourceTokenLen {
							b.WriteString(tsep)
						}
					}
				} else { // too small to slice at the requested size: don't slice
					b.WriteString(sourceToken)
				}
			case Split:
				sourceToken := tokens[tr.dtokmftokindexesargs[i][0]]
				splits := strings.Split(sourceToken, tr.dtokmfstringargs[i])
				for j, split := range splits {
					if split != _EMPTY_ {
						b.WriteString(split)
					}
					if j < len(splits)-1 && splits[j+1] != _EMPTY_ && !(j == 0 && split == _EMPTY_) {
						b.WriteString(tsep)
					}
				}
			case Left:
				sourceToken := tokens[tr.dtokmftokindexesargs[i][0]]
				sourceTokenLen := len(sourceToken)
				sliceSize := int(tr.dtokmfintargs[i])
				if sliceSize > 0 && sliceSize < sourceTokenLen {
					b.WriteString(sourceToken[0:sliceSize])
				} else { // too small to slice at the requested size: don't slice
					b.WriteString(sourceToken)
				}
			case Right:
				sourceToken := tokens[tr.dtokmftokindexesargs[i][0]]
				sourceTokenLen := len(sourceToken)
				sliceSize := int(tr.dtokmfintargs[i])
				if sliceSize > 0 && sliceSize < sourceTokenLen {
					b.WriteString(sourceToken[sourceTokenLen-sliceSize : sourceTokenLen])
				} else { // too small to slice at the requested size: don't slice
					b.WriteString(sourceToken)
				}
			case Random:
				b.WriteString(tr.getRandomPartition(int(tr.dtokmfintargs[i])))
			}
		}

		if i < li {
			b.WriteByte(btsep)
		}
	}

	// We may have more source tokens available. This happens with ">".
	if tr.dtoks[len(tr.dtoks)-1] == fwcs {
		for sli, i := len(tokens)-1, len(tr.stoks)-1; i < len(tokens); i++ {
			b.WriteString(tokens[i])
			if i < sli {
				b.WriteByte(btsep)
			}
		}
	}
	return b.String()
}

// Reverse a subjectTransform.
func (tr *subjectTransform) reverse() *subjectTransform {
	if len(tr.dtokmftokindexesargs) == 0 {
		rtr, _ := NewSubjectTransformStrict(tr.dest, tr.src)
		return rtr
	}
	// If we are here we need to dynamically get the correct reverse
	// of this subjectTransform.
	nsrc, phs := transformUntokenize(tr.dest)
	var nda []string
	for _, token := range tr.stoks {
		if token == pwcs {
			if len(phs) == 0 {
				// TODO(dlc) - Should not happen
				return nil
			}
			nda = append(nda, phs[0])
			phs = phs[1:]
		} else {
			nda = append(nda, token)
		}
	}
	ndest := strings.Join(nda, tsep)
	rtr, _ := NewSubjectTransformStrict(nsrc, ndest)
	return rtr
}

// Will share relevant info regarding the subject.
// Returns valid, tokens, num pwcs, has fwc.
func subjectInfo(subject string) (bool, []string, int, bool) {
	if subject == "" {
		return false, nil, 0, false
	}
	npwcs := 0
	sfwc := false
	tokens := strings.Split(subject, tsep)
	for _, t := range tokens {
		if len(t) == 0 || sfwc {
			return false, nil, 0, false
		}
		if len(t) > 1 {
			continue
		}
		switch t[0] {
		case fwc:
			sfwc = true
		case pwc:
			npwcs++
		}
	}
	return true, tokens, npwcs, sfwc
}

// Account scoped mapping functions.
//
// Unlike the functions above, these are not evaluated per message: they are resolved once, into a literal token,
// when the subject that contains them is installed (account mappings, imports and exports, leafnode remote deny
// lists). This is what allows them to be used in subjects that are never transformed, such as import/export
// subjects and deny lists, and what keeps import transforms reversible. They are not resolved anywhere else
// (e.g. stream transforms), where the transform engine rejects them.
var (
	accountHashMappingFunctionRegEx           = regexp.MustCompile(`{{\s*[aA]ccount-[hH]ash\s*\((.*)\)\s*}}`)
	accountHashImportMappingFunctionRegEx     = regexp.MustCompile(`{{\s*[aA]ccount-[hH]ash-[iI]mport\s*\((.*)\)\s*}}`)
	accountHashLeafRemoteMappingFunctionRegEx = regexp.MustCompile(`{{\s*[aA]ccount-[hH]ash-[lL]eaf-[rR]emote\s*\((.*)\)\s*}}`)
)

// subjectPlaceholderContext holds the account names the account scoped mapping functions hash.
// An empty value means that the function is not available in the context the subject is used in.
type subjectPlaceholderContext struct {
	account       string // {{account-hash()}}
	importAccount string // {{account-hash-import()}}
	leafRemote    string // {{account-hash-leaf-remote()}}
	// When set, the remote account of a leafnode connection is not known yet: {{account-hash-leaf-remote()}}
	// is left untouched instead of being an error, to be resolved when the permissions are applied again.
	leafRemotePending bool
}

// The account scoped mapping functions take no arguments.
type accountMappingFunction struct {
	re      *regexp.Regexp
	value   func(ctx *subjectPlaceholderContext) string
	pending func(ctx *subjectPlaceholderContext) bool // the value may become available later
}

func neverPending(*subjectPlaceholderContext) bool { return false }

var accountMappingFunctions = []accountMappingFunction{
	{accountHashImportMappingFunctionRegEx, func(c *subjectPlaceholderContext) string { return c.importAccount }, neverPending},
	{accountHashLeafRemoteMappingFunctionRegEx, func(c *subjectPlaceholderContext) string { return c.leafRemote }, func(c *subjectPlaceholderContext) bool { return c.leafRemotePending }},
	{accountHashMappingFunctionRegEx, func(c *subjectPlaceholderContext) string { return c.account }, neverPending},
}

// isMappingFunctionToken reports whether the token has the {{...}} shape of a mapping function.
func isMappingFunctionToken(token string) bool {
	l := len(token)
	return l > 4 && token[0] == '{' && token[1] == '{' && token[l-2] == '}' && token[l-1] == '}'
}

// isAccountMappingFunction reports whether the token is one of the account scoped mapping functions.
func isAccountMappingFunction(token string) bool {
	if !isMappingFunctionToken(token) {
		return false
	}
	for _, f := range accountMappingFunctions {
		if f.re.MatchString(token) {
			return true
		}
	}
	return false
}

// hasSubjectPlaceholders is a cheap check for subjects that may contain mapping functions.
func hasSubjectPlaceholders(subject string) bool {
	return strings.Contains(subject, "{{")
}

// expandSubjectPlaceholderToken resolves a single account scoped mapping function token.
// Returns the hash and true when the token was resolved, or the token itself and false when it is not
// an account scoped mapping function or when its value is pending.
func expandSubjectPlaceholderToken(token string, ctx *subjectPlaceholderContext) (string, bool, error) {
	if !isMappingFunctionToken(token) {
		return token, false, nil
	}
	for _, f := range accountMappingFunctions {
		args := getMappingFunctionArgs(f.re, token)
		if args == nil {
			continue
		}
		// No arguments for the account mapping functions
		if len(args) > 1 || strings.TrimSpace(args[0]) != _EMPTY_ {
			return _EMPTY_, false, &mappingDestinationErr{token, ErrMappingDestinationTooManyArgs}
		}
		value := f.value(ctx)
		if value == _EMPTY_ {
			if f.pending(ctx) {
				return token, false, nil
			}
			return _EMPTY_, false, &mappingDestinationErr{token, ErrMappingFunctionNotAvailable}
		}
		return getHash(value), true, nil
	}
	return token, false, nil
}

// expandSubjectPlaceholders replaces the account scoped mapping functions in subject with their literal values.
// Any other mapping function, and any other token, is left untouched. Errors are of type *mappingDestinationErr.
func expandSubjectPlaceholders(subject string, ctx subjectPlaceholderContext) (string, error) {
	if !hasSubjectPlaceholders(subject) {
		return subject, nil
	}
	tokens := strings.Split(subject, tsep)
	changed := false
	for i, token := range tokens {
		value, ok, err := expandSubjectPlaceholderToken(token, &ctx)
		if err != nil {
			return _EMPTY_, err
		}
		if ok {
			tokens[i] = value
			changed = true
		}
	}
	if !changed {
		return subject, nil
	}
	return strings.Join(tokens, tsep), nil
}

// expandSubjectsPlaceholders resolves the account scoped mapping functions in the given subjects in place.
// Empty subjects are skipped.
func expandSubjectsPlaceholders(ctx subjectPlaceholderContext, subjects ...*string) error {
	for _, subject := range subjects {
		if *subject == _EMPTY_ {
			continue
		}
		expanded, err := expandSubjectPlaceholders(*subject, ctx)
		if err != nil {
			return err
		}
		*subject = expanded
	}
	return nil
}

// expandPermissionSubjectPlaceholders is expandSubjectPlaceholders for permission entries, which for
// subscribe permissions may carry a queue name after the subject.
func expandPermissionSubjectPlaceholders(entry string, ctx subjectPlaceholderContext) (string, error) {
	if !hasSubjectPlaceholders(entry) {
		return entry, nil
	}
	subject, queue, hasQueue := strings.Cut(entry, " ")
	expanded, err := expandSubjectPlaceholders(subject, ctx)
	if err != nil {
		return _EMPTY_, err
	}
	if hasQueue {
		return expanded + " " + queue, nil
	}
	return expanded, nil
}

// permissionsHavePlaceholders reports whether any subject in perms may contain a mapping function.
func permissionsHavePlaceholders(perms *Permissions) bool {
	if perms == nil {
		return false
	}
	for _, sp := range []*SubjectPermission{perms.Publish, perms.Subscribe} {
		if sp == nil {
			continue
		}
		for _, s := range sp.Allow {
			if hasSubjectPlaceholders(s) {
				return true
			}
		}
		for _, s := range sp.Deny {
			if hasSubjectPlaceholders(s) {
				return true
			}
		}
	}
	return false
}

// expandPermissionSubjectsPlaceholders returns a copy of the permission entries with the account scoped mapping
// functions replaced by their literal values. Entries that fail to expand are kept as is and reported in the
// returned error, which may combine several errors. The given slice is never modified.
func expandPermissionSubjectsPlaceholders(subjects []string, ctx subjectPlaceholderContext) ([]string, error) {
	if len(subjects) == 0 {
		return subjects, nil
	}
	var errs []error
	expanded := make([]string, len(subjects))
	for i, s := range subjects {
		e, err := expandPermissionSubjectPlaceholders(s, ctx)
		if err != nil {
			errs = append(errs, fmt.Errorf("permission subject %q: %w", s, err))
			e = s
		}
		expanded[i] = e
	}
	return expanded, errors.Join(errs...)
}

// expandPermissionsPlaceholders returns perms with the account scoped mapping functions in its subjects replaced
// by their literal values. The given perms are never modified: the same pointer is returned when there is nothing
// to expand, a clone otherwise. Entries that fail to expand are kept as is and reported in the returned error,
// which may combine several errors.
func expandPermissionsPlaceholders(perms *Permissions, ctx subjectPlaceholderContext) (*Permissions, error) {
	if !permissionsHavePlaceholders(perms) {
		return perms, nil
	}
	var errs []error
	clone := perms.clone()
	for _, sp := range []*SubjectPermission{clone.Publish, clone.Subscribe} {
		if sp == nil {
			continue
		}
		var err error
		if sp.Allow, err = expandPermissionSubjectsPlaceholders(sp.Allow, ctx); err != nil {
			errs = append(errs, err)
		}
		if sp.Deny, err = expandPermissionSubjectsPlaceholders(sp.Deny, ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return clone, errors.Join(errs...)
}
